#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import signal
import datetime, time
import logging
import re
import urlparse
import traceback
import psycopg2
import yaml
import dateutil.relativedelta
from dotenv import load_dotenv, find_dotenv
from csvdedupe import csvdedupe

class DBConnector(object):
    def __init__(self, dbstring):
        super(DBConnector, self).__init__()
        url = urlparse.urlparse(dbstring)
        self._database = url.path[1:]
        self._username = url.username
        self._passwd = url.password
        self._host = url.hostname
        self._port = url.port

    def connect_database(self): 
        self._connection = psycopg2.connect(database=self._database,
            user=self._username, password=self._passwd,
            host=self._host, port=self._port)
        cursor = self._connection.cursor()
        logger.info("Connected to DelayedJob database")
        return cursor
        
    def commit(self):
        self._connection.commit()

    def disconnect_database(self):
        self._connection.close(); 
        logger.info("Disconnected from DelayedJob database")

class Worker(object):
    def __init__(self):
        super(Worker, self).__init__()
        logger.info("Starting pyworker...")
        dbstring = os.environ.get('DATABASE_URL')
        if not dbstring:
            raise EnvironmentError("DATABASE_URL environment variable missing")
        self._connector = DBConnector(dbstring)
        self.sleep_delay = 10
        self.max_attempts = 1
        self.max_run_time = 3600
        self.queue_names = 'default'
        hostname = os.uname()[1]
        pid = os.getpid()
        self.name = "host:%s pid:%d" % (hostname, pid)

    def run(self):
        # continuously check for new jobs on specified queue from db
        self._cursor = self._connector.connect_database()
        signal.signal(signal.SIGINT, self._exit)
        signal.signal(signal.SIGTERM, self._exit)
        while True:
            logger.debug('Picking up jobs...')
            job = self.get_job()
            self._current_job = job # used in signal handlers
            start_time = time.time()
            try:
                if type(job) == Job:
                    raise ValueError('Unsupported Job: %s' % job.class_name)
                elif job is not None:
                    logger.info("Running Job %d" % job.job_id)
                    job.before()
                    job.run()
                    job.after()
                    self._job_remove(job)
            except Exception as error:
                error_str = traceback.format_exc()
                self._job_set_error_unlock(job, error_str)
            finally:
                if job is not None:
                    time_diff = time.time() - start_time
                    logger.info("Job %d finished in %d seconds" % \
                        (job.job_id, time_diff))

            time.sleep(self.sleep_delay)

    def get_job(self):
        def get_job_row():
            now = self._get_current_time()
            expired = now - self._get_time_delta(seconds=self.max_run_time)
            now, expired = str(now), str(expired)
            queues = self.queue_names.split(',')
            queues = ', '.join(["'%s'" % q for q in queues])
            query = '''
            UPDATE "delayed_jobs" SET locked_at = '%s', locked_by = '%s'
            WHERE id IN (SELECT "delayed_jobs"."id" FROM "delayed_jobs"
                WHERE ((run_at <= '%s'
                AND (locked_at IS NULL OR locked_at < '%s')
                OR locked_by = '%s') AND failed_at IS NULL)
                AND "delayed_jobs"."queue" IN (%s)
            ORDER BY priority ASC, run_at ASC LIMIT 1 FOR UPDATE) RETURNING
                id, attempts, handler
            ''' % (now, self.name, now, expired, self.name, queues)
            logger.debug("query: %s" % query)
            self._cursor.execute(query)
            return self._cursor.fetchone()

        job_row = get_job_row()
        if job_row:
            return Job.from_row(job_row)
        else:
            return None
        
    def _get_current_time(self):
        # TODO return timezone or utc? get config from user?
        return datetime.datetime.utcnow()

    def _get_time_delta(self, **kwargs):
        return dateutil.relativedelta.relativedelta(**kwargs)

    def _job_set_error_unlock(self, job, error):
        logger.error("Job %d raised error: %s" % (job.job_id, error))
        job.attempts += 1
        now = self._get_current_time()
        setters = [
            'locked_at = null',
            'locked_by = null',
            'attempts = %d' % job.attempts,
            'last_error = %s'
        ]
        values = [
            error
        ]
        if job.attempts >= self.max_attempts:
            # set failed_at = now
            setters.append('failed_at = %s')
            values.append(now)
        else:
            # set new exponential run_at
            setters.append('run_at = %s')
            delta = (job.attempts**4) + 5
            values.append(str(now + self._get_time_delta(seconds=delta)))
        query = 'UPDATE delayed_jobs SET %s WHERE id = %d' % \
            (', '.join(setters), job.job_id)
        logger.debug("set error query: %s" % query)
        logger.debug("set error values: %s" % str(values))
        self._cursor.execute(query, tuple(values))
        self._connector.commit()

    def _job_remove(self, job):
        logger.debug("Job %d finished successfully" % job.job_id)
        query = 'DELETE FROM delayed_jobs WHERE id = %d' % job.job_id
        self._cursor.execute(query)
        self._connector.commit()

    def _exit(self, signum, frame):
        signal_name = 'SIGTERM' if signum == 15 else 'SIGINT'
        logger.info("Received signal: %s" % signal_name)
        self._job_set_error_unlock(self._current_job, signal_name)
        self._connector.disconnect_database()
        sys.exit(0)

class Job(object):
    """docstring for Job"""
    def __init__(self, class_name, job_id, attempts=0, attributes=None):
        super(Job, self).__init__()
        self.class_name = class_name
        self.job_id = job_id
        self.attempts = attempts
        self.attributes = attributes

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, str(self.__dict__))

    @classmethod
    def from_row(cls, job_row):
        '''job_row is a tuple of (id, attempts, handler)'''
        def extract_class_name(line):
            # TODO cache regex
            regex = re.compile('object: !ruby/object:(.+)')
            match = regex.match(line)
            if match:
                return match.group(1)
            else:
                return None

        def extract_attributes(lines):
            attributes = []
            collect = False
            for line in lines:
                if line.startswith('  raw_attributes:'):
                    collect = True
                elif not line.startswith('    '):
                    if collect:
                        break
                elif collect:
                    attributes.append(line)
            return attributes

        job_id, attempts, handler = job_row
        handler = handler.splitlines()

        class_name = extract_class_name(handler[1])
        logger.debug("Found Job %d with class name: %s" % (job_id, class_name))
        try:
            klass = globals()[class_name]
        except KeyError:
            return Job(class_name=class_name,
                job_id=job_id, attempts=attempts)

        attributes = extract_attributes(handler[2:])
        logger.debug("Found attributes: %s" % str(attributes))

        stripped = '\n'.join(['object:', '  attributes:'] + attributes)
        payload = yaml.load(stripped)
        logger.debug("payload object: %s" % str(payload))

        return klass(class_name=class_name,
            job_id=job_id, attempts=attempts,
            attributes=payload['object']['attributes'])

    def before(self):
        logger.debug("Running Job.before hook")

    def after(self):
        logger.debug("Running Job.after hook")


class DedupJob(Job):
    def __init__(self, *args, **kwargs):
        super(DedupJob, self).__init__(*args, **kwargs)

    def after(self):
        logger.debug("Running DedupJob.after hook")
        # TODO

    def run(self):
        logger.info("Running DedupJob.run")
        time.sleep(120)
        # TODO

if __name__ == "__main__":
    load_dotenv(find_dotenv())
    logger = logging.getLogger('pyworker')
    logger.setLevel(logging.INFO)
    w = Worker()
    w.sleep_delay = int(os.environ.get('DJ_SLEEP_DELAY', '5'))
    w.max_attempts = int(os.environ.get('DJ_MAX_ATTEMPTS', '3'))
    w.max_run_time = int(os.environ.get('DJ_MAX_RUN_TIME', '3600'))
    w.queue_names = os.environ.get('QUEUES', 'dedup')
    w.run()
