#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import signal
import datetime, time
import logging
import re
import urlparse
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
            try:
                if type(job) == Job:
                    raise ValueError('Unsupported Job: %s' % job.class_name)
                elif job is not None:
                    logger.debug("Running job %s" % str(job))
                    job.before()
                    job.run()
                    job.after()
            except Exception as error:
                logger.error(str(error))
                job.set_error_unlock(str(error))
            time.sleep(self.sleep_delay)

    def get_job(self):
        # TODO get 1 job from the specified queue
        # if compatible job, run it
        # otherwise raise an error
        def get_job_row():
            now = datetime.datetime.utcnow()
            expired = str(now - dateutil.relativedelta.relativedelta(
                seconds=self.max_run_time))
            now = str(now)
            queues = self.queue_names.split(',')
            queues = ', '.join(["'%s'" % q for q in queues])
            query = '''
            UPDATE "delayed_jobs" SET locked_at = '%s', locked_by = '%s'
            WHERE id IN (SELECT  "delayed_jobs"."id" FROM "delayed_jobs"
                WHERE ((run_at <= '%s'
                AND (locked_at IS NULL OR locked_at < '%s')
                OR locked_by = '%s') AND failed_at IS NULL)
                AND "delayed_jobs"."queue" IN (%s)
            ORDER BY priority ASC, run_at ASC LIMIT 1 FOR UPDATE) RETURNING
                id, attempts, handler
            ''' % (now, self.name, now, expired, self.name, queues)
            self._cursor.execute(query)
            return self._cursor.fetchone()

        job_row = get_job_row()
        if job_row:
            return Job.from_row(job_row)
        else:
            return None
        
    def _exit(self, signum, frame):
        # TODO unlock any locked jobs
        # TODO gracefully stop job (raise and catch?)
        logger.info("Received signal: %d" % signum)
        self._connector.disconnect_database()
        sys.exit(0)

class Job(object):
    """docstring for Job"""
    def __init__(self, class_name, attributes=None):
        super(Job, self).__init__()
        self.class_name = class_name
        if attributes:
            self.__dict__.update(attributes)

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, str(self.__dict__))

    @classmethod
    def from_row(cls, job_row):
        '''job_row is a tuple of (id, attempts, handler)'''
        def extract_class_name(line):
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

        handler = job_row[2].splitlines()

        class_name = extract_class_name(handler[1])
        logger.debug("Found class name: %s" % class_name) 
        try:
            klass = globals()[class_name]
        except KeyError:
            return Job(class_name=class_name)

        attributes = extract_attributes(handler[2:])
        logger.debug("Found attributes: %s" % str(attributes))

        stripped = '\n'.join(['object:', '  attributes:'] + attributes)
        payload = yaml.load(stripped)
        logger.debug("payload object: %s" % str(payload))

        return klass(class_name=class_name,
            attributes=payload['object']['attributes'])

    def unlock(self):
        pass
        # TODO

    def before(self):
        logger.debug("Running Job.before hook")

    def after(self):
        logger.debug("Running Job.after hook")

    def set_error_unlock(self, error):
        pass
        # TODO

class DedupJob(Job):
    def __init__(self, *args, **kwargs):
        super(DedupJob, self).__init__(*args, **kwargs)

    def after(self):
        logger.debug("Running DedupJob.after hook")
        # TODO

    def run(self):
        logger.debug("Running DedupJob.run")
        # TODO

if __name__ == "__main__":
    load_dotenv(find_dotenv())
    logger = logging.getLogger('pyworker')
    logger.setLevel(logging.DEBUG)
    w = Worker()
    w.sleep_delay = int(os.environ.get('DJ_SLEEP_DELAY', '5'))
    w.max_attempts = int(os.environ.get('DJ_MAX_ATTEMPTS', '3'))
    w.max_run_time = int(os.environ.get('DJ_MAX_RUN_TIME', '3600'))
    w.queue_names = os.environ.get('QUEUES', 'dedup')
    w.run()
