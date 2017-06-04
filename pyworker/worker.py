import os, sys, signal, traceback
import logging
import datetime, time
from contextlib import contextmanager
import dateutil.relativedelta
from db import DBConnector
from job import Job

class TimeoutException(Exception): pass

class Worker(object):
    def __init__(self, dbstring):
        super(Worker, self).__init__()
        logging.basicConfig()
        self.logger = logging.getLogger('pyworker')
        if os.environ.get('DEBUG', '0') == '1':
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
        self.logger.info('Starting pyworker...')
        self.database = DBConnector(dbstring, self.logger)
        self.sleep_delay = 10
        self.max_attempts = 3
        self.max_run_time = 3600
        self.queue_names = 'default'
        hostname = os.uname()[1]
        pid = os.getpid()
        self.name = 'host:%s pid:%d' % (hostname, pid)

    @contextmanager
    def _time_limit(self, seconds):
        def signal_handler(signum, frame):
            raise TimeoutException, 'Execution expired. Either do ' + \
                'the job faster or raise max_run_time > %d seconds' % \
                self.max_run_time
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)

    def run(self):
        # continuously check for new jobs on specified queue from db
        self._cursor = self.database.connect().cursor()
        signal.signal(signal.SIGINT, self._exit)
        signal.signal(signal.SIGTERM, self._exit)
        while True:
            self.logger.debug('Picking up jobs...')
            job = self.get_job()
            self._current_job = job # used in signal handlers
            start_time = time.time()
            try:
                if type(job) == Job:
                    raise ValueError(('Unsupported Job: %s, please import it ' \
                        + 'before you can handle it') % job.class_name)
                elif job is not None:
                    self.logger.info('Running Job %d' % job.job_id)
                    with self._time_limit(self.max_run_time):
                        job.before()
                        job.run()
                        job.after()
                    self._job_remove(job)
            except Exception:
                error_str = traceback.format_exc()
                self._job_set_error_unlock(job, error_str)
            finally:
                if job is not None:
                    time_diff = time.time() - start_time
                    self.logger.info('Job %d finished in %d seconds' % \
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
            UPDATE delayed_jobs SET locked_at = '%s', locked_by = '%s'
            WHERE id IN (SELECT delayed_jobs.id FROM delayed_jobs
                WHERE ((run_at <= '%s'
                AND (locked_at IS NULL OR locked_at < '%s')
                OR locked_by = '%s') AND failed_at IS NULL)
                AND delayed_jobs.queue IN (%s)
            ORDER BY priority ASC, run_at ASC LIMIT 1 FOR UPDATE) RETURNING
                id, attempts, handler
            ''' % (now, self.name, now, expired, self.name, queues)
            self.logger.debug('query: %s' % query)
            self._cursor.execute(query)
            return self._cursor.fetchone()

        job_row = get_job_row()
        if job_row:
            return Job.from_row(job_row, self.logger)
        else:
            return None
        
    def _get_current_time(self):
        # TODO return timezone or utc? get config from user?
        return datetime.datetime.utcnow()

    def _get_time_delta(self, **kwargs):
        return dateutil.relativedelta.relativedelta(**kwargs)

    def _job_set_error_unlock(self, job, error):
        self.logger.error('Job %d raised error: %s' % (job.job_id, error))
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
        self.logger.debug('set error query: %s' % query)
        self.logger.debug('set error values: %s' % str(values))
        self._cursor.execute(query, tuple(values))
        self.database.commit()

    def _job_remove(self, job):
        self.logger.debug('Job %d finished successfully' % job.job_id)
        query = 'DELETE FROM delayed_jobs WHERE id = %d' % job.job_id
        self._cursor.execute(query)
        self.database.commit()

    def _exit(self, signum, frame):
        signal_name = 'SIGTERM' if signum == 15 else 'SIGINT'
        self.logger.info('Received signal: %s' % signal_name)
        if self._current_job:
            self._job_set_error_unlock(self._current_job, signal_name)
        self.database.disconnect()
        sys.exit(0)

