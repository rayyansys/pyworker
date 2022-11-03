import os, sys, signal, traceback
import time
from contextlib import contextmanager
from pyworker.db import DBConnector
from pyworker.job import Job
from pyworker.logger import Logger
from pyworker.util import get_current_time, get_time_delta

class TimeoutException(Exception): pass
class TerminatedException(Exception): pass

class Worker(object):
    def __init__(self, dbstring, logger=None):
        super(Worker, self).__init__()
        self.logger = Logger(logger)
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
            raise TimeoutException(('Execution expired. Either do ' + \
                'the job faster or raise max_run_time > %d seconds') % \
                self.max_run_time)
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)

    @contextmanager
    def _terminatable(self):
        def signal_handler(signum, frame):
            signal_name = 'SIGTERM' if signum == 15 else 'SIGINT'
            self.logger.info('Received signal: %s' % signal_name)
            raise TerminatedException(signal_name)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        yield

    def run(self):
        # continuously check for new jobs on specified queue from db
        self._cursor = self.database.connect().cursor()
        with self._terminatable():
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
                        job.success()
                        job.remove()
                    time.sleep(self.sleep_delay)
                except Exception as exception:
                    if job is not None:
                        error_str = traceback.format_exc()
                        job.set_error_unlock(error_str)
                    if type(exception) == TerminatedException:
                        break
                finally:
                    if job is not None:
                        time_diff = time.time() - start_time
                        self.logger.info('Job %d finished in %d seconds' % \
                            (job.job_id, time_diff))
            
            self.database.disconnect()

    def get_job(self):
        def get_job_row():
            now = get_current_time()
            expired = now - get_time_delta(seconds=self.max_run_time)
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
            return Job.from_row(job_row, max_attempts=self.max_attempts,
                database=self.database, logger=self.logger)
        else:
            return None
