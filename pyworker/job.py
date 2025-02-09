import re
import yaml
from pyworker.util import get_current_time, get_time_delta

_job_class_registry = {}

def _register_class(target_class):
    _job_class_registry[target_class.__name__] = target_class


class Meta(type):
    def __new__(meta, name, bases, class_dict):
        cls = type.__new__(meta, name, bases, class_dict)
        _register_class(cls)
        return cls


class Job(object, metaclass=Meta):
    """docstring for Job"""
    def __init__(self, class_name, database, logger,
                 job_id, queue, run_at, attempts=0, max_attempts=1,
                 attributes=None, abstract=False, extra_fields=None,
                 reporter=None, max_backoff_delay_seconds=None):
        super(Job, self).__init__()
        self.class_name = class_name
        self.database = database
        self.logger = logger
        self.job_id = job_id
        self.job_name = '%s#run' % class_name
        self.attempts = attempts
        if max_backoff_delay_seconds:
            max_backoff_delay_seconds = max(max_backoff_delay_seconds, 5) # max_backoff_delay_seconds can not be less than 5 seconds
        self.max_backoff_delay_seconds = max_backoff_delay_seconds
        self.run_at = run_at
        self.queue = queue
        self.max_attempts = max_attempts
        self.attributes = attributes
        self.abstract = abstract
        self.extra_fields = extra_fields
        self.reporter = reporter

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, str(self.__dict__))

    @classmethod
    def from_row(cls, job_row, max_attempts, database, logger,
                 extra_fields=None, reporter=None, max_backoff_delay_seconds=None):
        '''job_row is a tuple of (id, attempts, run_at, queue, handler, *extra_fields)'''
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

        def extract_extra_fields(extra_fields, extra_field_values):
            if extra_fields is None or extra_field_values is None:
                return None

            return dict(zip(extra_fields, extra_field_values))

        job_id, attempts, run_at, queue, handler, *extra_field_values = job_row
        extra_fields_dict = extract_extra_fields(extra_fields, extra_field_values)
        handler = handler.splitlines()

        class_name = extract_class_name(handler[1])
        logger.debug("Found Job %d with class name: %s" % (job_id, class_name))
        try:
            target_class = _job_class_registry[class_name]
        except KeyError:
            return Job(class_name=class_name, logger=logger,
                max_attempts=max_attempts,
                job_id=job_id, attempts=attempts,
                run_at=run_at, queue=queue, database=database,
                abstract=True, extra_fields=extra_fields_dict,
                reporter=reporter, max_backoff_delay_seconds=max_backoff_delay_seconds
            )

        attributes = extract_attributes(handler[2:])
        logger.debug("Found attributes: %s" % str(attributes))

        stripped = '\n'.join(['object:', '  attributes:'] + attributes)
        payload = yaml.load(stripped, Loader=yaml.FullLoader)
        logger.debug("payload object: %s" % str(payload))

        return target_class(class_name=class_name, logger=logger,
            job_id=job_id, attempts=attempts,
            run_at=run_at, queue=queue, database=database,
            max_attempts=max_attempts,
            attributes=payload['object']['attributes'],
            abstract=False, extra_fields=extra_fields_dict,
            reporter=reporter, max_backoff_delay_seconds=max_backoff_delay_seconds
        )

    def before(self):
        self.logger.debug("Running Job.before hook")

    def after(self):
        self.logger.debug("Running Job.after hook")

    def error(self, error):
        self.logger.debug("Running Job.error hook")

    def failure(self, error):
        self.logger.debug("Running Job.failure hook")

    def success(self):
        self.logger.debug("Running Job.success hook")

    def set_error_unlock(self, error):
        failed = False
        self.logger.error('Job %d raised error: %s' % (self.job_id, error))
        # run error hook
        self.error(error)
        self.attempts += 1
        now = get_current_time()
        setters = [
            'locked_at = %s',
            'locked_by = %s',
            'attempts = %s',
            'last_error = %s'
        ]
        values = [
            None,
            None,
            self.attempts,
            error
        ]
        if self.attempts >= self.max_attempts:
            failed = True
            # set failed_at = now
            setters.append('failed_at = %s')
            values.append(now)
            self.failure(error)
        else:
            # set new exponential run_at
            setters.append('run_at = %s')
            delta = (self.attempts**4) + 5
            if self.max_backoff_delay_seconds and delta > self.max_backoff_delay_seconds:
                delta = self.max_backoff_delay_seconds
            values.append(str(now + get_time_delta(seconds=delta)))

        self._update_job(setters, values)
        return failed

    def remove(self):
        self.logger.debug('Job %d finished successfully' % self.job_id)
        query = 'DELETE FROM delayed_jobs WHERE id = %d' % self.job_id
        self.database.cursor().execute(query)
        self.database.commit()

    def _update_job(self, setters, values):
        query = 'UPDATE delayed_jobs SET %s WHERE id = %d' % \
            (', '.join(setters), self.job_id)
        self.logger.debug('update query: %s' % query)
        self.logger.debug('update values: %s' % str(values))
        self.database.cursor().execute(query, tuple(values))
        self.database.commit()
