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


class Job(object):
    """docstring for Job"""
    __metaclass__ = Meta
    def __init__(self, class_name, database, logger,
        job_id, attempts=0, max_attempts=1, attributes=None):
        super(Job, self).__init__()
        self.class_name = class_name
        self.database = database
        self.logger = logger
        self.job_id = job_id
        self.attempts = attempts
        self.max_attempts = max_attempts
        self.attributes = attributes

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, str(self.__dict__))

    @classmethod
    def from_row(cls, job_row, max_attempts, database, logger):
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
            target_class = _job_class_registry[class_name]
        except KeyError:
            return Job(class_name=class_name, logger=logger,
                max_attempts=max_attempts,
                job_id=job_id, attempts=attempts, database=database)

        attributes = extract_attributes(handler[2:])
        logger.debug("Found attributes: %s" % str(attributes))

        stripped = '\n'.join(['object:', '  attributes:'] + attributes)
        payload = yaml.load(stripped)
        logger.debug("payload object: %s" % str(payload))

        return target_class(class_name=class_name, logger=logger,
            job_id=job_id, attempts=attempts, database=database,
            max_attempts=max_attempts,
            attributes=payload['object']['attributes'])

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
        self.logger.error('Job %d raised error: %s' % (self.job_id, error))
        # run error hook
        self.error(error)
        self.attempts += 1
        now = get_current_time()
        setters = [
            'locked_at = null',
            'locked_by = null',
            'attempts = %d' % self.attempts,
            'last_error = %s'
        ]
        values = [
            error
        ]
        if self.attempts >= self.max_attempts:
            # set failed_at = now
            setters.append('failed_at = %s')
            values.append(now)
            self.failure(error)
        else:
            # set new exponential run_at
            setters.append('run_at = %s')
            delta = (self.attempts**4) + 5
            values.append(str(now + get_time_delta(seconds=delta)))
        query = 'UPDATE delayed_jobs SET %s WHERE id = %d' % \
            (', '.join(setters), self.job_id)
        self.logger.debug('set error query: %s' % query)
        self.logger.debug('set error values: %s' % str(values))
        self.database.cursor().execute(query, tuple(values))
        self.database.commit()

    def remove(self):
        self.logger.debug('Job %d finished successfully' % self.job_id)
        query = 'DELETE FROM delayed_jobs WHERE id = %d' % self.job_id
        self.database.cursor().execute(query)
        self.database.commit()
