import re
import yaml

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
    def __init__(self, class_name, logger,
        job_id, attempts=0, attributes=None):
        super(Job, self).__init__()
        self.class_name = class_name
        self.logger = logger
        self.job_id = job_id
        self.attempts = attempts
        self.attributes = attributes

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, str(self.__dict__))

    @classmethod
    def from_row(cls, job_row, logger):
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
                job_id=job_id, attempts=attempts)

        attributes = extract_attributes(handler[2:])
        logger.debug("Found attributes: %s" % str(attributes))

        stripped = '\n'.join(['object:', '  attributes:'] + attributes)
        payload = yaml.load(stripped)
        logger.debug("payload object: %s" % str(payload))

        return target_class(class_name=class_name, logger=logger,
            job_id=job_id, attempts=attempts,
            attributes=payload['object']['attributes'])

    def before(self):
        self.logger.debug("Running Job.before hook")

    def after(self):
        self.logger.debug("Running Job.after hook")

