import json
from contextlib import contextmanager
import newrelic.agent


class Reporter(object):

    def __init__(self, attribute_prefix='', logger=None):
        self._prefix = attribute_prefix
        self._logger = logger
        if self._logger:
            self._logger.info('Reporter: initializing NewRelic')
        newrelic.agent.initialize()
        self._newrelic_app = newrelic.agent.register_application()

    def report(self, **attributes):
        # format attributes
        attributes = self._format_attributes(attributes)
        # report to NewRelic
        self._report_newrelic(attributes)

    @contextmanager
    def recorder(self, name):
        return newrelic.agent.BackgroundTask(
            application=self._newrelic_app,
            name=name,
            group='DelayedJob')

    def shutdown(self):
        newrelic.agent.shutdown_agent()

    def record_exception(self, exception):
        newrelic.agent.record_exception(exception)

    def _format_attributes(self, attributes):
        # prefix then convert all attribute keys to camelCase
        # ensure values types are supported or json dump them
        return {
            self._prefix + self._to_camel_case(key): self._convert_value(value)
            for key, value in attributes.items()
            if key is not None and value is not None
        }

    @staticmethod
    def _to_camel_case(string):
        return string[0]+string.title()[1:].replace("-","").replace("_","").replace(" ","")

    @staticmethod
    def _convert_value(value):
        if type(value) not in [str, int, float, bool]:
            return json.dumps(value)
        return value

    def _report_newrelic(self, attributes):
        if self._logger:
            self._logger.debug('Reporter: reporting to NewRelic: %s' % attributes)
        # convert attributes dict to list of tuples
        attributes = list(attributes.items())
        newrelic.agent.add_custom_attributes(attributes)
