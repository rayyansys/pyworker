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
        # flatten attributes
        attributes = self._flatten_attributes(attributes)
        # format attributes
        attributes = self._format_attributes(attributes)
        self.report_raw(**attributes)

    def report_raw(self, **attributes):
        # report to NewRelic
        self._report_newrelic(attributes)

    @contextmanager
    def recorder(self, name):
        with newrelic.agent.BackgroundTask(
                application=self._newrelic_app,
                name=name,
                group='DelayedJob') as task:
            yield task

    def shutdown(self):
        newrelic.agent.shutdown_agent()

    def record_exception(self, exc_info):
        newrelic.agent.notice_error(error=exc_info)

    def _flatten_attributes(self, attributes):
        # flatten nested dict attributes
        flattened_attributes = {}
        for key, value in attributes.items():
            if type(value) == dict:
                for nested_key, nested_value in value.items():
                    flattened_attributes[nested_key] = nested_value
            else:
                flattened_attributes[key] = value
        return flattened_attributes

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
