from unittest import TestCase
from unittest.mock import patch, MagicMock
from pyworker.reporter import Reporter


class TestReporter(TestCase):

    def setUp(self) -> None:
        return super().setUp()

    def tearDown(self) -> None:
        return super().tearDown()

    #********** __init__ tests **********#

    @patch('pyworker.reporter.newrelic.agent')
    def test_reporter_init_initializes_newrelic(self, newrelic_agent):
        newrelic_app = MagicMock()
        newrelic_agent.register_application.return_value = newrelic_app
        reporter = Reporter(attribute_prefix='test_prefix')

        self.assertEqual(reporter._prefix, 'test_prefix')
        self.assertIsNone(reporter._logger)
        self.assertEqual(reporter._newrelic_app, newrelic_app)
        newrelic_agent.register_application.assert_called_once_with()
        newrelic_agent.initialize.assert_called_once_with()

    #********** .report tests **********#

    @patch('pyworker.reporter.Reporter._report_newrelic')
    @patch('pyworker.reporter.Reporter._format_attributes')
    @patch('pyworker.reporter.Reporter._flatten_attributes')
    def test_reporter_report_formats_attributes_and_reports_to_newrelic(self,
            mock_flatten_attributes, mock_format_attributes, mock_report_newrelic):
        reporter = Reporter()
        mock_flatten_attributes.return_value = {'flattened_attribute': '1'}
        mock_format_attributes.return_value = {'formatted_attribute': '1'}
        reporter.report(test_attribute={'nested_attribute': 1})

        mock_flatten_attributes.assert_called_once_with({'test_attribute': {'nested_attribute': 1}})
        mock_format_attributes.assert_called_once_with({'flattened_attribute': '1'})
        mock_report_newrelic.assert_called_once_with({'formatted_attribute': '1'})

    #********** .report_raw tests **********#

    @patch('pyworker.reporter.Reporter._report_newrelic')
    def test_reporter_report_raw_reports_to_newrelic(self, mock_report_newrelic):
        reporter = Reporter()
        reporter.report_raw(test_attribute=1)

        mock_report_newrelic.assert_called_once_with({'test_attribute': 1})

    #********** .recorder tests **********#

    @patch('pyworker.reporter.newrelic.agent')
    def test_reporter_recorder_returns_newrelic_background_task(self, newrelic_agent):
        reporter = Reporter()

        with reporter.recorder('test_name'):
            newrelic_agent.BackgroundTask.assert_called_once_with(
                application=reporter._newrelic_app,
                name='test_name',
                group='DelayedJob'
            )

    #********** .shutdown tests **********#

    @patch('pyworker.reporter.newrelic.agent')
    def test_reporter_shutdown_calls_newrelic_shutdown_agent(self, newrelic_agent):
        reporter = Reporter()
        reporter.shutdown()

        newrelic_agent.shutdown_agent.assert_called_once_with()

    #********** .record_exception tests **********#

    @patch('pyworker.reporter.newrelic.agent')
    def test_reporter_record_exception_calls_newrelic_notice_error(self, newrelic_agent):
        reporter = Reporter()
        mock_exc_info = ('test_exception', 'test_value', 'test_traceback')
        reporter.record_exception(mock_exc_info)

        newrelic_agent.notice_error.assert_called_once_with(error=mock_exc_info)

    #********** ._flatten_attributes tests **********#

    def test_reporter_flatten_attributes_flattens_nested_dict_attributes(self):
        reporter = Reporter()

        self.assertEqual(
            reporter._flatten_attributes({
                'test_key1': 'test_value1',
                'test_key2': 2,
                'test_key3': 3.0,
                'test_key4': True,
                'test_key5': {'test_key6': [1, 'test_value6']},
                'test_key7': None,
                None: 'test_value8'
            }),
            {
                'test_key1': 'test_value1',
                'test_key2': 2,
                'test_key3': 3.0,
                'test_key4': True,
                'test_key6': [1, 'test_value6'],
                'test_key7': None,
                None: 'test_value8'
            }
        )

    #********** ._format_attributes tests **********#

    def test_reporter_format_attributes_prefixes_and_camel_cases_keys_and_converts_values(self):
        reporter = Reporter(attribute_prefix='prefix.')

        self.assertEqual(
            reporter._format_attributes({
                'test_key1': 'test_value1',
                'test_key2': 2,
                'test_key3': 3.0,
                'test_key4': True,
                'test_key5': [1, 'test_value6'],
                'test_key7': None,
                None: 'test_value8'
            }),
            {
                'prefix.testKey1': 'test_value1',
                'prefix.testKey2': 2,
                'prefix.testKey3': 3.0,
                'prefix.testKey4': True,
                'prefix.testKey5': '[1, "test_value6"]'
            }
        )

    #********** ._to_camel_case tests **********#

    def test_reporter_to_camel_case_converts_to_camel_case(self):
        reporter = Reporter()

        self.assertEqual(
            reporter._to_camel_case('test-key'),
            'testKey'
        )
        self.assertEqual(
            reporter._to_camel_case('test_key'),
            'testKey'
        )
        self.assertEqual(
            reporter._to_camel_case('test key'),
            'testKey'
        )

    #********** ._convert_value tests **********#

    def test_reporter_convert_value_converts_value_to_json_if_not_supported(self):
        reporter = Reporter()

        self.assertEqual(
            reporter._convert_value({'test_key': 'test_value'}),
            '{"test_key": "test_value"}'
        )
        self.assertEqual(
            reporter._convert_value('test_value'),
            'test_value'
        )
        self.assertEqual(
            reporter._convert_value(1),
            1
        )
        self.assertEqual(
            reporter._convert_value(1.0),
            1.0
        )
        self.assertEqual(
            reporter._convert_value(True),
            True
        )
        self.assertEqual(
            reporter._convert_value(False),
            False
        )
        self.assertEqual(
            reporter._convert_value(None),
            'null'
        )

    #********** ._report_newrelic tests **********#

    @patch('pyworker.reporter.newrelic.agent')
    def test_reporter_report_newrelic_calls_newrelic_record_exception(self, newrelic_agent):
        reporter = Reporter()
        reporter._report_newrelic({
            'test_key1': 'test_value',
            'test_key2': 2
        })

        newrelic_agent.add_custom_attributes.assert_called_once_with([
            ('test_key1', 'test_value'),
            ('test_key2', 2)
        ])
