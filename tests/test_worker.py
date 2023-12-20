import datetime
from unittest import TestCase
from unittest.mock import patch, MagicMock
from pyworker.worker import Worker, TerminatedException

class TestWorker(TestCase):
    @patch('pyworker.worker.DBConnector')
    def setUp(self, mock_db):
        self.worker = Worker('dummy')
        self.mock_db = mock_db
        mock_db.connect = MagicMock()
        mock_db.disconnect = MagicMock()
        mocked_run_at = datetime.datetime(2023, 10, 7, 0, 0, 1)
        self.mocked_now = datetime.datetime(2023, 10, 7, 0, 0, 10)
        self.mocked_latency = 9 # seconds: mocked_now - mocked_run_at
        self.mock_job = MagicMock(
            abstract=False,
            job_id=1,
            job_name='test_job',
            queue='default',
            attempts=0,
            run_at=mocked_run_at)
        self.mock_extra_fields = {
            'extra_field1_str': 'extra_field1_value',
            'extra_field2_int': 100,
            'extra_field3_float': 1.1,
            'extra_field4_bool': True,
            'extra_field5_bool': False,
            'extra_field6_json': {'a': [1, 2, 3]},
            'extra_field7_none': None
        }

    def tearDown(self):
        pass

    #********** __init__ tests **********#

    @patch('pyworker.worker.os.uname', return_value=['pytest', 'localhost'])
    @patch('pyworker.worker.os.getpid', return_value=1234)
    @patch('pyworker.worker.DBConnector')
    def test_worker_init(self, mock_db, *_):
        worker = Worker('dummy')

        self.assertEqual(worker.database, mock_db.return_value)
        self.assertEqual(worker.sleep_delay, 10)
        self.assertEqual(worker.max_attempts, 3)
        self.assertEqual(worker.max_run_time, 3600)
        self.assertEqual(worker.queue_names, 'default')
        self.assertEqual(worker.name, 'host:localhost pid:1234')
        self.assertIsNone(worker.extra_delayed_job_fields)
        self.assertIsNone(worker.reporter)

    @patch('pyworker.worker.DBConnector')
    def test_worker_init_with_extra_delayed_job_fields(self, *_):
        worker = Worker('dummy', extra_delayed_job_fields=self.mock_extra_fields.keys())

        self.assertEqual(worker.extra_delayed_job_fields, self.mock_extra_fields.keys())

    @patch('pyworker.worker.DBConnector')
    @patch('pyworker.worker.os.environ', {
        'NEW_RELIC_LICENSE_KEY': 'test', 'NEW_RELIC_APP_NAME': 'test'})
    @patch('pyworker.worker.Reporter')
    def test_worker_init_with_reporter(self, mock_reporter, *_):
        mock_reporter.return_value = MagicMock()
        worker = Worker('dummy', reported_attributes_prefix='test_prefix')

        self.assertEqual(worker.reporter, mock_reporter.return_value)
        mock_reporter.assert_called_once_with(
            attribute_prefix='test_prefix', logger=worker.logger)

    #********** .run tests **********#

    @patch('pyworker.worker.Worker.get_job', return_value=None)
    # make sleep raise an exception to stop the loop
    @patch('pyworker.worker.time.sleep', side_effect=TerminatedException('SIGTERM'))
    def test_worker_run_connects_to_and_disconnects_from_database(self, *_):
        self.worker.run()

        self.worker.database.connect.assert_called_once_with()
        self.worker.database.disconnect.assert_called_once_with()

    @patch('pyworker.worker.Worker.get_job', return_value=None)
    @patch('pyworker.worker.time.sleep', side_effect=TerminatedException('SIGTERM'))
    def test_worker_run_shuts_down_reporter(self, *_):
        self.worker.reporter = MagicMock()

        self.worker.run()

        self.worker.reporter.shutdown.assert_called_once_with()

    @patch('pyworker.worker.time.sleep', side_effect=TerminatedException('SIGTERM'))
    @patch('pyworker.worker.Worker.get_job', return_value=None)
    def test_worker_run_when_no_jobs_found_sleeps(self, mock_get_job, mock_time_sleep):
        self.worker.run()

        mock_get_job.assert_called_once_with()
        mock_time_sleep.assert_called_once_with(self.worker.sleep_delay)

    @patch('pyworker.worker.Worker.handle_job', side_effect=TerminatedException('SIGTERM'))
    @patch('pyworker.worker.Worker.get_job', return_value=MagicMock())
    def test_worker_run_when_job_found_handles_job(self, mock_get_job, mock_handle_job):
        self.worker.run()

        mock_get_job.assert_called_once_with()
        mock_handle_job.assert_called_once_with(mock_get_job.return_value)

    #********** .handle_job tests **********#

    def assert_instrument_context_reports_custom_attributes(self, job, reporter):
        reporter.recorder.assert_called_once()
        reporter.report.assert_any_call(
            job_id=job.job_id,
            job_name=job.job_name,
            job_queue=job.queue,
            job_latency=self.mocked_latency,
            job_attempts=job.attempts
        )
        if job.extra_fields is not None:
            reporter.report.assert_any_call(**job.extra_fields)

    def test_worker_handle_job_when_job_is_none_does_nothing(self):
        self.worker.handle_job(None) # no error raised

    def test_worker_handle_job_when_job_is_unsupported_type_sets_error(self):
        job = self.mock_job
        job.abstract = True

        self.worker.handle_job(job)

        job.set_error_unlock.assert_called_once()
        assert 'Unsupported Job' in job.set_error_unlock.call_args[0][0]

    @patch('pyworker.worker.get_current_time')
    def test_worker_handle_job_when_job_is_unsupported_type_reports_error(
            self, get_current_time):
        get_current_time.return_value = self.mocked_now
        job = self.mock_job
        job.abstract = True
        job.set_error_unlock.return_value = False
        reporter = MagicMock()
        self.worker.reporter = reporter

        self.worker.handle_job(job)

        self.assert_instrument_context_reports_custom_attributes(job, reporter)
        reporter.record_exception.assert_called_once()
        reporter.report.assert_any_call(job_failure=False)
        reporter.report_raw.assert_any_call(error=True)

    @patch('pyworker.worker.get_current_time')
    def test_worker_handle_job_when_job_is_unsupported_type_reports_extra_fields(
            self, get_current_time):
        get_current_time.return_value = self.mocked_now
        job = self.mock_job
        job.abstract = True
        job.extra_fields = self.mock_extra_fields
        reporter = MagicMock()
        self.worker.reporter = reporter

        self.worker.handle_job(job)

        self.assert_instrument_context_reports_custom_attributes(job, reporter)

    def test_worker_handle_job_calls_all_hooks_then_removes_from_queue(self):
        self.worker.handle_job(self.mock_job)

        self.mock_job.before.assert_called_once()
        self.mock_job.run.assert_called_once()
        self.mock_job.after.assert_called_once()
        self.mock_job.success.assert_called_once()

        self.mock_job.remove.assert_called_once()

    @patch('pyworker.worker.get_current_time')
    def test_worker_handle_job_when_no_errors_reports_success(
            self, get_current_time):
        get_current_time.return_value = self.mocked_now
        job = self.mock_job
        job.extra_fields = self.mock_extra_fields
        reporter = MagicMock()
        self.worker.reporter = reporter

        self.worker.handle_job(job)

        reporter.record_exception.assert_not_called()
        reporter.report.assert_any_call(job_failure=False)
        reporter.report_raw.assert_any_call(error=False)
        self.assert_instrument_context_reports_custom_attributes(job, reporter)

    def test_worker_handle_job_when_error_sets_error_and_unlocks_job(self):
        job = self.mock_job
        job.run.side_effect = Exception('test error')

        self.worker.handle_job(job)

        job.set_error_unlock.assert_called_once()
        assert 'test error' in job.set_error_unlock.call_args[0][0]
        job.remove.assert_not_called()

    @patch('pyworker.worker.get_current_time')
    def test_worker_handle_job_when_error_reports_error(self,
            get_current_time):
        get_current_time.return_value = self.mocked_now
        job = self.mock_job
        job.set_error_unlock.return_value = False
        job.run.side_effect = Exception('test error')
        job.extra_fields = self.mock_extra_fields
        reporter = MagicMock()
        self.worker.reporter = reporter

        self.worker.handle_job(job)

        self.assert_instrument_context_reports_custom_attributes(job, reporter)
        reporter.record_exception.assert_called_once()
        reporter.report.assert_any_call(job_failure=False)
        reporter.report_raw.assert_any_call(error=True)
        job.remove.assert_not_called()

    @patch('pyworker.worker.get_current_time')
    def test_worker_handle_job_when_permanent_error_reports_failure(
            self, get_current_time):
        get_current_time.return_value = self.mocked_now
        job = self.mock_job
        job.set_error_unlock.return_value = True
        job.run.side_effect = Exception('test error')
        job.extra_fields = self.mock_extra_fields
        reporter = MagicMock()
        self.worker.reporter = reporter

        self.worker.handle_job(job)

        self.assert_instrument_context_reports_custom_attributes(job, reporter)
        reporter.record_exception.assert_called_once()
        reporter.report.assert_any_call(job_failure=True)
        reporter.report_raw.assert_any_call(error=True)
        job.remove.assert_not_called()

    def test_worker_handle_job_when_error_is_termination_error_bubbles_up(self):
        job = self.mock_job
        job.run.side_effect = TerminatedException('SIGTERM')

        with self.assertRaises(TerminatedException):
            self.worker.handle_job(job)
