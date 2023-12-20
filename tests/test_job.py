import datetime
from unittest import TestCase
from unittest.mock import patch, MagicMock
from pyworker.job import Job, get_current_time, get_time_delta


class RegisteredJob(Job): # matching the registered class fixture
    def run(self):
        pass


class TestJob(TestCase):
    def setUp(self):
        self.mock_job_id = 1
        self.mock_attempts = 0
        self.mock_run_at = datetime.datetime(2023, 10, 7, 0, 0, 1)
        self.mock_queue = 'default'
        self.mock_max_attempts = 5
        self.mock_now = datetime.datetime(2023, 10, 7, 0, 0, 0)
        self.mock_extra_fields = {
            'extra_field1': 'extra_field1_value',
            'extra_field2': 100,
            'extra_field3': True,
            'extra_field4': {'a': [1, 2, 3]},
            'extra_field5': None
        }

    def tearDown(self):
        pass

    def load_fixture(self, filename):
        with open('tests/fixtures/%s' % filename) as f:
            return f.read()

    def load_job(self, filename):
        mock_handler = self.load_fixture(filename)
        mock_row = (
            self.mock_job_id,
            self.mock_attempts,
            self.mock_run_at,
            self.mock_queue,
            mock_handler
        )
        return Job.from_row(mock_row,
                            self.mock_max_attempts,
                            MagicMock(), MagicMock())

    def load_job_with_extra_fields(self, filename):
        mock_handler = self.load_fixture(filename)
        mock_row = (
            self.mock_job_id,
            self.mock_attempts,
            self.mock_run_at,
            self.mock_queue,
            mock_handler,
            *self.mock_extra_fields.values()
        )
        return Job.from_row(mock_row,
                            self.mock_max_attempts,
                            MagicMock(), MagicMock(),
                            extra_fields=self.mock_extra_fields.keys())

    def load_unregistered_job(self):
        return self.load_job('handler_unregistered.yaml')

    def load_unregistered_job_with_extra_fields(self):
        return self.load_job_with_extra_fields('handler_unregistered.yaml')

    def load_unregistered_job_with_reporter(self, reporter):
        job = self.load_unregistered_job()
        job.reporter = reporter
        return job

    def load_registered_job(self):
        job = self.load_job('handler_registered.yaml')
        job.error = MagicMock()
        job.failure = MagicMock()
        job._update_job = MagicMock()
        return job

    def load_registered_job_with_extra_fields(self):
        return self.load_job_with_extra_fields('handler_registered.yaml')

    def load_registered_job_with_reporter(self, reporter):
        job = self.load_registered_job()
        job.reporter = reporter
        return job

    def load_registered_job_with_attempts_exceeded(self):
        job = self.load_registered_job()
        job.attempts = self.mock_max_attempts - 1
        return job

    #********** .from_row tests **********#

    def test_from_row_when_unregistered_class_returns_abstract_job_instance(self):
        job = self.load_unregistered_job()

        self.assertEqual(job.class_name, 'UnregisteredJob')
        self.assertEqual(job.abstract, True)

    def test_from_row_when_unregistered_class_returns_job_instance_without_attributes(self):
        job = self.load_unregistered_job()

        self.assertEqual(job.job_id, self.mock_job_id)
        self.assertEqual(job.attempts, self.mock_attempts)
        self.assertEqual(job.run_at, self.mock_run_at)
        self.assertEqual(job.queue, self.mock_queue)
        self.assertEqual(job.max_attempts, self.mock_max_attempts)
        self.assertIsNone(job.extra_fields)
        self.assertIsNone(job.attributes)
        self.assertIsNone(job.reporter)

    def test_from_row_when_unregistered_class_returns_job_instance_with_extra_fields(self):
        job = self.load_unregistered_job_with_extra_fields()

        self.assertDictEqual(job.extra_fields, self.mock_extra_fields)

    def test_from_row_when_unregistered_class_returns_abstract_job_instance_with_reporter(self):
        mock_reporter = MagicMock()
        job = self.load_unregistered_job_with_reporter(mock_reporter)

        self.assertEqual(job.reporter, mock_reporter)

    def test_from_row_when_registered_class_returns_concrete_job_instance(self):
        job = self.load_registered_job()

        self.assertEqual(job.class_name, 'RegisteredJob')
        self.assertEqual(job.abstract, False)

    def test_from_row_when_registered_class_returns_job_instance_with_attributes(self):
        job = self.load_registered_job()

        self.assertEqual(job.job_id, self.mock_job_id)
        self.assertEqual(job.attempts, self.mock_attempts)
        self.assertEqual(job.run_at, self.mock_run_at)
        self.assertEqual(job.queue, self.mock_queue)
        self.assertEqual(job.max_attempts, self.mock_max_attempts)
        self.assertIsNone(job.extra_fields)
        # below attributes match the registered class fixture
        self.assertDictEqual(job.attributes, {
            'id': 100,
            'title': 'review title',
            'description': 'review description\nmultiline\n',
            'total_articles': 1000,
            'is_blind': True
        })
        self.assertIsNone(job.reporter)

    def test_from_row_when_registered_class_returns_job_instance_with_extra_fields(self):
        job = self.load_registered_job_with_extra_fields()

        self.assertDictEqual(job.extra_fields, self.mock_extra_fields)

    def test_from_row_when_registered_class_returns_concrete_job_instance_with_reporter(self):
        mock_reporter = MagicMock()
        job = self.load_registered_job_with_reporter(mock_reporter)

        self.assertEqual(job.reporter, mock_reporter)

    #********** .set_error_unlock tests **********#

    def assert_job_updated_field(self, job, field, value):
        job._update_job.assert_called_once()
        setters = job._update_job.call_args[0][0]
        values = job._update_job.call_args[0][1]
        index = setters.index("%s = %%s" % field)
        assert index >= 0
        assert values[index] == value

    def assert_job_non_updated_field(self, job, field):
        job._update_job.assert_called_once()
        setters = job._update_job.call_args[0][0]
        assert "%s = %%s" % field not in setters

    def assert_job_updated_run_at(self, job, attempts, expected_value):
        job.attempts = attempts
        expected_value = expected_value.strftime('%Y-%m-%d %H:%M:%S')

        job.set_error_unlock('some error')

        self.assert_job_updated_field(job, 'run_at', expected_value)

    # max attempts not exceeded

    ## error and failure hooks
    def test_set_error_unlock_if_max_attempts_not_exceeded_calls_error_hook_only(self):
        job = self.load_registered_job()

        job.set_error_unlock('some error')

        job.error.assert_called_once_with('some error')
        job.failure.assert_not_called()

    ## attempts
    def test_set_error_unlock_if_max_attempts_not_exceeded_increments_attempts(self):
        job = self.load_registered_job()
        expected_value = job.attempts + 1

        job.set_error_unlock('some error')

        self.assert_job_updated_field(job, 'attempts', expected_value)

    ## run_at
    @patch('pyworker.job.get_current_time')
    def test_set_error_unlock_if_max_attempts_not_exceeded_updates_run_at_exponentially_when_attempts_0(
            self, mock_get_current_time):
        mock_get_current_time.return_value = self.mock_now
        job = self.load_registered_job()

        self.assert_job_updated_run_at(job, attempts=0, expected_value=datetime.datetime(2023, 10, 7, 0, 0, 6))

    @patch('pyworker.job.get_current_time')
    def test_set_error_unlock_if_max_attempts_not_exceeded_updates_run_at_exponentially_when_attempts_1(
            self, mock_get_current_time):
        mock_get_current_time.return_value = self.mock_now
        job = self.load_registered_job()

        self.assert_job_updated_run_at(job, attempts=1, expected_value=datetime.datetime(2023, 10, 7, 0, 0, 21))

    @patch('pyworker.job.get_current_time')
    def test_set_error_unlock_if_max_attempts_not_exceeded_updates_run_at_exponentially_when_attempts_2(
            self, mock_get_current_time):
        mock_get_current_time.return_value = self.mock_now
        job = self.load_registered_job()

        self.assert_job_updated_run_at(job, attempts=2, expected_value=datetime.datetime(2023, 10, 7, 0, 1, 26))

    @patch('pyworker.job.get_current_time')
    def test_set_error_unlock_if_max_attempts_not_exceeded_updates_run_at_exponentially_when_attempts_3(
            self, mock_get_current_time):
        mock_get_current_time.return_value = self.mock_now
        job = self.load_registered_job()

        self.assert_job_updated_run_at(job, attempts=3, expected_value=datetime.datetime(2023, 10, 7, 0, 4, 21))

    ## failed_at
    @patch('pyworker.job.get_current_time')
    def test_set_error_unlock_if_max_attempts_not_exceeded_does_not_update_failed_at(
            self, mock_get_current_time):
        mock_get_current_time.return_value = self.mock_now
        job = self.load_registered_job()

        job.set_error_unlock('some error')

        self.assert_job_non_updated_field(job, 'failed_at')

    ## locked_at
    def test_set_error_unlock_if_max_attempts_not_exceeded_nullifies_job_locked_at(self):
        job = self.load_registered_job()

        job.set_error_unlock('some error')

        self.assert_job_updated_field(job, 'locked_at', None)

    ## locked_by
    def test_set_error_unlock_if_max_attempts_not_exceeded_nullifies_job_locked_by(self):
        job = self.load_registered_job()

        job.set_error_unlock('some error')

        self.assert_job_updated_field(job, 'locked_by', None)

    ## last_error
    def test_set_error_unlock_if_max_attempts_not_exceeded_updates_last_error(self):
        job = self.load_registered_job()

        job.set_error_unlock('some error')

        self.assert_job_updated_field(job, 'last_error', 'some error')

    ## returns
    def test_set_error_unlock_if_max_attempts_not_exceeded_returns_false(self):
        job = self.load_registered_job()

        self.assertFalse(job.set_error_unlock('some error'))

    ## max attempts exceeded

    ## error and failure hooks
    def test_set_error_unlock_if_max_attempts_exceeded_calls_error_and_failure_hooks(self):
        job = self.load_registered_job_with_attempts_exceeded()

        job.set_error_unlock('some error')

        job.error.assert_called_once_with('some error')
        job.failure.assert_called_once_with('some error')

    ## attempts
    def test_set_error_unlock_if_max_attempts_exceeded_increments_attempts(self):
        job = self.load_registered_job_with_attempts_exceeded()
        expected_value = job.attempts + 1

        job.set_error_unlock('some error')

        self.assert_job_updated_field(job, 'attempts', expected_value)

    ## run_at
    def test_set_error_unlock_if_max_attempts_exceeded_does_not_update_run_at(self):
        job = self.load_registered_job_with_attempts_exceeded()

        job.set_error_unlock('some error')

        self.assert_job_non_updated_field(job, 'run_at')

    ## failed_at
    @patch('pyworker.job.get_current_time')
    def test_set_error_unlock_if_max_attempts_exceeded_updates_failed_at(
            self, mock_get_current_time):
        mock_get_current_time.return_value = self.mock_now
        job = self.load_registered_job_with_attempts_exceeded()

        job.set_error_unlock('some error')

        self.assert_job_updated_field(job, 'failed_at', mock_get_current_time.return_value)

    ## locked_at
    def test_set_error_unlock_if_max_attempts_exceeded_nullifies_job_locked_at(self):
        job = self.load_registered_job_with_attempts_exceeded()

        job.set_error_unlock('some error')

        self.assert_job_updated_field(job, 'locked_at', None)

    ## locked_by
    def test_set_error_unlock_if_max_attempts_exceeded_nullifies_job_locked_by(self):
        job = self.load_registered_job_with_attempts_exceeded()

        job.set_error_unlock('some error')

        self.assert_job_updated_field(job, 'locked_by', None)

    ## last_error
    def test_set_error_unlock_if_max_attempts_exceeded_updates_last_error(self):
        job = self.load_registered_job_with_attempts_exceeded()

        job.set_error_unlock('some error')

        self.assert_job_updated_field(job, 'last_error', 'some error')

    ## returns
    def test_set_error_unlock_if_max_attempts_exceeded_returns_true(self):
        job = self.load_registered_job_with_attempts_exceeded()

        self.assertTrue(job.set_error_unlock('some error'))

