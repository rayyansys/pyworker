import datetime
from unittest import TestCase
from unittest.mock import patch, MagicMock
from pyworker.job import Job, get_current_time, get_time_delta


class RegisteredJob(Job): # matching the registered class fixture
    def run(self):
        pass


class TestJob(TestCase):
    def setUp(self):
        self.mock_run_at = datetime.datetime(2023, 10, 7, 0, 0, 1)

    def tearDown(self):
        pass

    def load_fixture(self, filename):
        with open('tests/fixtures/%s' % filename) as f:
            return f.read()

    #********** .from_row tests **********#

    def test_from_row_when_unregistered_class_returns_abstract_job_instance(self):
        mock_handler = self.load_fixture('handler_unregistered.yaml')
        mock_row = (1, 0, self.mock_run_at, 'default', mock_handler)
        job = Job.from_row(mock_row, 1, MagicMock(), MagicMock())

        self.assertEqual(job.class_name, 'UnregisteredJob')
        self.assertEqual(job.abstract, True)

    def test_from_row_when_registered_class_returns_concrete_job_instance(self):
        mock_handler = self.load_fixture('handler_registered.yaml')
        mock_row = (1, 0, self.mock_run_at, 'default', mock_handler)
        job = Job.from_row(mock_row, 1, MagicMock(), MagicMock())

        self.assertEqual(job.class_name, 'RegisteredJob')
        self.assertEqual(job.abstract, False)

    def test_from_row_when_registered_class_returns_concrete_job_instance_with_attributes(self):
        mock_handler = self.load_fixture('handler_registered.yaml')
        mock_row = (1, 0, self.mock_run_at, 'default', mock_handler)
        job = Job.from_row(mock_row, 1, MagicMock(), MagicMock())

        self.assertEqual(job.job_id, 1)
        self.assertEqual(job.attempts, 0)
        self.assertEqual(job.run_at, self.mock_run_at)
        self.assertEqual(job.queue, 'default')
        self.assertEqual(job.max_attempts, 1)
        self.assertDictEqual(job.attributes, {
            'id': 100,
            'title': 'review title',
            'description': 'review description\nmultiline\n',
            'total_articles': 1000,
            'is_blind': True
        })

    #********** .set_error_unlock tests **********#
