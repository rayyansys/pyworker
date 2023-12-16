from unittest import TestCase
from unittest.mock import patch, MagicMock
from pyworker.worker import Worker, TerminatedException

class TestWorker(TestCase):
    @patch('pyworker.worker.DBConnector')
    def setUp(self, mock_db):
        self.dbstring = 'postgres://user:pass@host:1234/db'
        self.worker = Worker('dummy')
        self.worker.newrelic_app = MagicMock()
        self.mock_db = mock_db
        mock_db.connect = MagicMock()
        mock_db.disconnect = MagicMock()

    def tearDown(self):
        pass

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

    @patch('pyworker.worker.Worker.get_job', return_value=None)
    # make sleep raise an exception to stop the loop
    @patch('pyworker.worker.time.sleep', side_effect=TerminatedException('SIGTERM'))
    def test_worker_run_connects_to_and_disconnects_from_database(self, *_):
        self.worker.run()

        self.worker.database.connect.assert_called_once_with()
        self.worker.database.disconnect.assert_called_once_with()

    @patch('pyworker.worker.Worker.get_job', return_value=None)
    @patch('pyworker.worker.time.sleep', side_effect=TerminatedException('SIGTERM'))
    @patch('pyworker.worker.newrelic.agent', return_value=MagicMock())
    def test_worker_run_shuts_down_newrelic_agent(self, newrelic_agent, *_):
        self.worker.run()

        newrelic_agent.shutdown_agent.assert_called_once_with()

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
