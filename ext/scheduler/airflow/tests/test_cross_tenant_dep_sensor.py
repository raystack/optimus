import unittest
import sys
sys.path.insert(1, '../../../../resources/pack/templates/scheduler/airflow_1')

from datetime import datetime
from __lib import CrossTenantDependencySensor, OptimusAPIClient

from unittest.mock import Mock


class TestCrossTenantDependencySensor(unittest.TestCase):
    base_project_response = {'project': {}, 'job': {'version': 1, 'name': 'pilotdata-integration.playground.characters', 'owner': 'sharma.ji', 'startDate': '2021-01-11', 'endDate': '', 'interval': '@daily', 'dependsOnPast': False, 'catchUp': True, 'taskName': 'bq2bq', 'config': {'DATASET': 'playground', 'JOB_LABELS': 'owner=optimus', 'LOAD_METHOD': 'REPLACE', 'PROJECT': 'pilotdata-integration', 'SQL_TYPE': 'STANDARD', 'TABLE': 'characters', 'TASK_TIMEZONE': 'UTC'}, 'windowSize': '24h', 'windowOffset': '24h', 'windowTruncateTo': 'd', 'dependencies': []}}

    def test_should_return_true_if_window_range_has_successful_runs(self):
        optimus_client_mock = Mock()
        optimus_client_mock.get_job_run_status.return_value = {'all': [{'state': 'failed', 'scheduledAt': '2021-01-25T00:00:00Z'}, {'state': 'success', 'scheduledAt': '2021-01-26T00:00:00Z'}, {'state': 'success', 'scheduledAt': '2021-01-27T11:12:41.681124Z'}]}
        optimus_client_mock.get_job_metadata.return_value = self.base_project_response
        optimus_client_mock.get_task_window.return_value = {'start': '2021-01-25T00:00:00Z', 'end': '2021-01-26T00:00:00Z'}

        sensor = CrossTenantDependencySensor(
            task_id='task',
            optimus_hostname="dummy-since-we-are-mocking",
            optimus_project="g-pilotdata-gl",
            optimus_job="pilotdata-integration.playground.characters",
        )
        sensor._optimus_client = optimus_client_mock # inject

        self.assertEqual(True, sensor.execute({"execution_date": datetime(2021, 1, 26, 0, 0, 0)}))

    def test_should_return_false_if_window_range_has_no_successful_runs(self):
        optimus_client_mock = Mock()
        optimus_client_mock.get_job_run_status.return_value = {'all': [{'state': 'failed', 'scheduledAt': '2021-01-25T00:00:00Z'}, {'state': 'success', 'scheduledAt': '2021-01-26T00:00:00Z'}, {'state': 'success', 'scheduledAt': '2021-01-27T11:12:41.681124Z'}]}
        optimus_client_mock.get_job_metadata.return_value = self.base_project_response
        optimus_client_mock.get_task_window.return_value = {'start': '2021-01-20T00:00:00Z', 'end': '2021-01-21T00:00:00Z'} # return window outside successful runs

        sensor = CrossTenantDependencySensor(
            task_id='task',
            optimus_hostname="dummy-since-we-are-mocking",
            optimus_project="g-pilotdata-gl",
            optimus_job="pilotdata-integration.playground.characters",
        )
        sensor._optimus_client = optimus_client_mock # inject
        self.assertEqual(False, sensor.execute({"execution_date": datetime(2021, 1, 26, 0, 0, 0)}))

    @unittest.skip("comment this if you want run this locally")
    def test_should_run_locally(self):
        sensor = CrossTenantDependencySensor(
            task_id='task',
            optimus_hostname="http://localhost:6666",
            optimus_project="g-pilotdata-gl",
            optimus_job="pilotdata-integration.playground.characters",
        )
        print(sensor.execute({"execution_date": datetime(2021, 1, 26, 0, 0, 0)}))
