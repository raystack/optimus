import unittest
import sys
sys.path.insert(1, '../../../../resources/pack/templates/scheduler/airflow_1')

import importlib.util


def load_file_as_module(filepath):
    spec = importlib.util.spec_from_file_location("dag", filepath)
    compiled_dag_lib = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(compiled_dag_lib)
    return compiled_dag_lib


class TestCompiledAirflowTemplate(unittest.TestCase):

    def test_should_run_compiled_airflow_template(self):
        compiled_dag_lib = load_file_as_module('../../../../integration_tests/airflow_1/expected_compiled_template.py')

        dag = compiled_dag_lib.dag

        self.assertEqual('foo', dag.dag_id)

        self.assertEqual(5, len(dag.tasks))

        self.assertEqual("bq", dag.tasks[0].task_id)
        self.assertEqual("hook_transporter", dag.tasks[1].task_id)
        self.assertEqual("hook_predator", dag.tasks[2].task_id)
        self.assertEqual("wait_foo-intra-dep-job-bq", dag.tasks[3].task_id)
        self.assertEqual("wait_foo-inter-dep-job-bq", dag.tasks[4].task_id)

        self.assertEqual("SuperKubernetesPodOperator", dag.tasks[0].__class__.__name__)
        self.assertEqual("SuperKubernetesPodOperator", dag.tasks[1].__class__.__name__)
        self.assertEqual("SuperKubernetesPodOperator", dag.tasks[2].__class__.__name__)
        self.assertEqual("SuperExternalTaskSensor", dag.tasks[3].__class__.__name__)
        self.assertEqual("CrossTenantDependencySensor", dag.tasks[4].__class__.__name__)
