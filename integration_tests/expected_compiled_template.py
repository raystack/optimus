from typing import Any, Callable, Dict, Optional
from datetime import datetime, timedelta, timezone

from airflow.models import DAG, Variable, DagRun, DagModel, TaskInstance, BaseOperator, XCom, XCOM_RETURN_KEY
from airflow.kubernetes.secret import Secret
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import provide_session
from airflow.configuration import conf
from airflow.utils.state import State
from airflow.utils.weight_rule import WeightRule

from __lib import alert_failed_to_slack, SuperKubernetesPodOperator, SuperExternalTaskSensor, \
    SlackWebhookOperator, CrossTenantDependencySensor


SECRET_NAME = Variable.get("secret_name", "optimus-google-credentials")
SECRET_KEY = Variable.get("secret_key", "auth.json")
SECRET_VOLUME_PATH = '/opt/optimus/secrets/'
SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS = int(Variable.get("sensor_poke_interval_in_secs", default_var=15 * 60))
SENSOR_DEFAULT_TIMEOUT_IN_SECS = int(Variable.get("sensor_timeout_in_secs", default_var=15 * 60 * 60))
DAG_RETRIES = int(Variable.get("dag_retries", default_var=3))
DAG_RETRY_DELAY = int(Variable.get("dag_retry_delay_in_secs", default_var=5 * 60))

gcloud_credentials_path = '{}{}'.format(SECRET_VOLUME_PATH, SECRET_KEY)
gcloud_secret = Secret(
    'volume',
    SECRET_VOLUME_PATH,
    SECRET_NAME,
    SECRET_KEY)

default_args = {
    "owner": "mee@mee",
    "depends_on_past": False ,
    "retries": DAG_RETRIES,
    "retry_delay": timedelta(seconds=DAG_RETRY_DELAY),
    "priority_weight": 2000,
    "start_date": datetime.strptime("2000-11-11", "%Y-%m-%d"),
    "end_date": datetime.strptime("2020-11-11","%Y-%m-%d"),
    "on_failure_callback": alert_failed_to_slack,
    "weight_rule": WeightRule.ABSOLUTE
}

dag = DAG(
    dag_id="foo",
    default_args=default_args,
    schedule_interval="* * * * *",
    catchup = True
)

transformation_bq = SuperKubernetesPodOperator(
    image_pull_policy="Always",
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "{}".format("odpf/namespace/image:latest"),
    cmds=[],
    name="bq",
    task_id="bq",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    secrets=[gcloud_secret],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": gcloud_credentials_path,
        "JOB_NAME":'foo', "OPTIMUS_HOSTNAME": 'http://airflow.io',
        "JOB_DIR":'/data', "PROJECT":'foo-project',
        "TASK_TYPE":'transformation', "TASK_NAME": "bq",
        "SCHEDULED_AT":'{{ next_execution_date }}',
    },
    reattach_on_restart=True,
)

# hooks loop start

hook_transporter = SuperKubernetesPodOperator(
    image_pull_policy="Always",
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "odpf/namespace/hook-image:latest",
    cmds=[],
    name="hook_transporter",
    task_id="hook_transporter",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    secrets=[gcloud_secret],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": gcloud_credentials_path,
        "JOB_NAME":'foo', "OPTIMUS_HOSTNAME": 'http://airflow.io',
        "JOB_DIR":'/data', "PROJECT":'foo-project',
        "TASK_TYPE":'hook', "TASK_NAME": "transporter",
        "SCHEDULED_AT":'{{ next_execution_date }}',
        # rest of the env vars are pulled from the container by making a GRPC call to optimus
   },
   reattach_on_restart=True,
)
hook_predator = SuperKubernetesPodOperator(
    image_pull_policy="Always",
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "odpf/namespace/predator-image:latest",
    cmds=[],
    name="hook_predator",
    task_id="hook_predator",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    secrets=[gcloud_secret],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": gcloud_credentials_path,
        "JOB_NAME":'foo', "OPTIMUS_HOSTNAME": 'http://airflow.io',
        "JOB_DIR":'/data', "PROJECT":'foo-project',
        "TASK_TYPE":'hook', "TASK_NAME": "predator",
        "SCHEDULED_AT":'{{ next_execution_date }}',
        # rest of the env vars are pulled from the container by making a GRPC call to optimus
   },
   reattach_on_restart=True,
)
# hooks loop ends


# create upstream sensors
wait_foo__dash__intra__dash__dep__dash__job = SuperExternalTaskSensor(
    external_dag_id = "foo-intra-dep-job",
    window_size = 1,
    window_offset = 0,
    window_truncate_upto = "d",
    task_id = "wait-foo-intra-dep-job-bq",
    poke_interval = SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout = SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    dag=dag
)
wait_foo__dash__inter__dash__dep__dash__job = CrossTenantDependencySensor(
    optimus_host="http://airflow.io",
    optimus_project="foo-external-project",
    optimus_job="foo-inter-dep-job",
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait-foo-inter-dep-job-bq",
    dag=dag
)

# arrange inter task dependencies
####################################

# upstream sensors -> base transformation task
wait_foo__dash__intra__dash__dep__dash__job >> transformation_bq
wait_foo__dash__inter__dash__dep__dash__job >> transformation_bq

# set inter-dependencies between task and hooks
hook_transporter >> transformation_bq
transformation_bq >> hook_predator

# set inter-dependencies between hooks and hooks
hook_transporter >> hook_predator
