# Code generated by optimus dev. DO NOT EDIT.

from datetime import datetime, timedelta

from airflow.configuration import conf
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.utils.weight_rule import WeightRule
from kubernetes.client import models as k8s

from ext.scheduler.airflow.__lib import JOB_START_EVENT_NAME, \
    JOB_END_EVENT_NAME, \
    log_success_event, \
    log_retry_event, \
    log_failure_event, \
    log_job_end, log_job_start
from ext.scheduler.airflow.__lib import optimus_sla_miss_notify, SuperKubernetesPodOperator, \
    SuperExternalTaskSensor

SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS = int(Variable.get("sensor_poke_interval_in_secs", default_var=15 * 60))
SENSOR_DEFAULT_TIMEOUT_IN_SECS = int(Variable.get("sensor_timeout_in_secs", default_var=15 * 60 * 60))
DAG_RETRIES = int(Variable.get("dag_retries", default_var=3))
DAG_RETRY_DELAY = int(Variable.get("dag_retry_delay_in_secs", default_var=5 * 60))
DAGRUN_TIMEOUT_IN_SECS = int(Variable.get("dagrun_timeout_in_secs", default_var=3 * 24 * 60 * 60))

default_args = {
    "params": {
        "project_name": "example-proj",
        "namespace": "billing",
        "job_name": "infra.billing.weekly-status-reports",
        "optimus_hostname": "http://optimus.example.com"
    },
    "pool": "billing",
    "owner": "infra-team@example.com",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=100),
    "retry_exponential_backoff": True,
    "priority_weight": 2000,
    "start_date": datetime.strptime("2022-11-10T05:02:00", "%Y-%m-%dT%H:%M:%S"),
    "end_date": datetime.strptime("2022-11-10T10:02:00", "%Y-%m-%dT%H:%M:%S"),
    "on_failure_callback": log_failure_event,
    "on_retry_callback": log_retry_event,
    "on_success_callback": log_success_event,
    "weight_rule": WeightRule.ABSOLUTE
}

# This job collects the billing information related to infrastructure
dag = DAG(
    dag_id="infra.billing.weekly-status-reports",
    default_args=default_args,
    schedule_interval="0 2 * * 0",
    sla_miss_callback=optimus_sla_miss_notify,
    catchup=True,
    dagrun_timeout=timedelta(seconds=DAGRUN_TIMEOUT_IN_SECS),
    tags=[
        "optimus",
    ]
)

publish_job_start_event = PythonOperator(
    task_id=JOB_START_EVENT_NAME,
    python_callable=log_job_start,
    provide_context=True,
    dag=dag
)

publish_job_end_event = PythonOperator(
    task_id=JOB_END_EVENT_NAME,
    python_callable=log_job_end,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag
)

resources = k8s.V1ResourceRequirements(
    limits={
        'memory': '2G',
        'cpu': '200m',
    },
)

transformation_bq__dash__bq = SuperKubernetesPodOperator(
    image_pull_policy="IfNotPresent",
    namespace=conf.get('kubernetes', 'namespace', fallback="default"),
    image="example.io/namespace/bq2bq-executor:latest",
    cmds=[],
    name="bq-bq",
    task_id="bq-bq",
    get_logs=True,
    dag=dag,
    depends_on_past=False,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=[
        k8s.V1EnvVar(name="JOB_NAME", value='infra.billing.weekly-status-reports'),
        k8s.V1EnvVar(name="OPTIMUS_HOST", value='http://optimus.example.com'),
        k8s.V1EnvVar(name="JOB_LABELS", value='orchestrator=optimus'),
        k8s.V1EnvVar(name="JOB_DIR", value='/data'),
        k8s.V1EnvVar(name="PROJECT", value='example-proj'),
        k8s.V1EnvVar(name="NAMESPACE", value='billing'),
        k8s.V1EnvVar(name="INSTANCE_TYPE", value='task'),
        k8s.V1EnvVar(name="INSTANCE_NAME", value='bq-bq'),
        k8s.V1EnvVar(name="SCHEDULED_AT", value='{{ next_execution_date }}'),
    ],
    sla=timedelta(seconds=7200),
    resources=resources,
    reattach_on_restart=True
)

# hooks loop start

hook_transporter = SuperKubernetesPodOperator(
    image_pull_policy="IfNotPresent",
    namespace=conf.get('kubernetes', 'namespace', fallback="default"),
    image="example.io/namespace/transporter-executor:latest",
    cmds=[],
    name="hook_transporter",
    task_id="hook_transporter",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=[
        k8s.V1EnvVar(name="JOB_NAME", value='infra.billing.weekly-status-reports'),
        k8s.V1EnvVar(name="OPTIMUS_HOST", value='http://optimus.example.com'),
        k8s.V1EnvVar(name="JOB_LABELS", value='orchestrator=optimus'),
        k8s.V1EnvVar(name="JOB_DIR", value='/data'),
        k8s.V1EnvVar(name="PROJECT", value='example-proj'),
        k8s.V1EnvVar(name="NAMESPACE", value='billing'),
        k8s.V1EnvVar(name="INSTANCE_TYPE", value='hook'),
        k8s.V1EnvVar(name="INSTANCE_NAME", value='transporter'),
        k8s.V1EnvVar(name="SCHEDULED_AT", value='{{ next_execution_date }}'),
        # rest of the env vars are pulled from the container by making a GRPC call to optimus
    ],
    resources=resources,
    reattach_on_restart=True
)

hook_predator = SuperKubernetesPodOperator(
    image_pull_policy="IfNotPresent",
    namespace=conf.get('kubernetes', 'namespace', fallback="default"),
    image="example.io/namespace/predator-image:latest",
    cmds=[],
    name="hook_predator",
    task_id="hook_predator",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=[
        k8s.V1EnvVar(name="JOB_NAME", value='infra.billing.weekly-status-reports'),
        k8s.V1EnvVar(name="OPTIMUS_HOST", value='http://optimus.example.com'),
        k8s.V1EnvVar(name="JOB_LABELS", value='orchestrator=optimus'),
        k8s.V1EnvVar(name="JOB_DIR", value='/data'),
        k8s.V1EnvVar(name="PROJECT", value='example-proj'),
        k8s.V1EnvVar(name="NAMESPACE", value='billing'),
        k8s.V1EnvVar(name="INSTANCE_TYPE", value='hook'),
        k8s.V1EnvVar(name="INSTANCE_NAME", value='predator'),
        k8s.V1EnvVar(name="SCHEDULED_AT", value='{{ next_execution_date }}'),
        # rest of the env vars are pulled from the container by making a GRPC call to optimus
    ],
    resources=resources,
    reattach_on_restart=True
)

hook_failureHook = SuperKubernetesPodOperator(
    image_pull_policy="IfNotPresent",
    namespace=conf.get('kubernetes', 'namespace', fallback="default"),
    image="example.io/namespace/failure-hook-image:latest",
    cmds=[],
    name="hook_failureHook",
    task_id="hook_failureHook",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=[
        k8s.V1EnvVar(name="JOB_NAME", value='infra.billing.weekly-status-reports'),
        k8s.V1EnvVar(name="OPTIMUS_HOST", value='http://optimus.example.com'),
        k8s.V1EnvVar(name="JOB_LABELS", value='orchestrator=optimus'),
        k8s.V1EnvVar(name="JOB_DIR", value='/data'),
        k8s.V1EnvVar(name="PROJECT", value='example-proj'),
        k8s.V1EnvVar(name="NAMESPACE", value='billing'),
        k8s.V1EnvVar(name="INSTANCE_TYPE", value='hook'),
        k8s.V1EnvVar(name="INSTANCE_NAME", value='failureHook'),
        k8s.V1EnvVar(name="SCHEDULED_AT", value='{{ next_execution_date }}'),
        # rest of the env vars are pulled from the container by making a GRPC call to optimus
    ],
    trigger_rule="one_failed",
    resources=resources,
    reattach_on_restart=True
)
# hooks loop ends


# create upstream sensors
wait_foo__dash__intra__dash__dep__dash__job = SuperExternalTaskSensor(
    optimus_hostname="http://optimus.example.com",
    upstream_optimus_project="example-proj",
    upstream_optimus_namespace="billing",
    upstream_optimus_job="foo-intra-dep-job",
    window_size="1h",
    window_version=int("1"),
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait_foo-intra-dep-job-bq",
    dag=dag
)

wait_foo__dash__inter__dash__dep__dash__job = SuperExternalTaskSensor(
    optimus_hostname="http://optimus.example.com",
    upstream_optimus_project="project",
    upstream_optimus_namespace="namespace",
    upstream_optimus_job="foo-inter-dep-job",
    window_size="1h",
    window_version=int("1"),
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait_foo-inter-dep-job-bq-bq",
    dag=dag
)

wait_foo__dash__external__dash__optimus__dash__dep__dash__job = SuperExternalTaskSensor(
    optimus_hostname="http://optimus.external.io",
    upstream_optimus_project="external-project",
    upstream_optimus_namespace="external-namespace",
    upstream_optimus_job="foo-external-optimus-dep-job",
    window_size="1h",
    window_version=int("1"),
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait_foo-external-optimus-dep-job-bq-bq",
    dag=dag
)
# arrange inter task dependencies
####################################

# upstream sensors -> base transformation task
publish_job_start_event >> wait_foo__dash__intra__dash__dep__dash__job >> transformation_bq__dash__bq
publish_job_start_event >> wait_foo__dash__inter__dash__dep__dash__job >> transformation_bq__dash__bq
publish_job_start_event >> wait_foo__dash__external__dash__optimus__dash__dep__dash__job >> transformation_bq__dash__bq

# setup hooks and dependencies
# start_event -> [Dependency/HttpDep/ExternalDep/PreHook] -> Task -> [Post Hook -> Fail Hook] -> end_event

# setup hook dependencies
publish_job_start_event >> hook_transporter >> transformation_bq__dash__bq

transformation_bq__dash__bq >> [hook_predator,] >> [hook_failureHook,] >> publish_job_end_event

# set inter-dependencies between hooks and hooks
hook_predator >> hook_transporter >> publish_job_end_event
