# Code generated by optimus dev. DO NOT EDIT.

from datetime import datetime, timedelta

# import Dag level callbacks
from __lib import job_success_event, job_failure_event

# import operator level callbacks
from __lib import operator_start_event, operator_success_event, operator_retry_event, operator_failure_event

from __lib import optimus_sla_miss_notify, SuperKubernetesPodOperator, SuperExternalTaskSensor

from airflow.configuration import conf
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.weight_rule import WeightRule
from kubernetes.client import models as k8s

SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS = int(Variable.get("sensor_poke_interval_in_secs", default_var=15 * 60))
SENSOR_DEFAULT_TIMEOUT_IN_SECS = int(Variable.get("sensor_timeout_in_secs", default_var=15 * 60 * 60))
DAG_RETRIES = int(Variable.get("dag_retries", default_var=3))
DAG_RETRY_DELAY = int(Variable.get("dag_retry_delay_in_secs", default_var=5 * 60))
DAGRUN_TIMEOUT_IN_SECS = int(Variable.get("dagrun_timeout_in_secs", default_var=3 * 24 * 60 * 60))
STARTUP_TIMEOUT_IN_SECS = int(Variable.get("startup_timeout_in_secs", default_var=2 * 60))
POOL_SENSOR = Variable.get("sensor_pool", default_var="default_pool")
POOL_TASK = Variable.get("task_pool", default_var="default_pool")
POOL_HOOK = Variable.get("hook_pool", default_var="default_pool")

default_args = {
    "params": {
        "project_name": "example-proj",
        "namespace": "billing",
        "job_name": "infra.billing.weekly-status-reports",
        "optimus_hostname": "http://optimus.example.com"
    },
    "owner": "infra-team@example.com",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(seconds=100),
    "startup_timeout_seconds": STARTUP_TIMEOUT_IN_SECS,
    "retry_exponential_backoff": True,
    "priority_weight": 2000,
    "start_date": datetime.strptime("2022-11-10T05:02:00", "%Y-%m-%dT%H:%M:%S"),
    "end_date": datetime.strptime("2022-11-10T10:02:00", "%Y-%m-%dT%H:%M:%S"),
    "weight_rule": WeightRule.ABSOLUTE,
    "sla": timedelta(seconds=7200),
    "on_execute_callback": operator_start_event,
    "on_success_callback": operator_success_event,
    "on_retry_callback"  : operator_retry_event,
    "on_failure_callback": operator_failure_event,
}

"""
This job collects the billing information related to infrastructure.
This job will run in a weekly basis.
"""
dag = DAG(
    dag_id="infra.billing.weekly-status-reports",
    default_args=default_args,
    schedule_interval="0 2 * * 0",
    catchup=False,
    dagrun_timeout=timedelta(seconds=DAGRUN_TIMEOUT_IN_SECS),
    tags=[
        "optimus",
    ],
    sla_miss_callback=optimus_sla_miss_notify,
    on_success_callback=job_success_event,
    on_failure_callback=job_failure_event,
)

resources = k8s.V1ResourceRequirements(
    limits={
        'memory': '2G',
        'cpu': '200m',
    },
)

JOB_DIR = "/data"
IMAGE_PULL_POLICY = "IfNotPresent"
INIT_CONTAINER_IMAGE = "gotocompany/optimus:dev"
INIT_CONTAINER_ENTRYPOINT = "/opt/entrypoint_init_container.sh"

def get_entrypoint_cmd(plugin_entrypoint_script):
    path_config = JOB_DIR + "/in/.env"
    path_secret = JOB_DIR + "/in/.secret"
    entrypoint = "set -o allexport; source {path_config}; set +o allexport; cat {path_config}; ".format(path_config=path_config)
    entrypoint += "set -o allexport; source {path_secret}; set +o allexport; ".format(path_secret=path_secret)
    return entrypoint + plugin_entrypoint_script

volume = k8s.V1Volume(
    name='asset-volume',
    empty_dir=k8s.V1EmptyDirVolumeSource()
)

asset_volume_mounts = [
    k8s.V1VolumeMount(mount_path=JOB_DIR, name='asset-volume', sub_path=None, read_only=False)
]

executor_env_vars = [
    k8s.V1EnvVar(name="JOB_LABELS", value='orchestrator=optimus'),
    k8s.V1EnvVar(name="JOB_DIR", value=JOB_DIR),
    k8s.V1EnvVar(name="JOB_NAME", value='infra.billing.weekly-status-reports'),
]

init_env_vars = [
    k8s.V1EnvVar(name="JOB_DIR", value=JOB_DIR),
    k8s.V1EnvVar(name="JOB_NAME", value='infra.billing.weekly-status-reports'),
    k8s.V1EnvVar(name="OPTIMUS_HOST", value='http://optimus.example.com'),
    k8s.V1EnvVar(name="PROJECT", value='example-proj'),
    k8s.V1EnvVar(name="SCHEDULED_AT", value='{{ next_execution_date }}'),
]

init_container = k8s.V1Container(
    name="init-container",
    image=INIT_CONTAINER_IMAGE,
    image_pull_policy=IMAGE_PULL_POLICY,
    env=init_env_vars + [
        k8s.V1EnvVar(name="INSTANCE_TYPE", value='task'),
        k8s.V1EnvVar(name="INSTANCE_NAME", value='bq-bq'),
    ],
    security_context=k8s.V1PodSecurityContext(run_as_user=0),
    volume_mounts=asset_volume_mounts,
    command=["/bin/sh", INIT_CONTAINER_ENTRYPOINT],
)

transformation_bq__dash__bq = SuperKubernetesPodOperator(
    image_pull_policy=IMAGE_PULL_POLICY,
    namespace=conf.get('kubernetes', 'namespace', fallback="default"),
    image="example.io/namespace/bq2bq-executor:latest",
    cmds=["/bin/bash", "-c"],
    arguments=[get_entrypoint_cmd("""python3 /opt/bumblebee/main.py """)],
    name="bq-bq",
    task_id="bq-bq",
    get_logs=True,
    dag=dag,
    depends_on_past=True,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=executor_env_vars,
    resources=resources,
    reattach_on_restart=True,
    volume_mounts=asset_volume_mounts,
    volumes=[volume],
    init_containers=[init_container],
    pool=POOL_TASK
)

# hooks loop start
init_container_transporter = k8s.V1Container(
    name="init-container",
    image=INIT_CONTAINER_IMAGE,
    image_pull_policy=IMAGE_PULL_POLICY,
    env=init_env_vars + [
        k8s.V1EnvVar(name="INSTANCE_TYPE", value='hook'),
        k8s.V1EnvVar(name="INSTANCE_NAME", value='transporter'),
    ],
    security_context=k8s.V1PodSecurityContext(run_as_user=0),
    volume_mounts=asset_volume_mounts,
    command=["/bin/sh", INIT_CONTAINER_ENTRYPOINT],
)

hook_transporter = SuperKubernetesPodOperator(
    image_pull_policy=IMAGE_PULL_POLICY,
    namespace=conf.get('kubernetes', 'namespace', fallback="default"),
    image="example.io/namespace/transporter-executor:latest",
    cmds=["/bin/sh", "-c"],
    arguments=[get_entrypoint_cmd("""java -cp /opt/transporter/transporter.jar:/opt/transporter/jolokia-jvm-agent.jar -javaagent:jolokia-jvm-agent.jar=port=7777,host=0.0.0.0 com.gojek.transporter.Main """)],
    name="hook_transporter",
    task_id="hook_transporter",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    depends_on_past=False,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=executor_env_vars,
    resources=resources,
    reattach_on_restart=True,
    volume_mounts=asset_volume_mounts,
    volumes=[volume],
    init_containers=[init_container_transporter],
    pool=POOL_HOOK
)
init_container_predator = k8s.V1Container(
    name="init-container",
    image=INIT_CONTAINER_IMAGE,
    image_pull_policy=IMAGE_PULL_POLICY,
    env=init_env_vars + [
        k8s.V1EnvVar(name="INSTANCE_TYPE", value='hook'),
        k8s.V1EnvVar(name="INSTANCE_NAME", value='predator'),
    ],
    security_context=k8s.V1PodSecurityContext(run_as_user=0),
    volume_mounts=asset_volume_mounts,
    command=["/bin/sh", INIT_CONTAINER_ENTRYPOINT],
)

hook_predator = SuperKubernetesPodOperator(
    image_pull_policy=IMAGE_PULL_POLICY,
    namespace=conf.get('kubernetes', 'namespace', fallback="default"),
    image="example.io/namespace/predator-image:latest",
    cmds=["/bin/sh", "-c"],
    arguments=[get_entrypoint_cmd("""predator ${SUB_COMMAND} -s ${PREDATOR_URL} -u "${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}" """)],
    name="hook_predator",
    task_id="hook_predator",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    depends_on_past=False,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=executor_env_vars,
    resources=resources,
    reattach_on_restart=True,
    volume_mounts=asset_volume_mounts,
    volumes=[volume],
    init_containers=[init_container_predator],
    pool=POOL_HOOK
)
init_container_failureHook = k8s.V1Container(
    name="init-container",
    image=INIT_CONTAINER_IMAGE,
    image_pull_policy=IMAGE_PULL_POLICY,
    env=init_env_vars + [
        k8s.V1EnvVar(name="INSTANCE_TYPE", value='hook'),
        k8s.V1EnvVar(name="INSTANCE_NAME", value='failureHook'),
    ],
    security_context=k8s.V1PodSecurityContext(run_as_user=0),
    volume_mounts=asset_volume_mounts,
    command=["/bin/sh", INIT_CONTAINER_ENTRYPOINT],
)

hook_failureHook = SuperKubernetesPodOperator(
    image_pull_policy=IMAGE_PULL_POLICY,
    namespace=conf.get('kubernetes', 'namespace', fallback="default"),
    image="example.io/namespace/failure-hook-image:latest",
    cmds=["/bin/sh", "-c"],
    arguments=[get_entrypoint_cmd("""sleep 5 """)],
    name="hook_failureHook",
    task_id="hook_failureHook",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    depends_on_past=False,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=executor_env_vars,
    trigger_rule="one_failed",
    resources=resources,
    reattach_on_restart=True,
    volume_mounts=asset_volume_mounts,
    volumes=[volume],
    init_containers=[init_container_failureHook],
    pool=POOL_HOOK
)
# hooks loop ends


# create upstream sensors
wait_foo__dash__intra__dash__dep__dash__job = SuperExternalTaskSensor(
    optimus_hostname="http://optimus.example.com",
    upstream_optimus_hostname="http://optimus.example.com",
    upstream_optimus_project="example-proj",
    upstream_optimus_namespace="billing",
    upstream_optimus_job="foo-intra-dep-job",
    window_size="1h",
    window_version=int("1"),
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait_foo-intra-dep-job-bq",
    depends_on_past=False,
    dag=dag,
    pool=POOL_SENSOR
)

wait_foo__dash__inter__dash__dep__dash__job = SuperExternalTaskSensor(
    optimus_hostname="http://optimus.example.com",
    upstream_optimus_hostname="http://optimus.example.com",
    upstream_optimus_project="project",
    upstream_optimus_namespace="namespace",
    upstream_optimus_job="foo-inter-dep-job",
    window_size="1h",
    window_version=int("1"),
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait_foo-inter-dep-job-bq-bq",
    depends_on_past=False,
    dag=dag,
    pool=POOL_SENSOR
)

wait_foo__dash__external__dash__optimus__dash__dep__dash__job = SuperExternalTaskSensor(
    optimus_hostname="http://optimus.example.com",
    upstream_optimus_hostname="http://optimus.external.io",
    upstream_optimus_project="external-project",
    upstream_optimus_namespace="external-namespace",
    upstream_optimus_job="foo-external-optimus-dep-job",
    window_size="1h",
    window_version=int("1"),
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait_foo-external-optimus-dep-job-bq-bq",
    depends_on_past=False,
    dag=dag,
    pool=POOL_SENSOR
)
# arrange inter task dependencies
####################################

# upstream sensors -> base transformation task
wait_foo__dash__intra__dash__dep__dash__job >> transformation_bq__dash__bq
wait_foo__dash__inter__dash__dep__dash__job >> transformation_bq__dash__bq
wait_foo__dash__external__dash__optimus__dash__dep__dash__job >> transformation_bq__dash__bq

# setup hooks and dependencies
# [Dependency/HttpDep/ExternalDep/PreHook] -> Task -> [Post Hook -> Fail Hook]

# setup hook dependencies
hook_transporter >> transformation_bq__dash__bq

transformation_bq__dash__bq >> [hook_predator,] >> [hook_failureHook,]

# set inter-dependencies between hooks and hooks
hook_predator >> hook_transporter
