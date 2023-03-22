# Code generated by optimus dev. DO NOT EDIT.

from typing import Any, Callable, Dict, Optional
from datetime import datetime, timedelta, timezone

from airflow.models import DAG, Variable, DagRun, DagModel, TaskInstance, BaseOperator, XCom, XCOM_RETURN_KEY
from airflow.configuration import conf
from airflow.operators.python_operator import PythonOperator
from airflow.utils.weight_rule import WeightRule
from kubernetes.client import models as k8s


from __lib import optimus_sla_miss_notify, SuperKubernetesPodOperator, \
    SuperExternalTaskSensor, ExternalHttpSensor

from __lib import JOB_START_EVENT_NAME, \
    JOB_END_EVENT_NAME, \
    log_start_event, \
    log_success_event, \
    log_retry_event, \
    log_failure_event, \
    EVENT_NAMES, \
    log_job_end, log_job_start

SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS = int(Variable.get("sensor_poke_interval_in_secs", default_var=15 * 60))
SENSOR_DEFAULT_TIMEOUT_IN_SECS = int(Variable.get("sensor_timeout_in_secs", default_var=15 * 60 * 60))
DAG_RETRIES = int(Variable.get("dag_retries", default_var=3))
DAG_RETRY_DELAY = int(Variable.get("dag_retry_delay_in_secs", default_var=5 * 60))
DAGRUN_TIMEOUT_IN_SECS = int(Variable.get("dagrun_timeout_in_secs", default_var=3 * 24 * 60 * 60))

default_args = {
    "params": {
        "project_name": "foo-project",
        "namespace": "bar-namespace",
        "job_name": "foo",
        "optimus_hostname": "http://airflow.example.io"
    },
    "owner": "mee@mee",
    "depends_on_past": True,
    "retries": 4,
    "retry_delay": timedelta(seconds=DAG_RETRY_DELAY),
    "retry_exponential_backoff": True,
    "priority_weight": 2000,
    "start_date": datetime.strptime("2000-11-11T00:00:00", "%Y-%m-%dT%H:%M:%S"),
    "end_date": datetime.strptime("2020-11-11T00:00:00","%Y-%m-%dT%H:%M:%S"),
    "on_failure_callback": log_failure_event,
    "on_retry_callback": log_retry_event,
    "on_success_callback": log_success_event,
    "weight_rule": WeightRule.ABSOLUTE
}

dag = DAG(
    dag_id="foo",
    default_args=default_args,
    schedule_interval="* * * * *",
    sla_miss_callback=optimus_sla_miss_notify,
    catchup=True,
    dagrun_timeout=timedelta(seconds=DAGRUN_TIMEOUT_IN_SECS),
    tags = [
            "optimus",
           ]
)

publish_job_start_event = PythonOperator(
        task_id = JOB_START_EVENT_NAME,
        python_callable = log_job_start,
        provide_context=True,
        depends_on_past=False,
        dag=dag
    )

publish_job_end_event = PythonOperator(
        task_id = JOB_END_EVENT_NAME,
        python_callable = log_job_end,
        provide_context=True,
        trigger_rule= 'all_success',
        depends_on_past=False,
        dag=dag
    )


JOB_DIR = "/data"
IMAGE_PULL_POLICY="IfNotPresent"
INIT_CONTAINER_IMAGE="odpf/optimus:dev"
INIT_CONTAINER_ENTRYPOINT = "/opt/entrypoint_init_container.sh"

volume = k8s.V1Volume(
    name='asset-volume',
    empty_dir=k8s.V1EmptyDirVolumeSource()
)
asset_volume_mounts = [
    k8s.V1VolumeMount(mount_path=JOB_DIR, name='asset-volume', sub_path=None, read_only=False)
]
executor_env_vars = [
    k8s.V1EnvVar(name="JOB_LABELS",value='orchestrator=optimus'),
    k8s.V1EnvVar(name="JOB_DIR",value=JOB_DIR),
    k8s.V1EnvVar(name="JOB_NAME",value='foo'),
]

init_env_vars = [
    k8s.V1EnvVar(name="JOB_DIR",value=JOB_DIR),
    k8s.V1EnvVar(name="JOB_NAME",value='foo'),
    k8s.V1EnvVar(name="OPTIMUS_HOST",value='http://airflow.example.io'),
    k8s.V1EnvVar(name="PROJECT",value='foo-project'),
    k8s.V1EnvVar(name="SCHEDULED_AT",value='{{ next_execution_date }}'),
]

init_container = k8s.V1Container(
    name="init-container",
    image=INIT_CONTAINER_IMAGE,
    image_pull_policy=IMAGE_PULL_POLICY,
    env=init_env_vars + [
        k8s.V1EnvVar(name="INSTANCE_TYPE",value='task'),
        k8s.V1EnvVar(name="INSTANCE_NAME",value='bq'),
    ],
    security_context=k8s.V1PodSecurityContext(run_as_user=0),
    volume_mounts=asset_volume_mounts,
    command=["/bin/sh", INIT_CONTAINER_ENTRYPOINT],
)

transformation_bq = SuperKubernetesPodOperator(
    optimus_hostname="http://airflow.example.io",
    optimus_projectname="foo-project",
    optimus_namespacename="bar-namespace",
    optimus_jobname="foo",
    optimus_jobtype="task",
    optimus_instancename="bq",
    image_pull_policy=IMAGE_PULL_POLICY,
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "example.io/namespace/image:latest",
    cmds=[],
    name="bq",
    task_id="bq",
    get_logs=True,
    dag=dag,
    depends_on_past=True,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=executor_env_vars,
    sla=timedelta(seconds=7200),
    reattach_on_restart=True,
    volume_mounts=asset_volume_mounts,
    volumes=[volume],
    init_containers=[init_container],
)

# hooks loop start

init_container_transporter = k8s.V1Container(
    name="init-container",
    image=INIT_CONTAINER_IMAGE,
    image_pull_policy=IMAGE_PULL_POLICY,
    env= init_env_vars + [
        k8s.V1EnvVar(name="INSTANCE_TYPE",value='hook'),
        k8s.V1EnvVar(name="INSTANCE_NAME",value='transporter'),
    ],
    security_context=k8s.V1PodSecurityContext(run_as_user=0),
    volume_mounts=asset_volume_mounts,
    command=["/bin/sh", INIT_CONTAINER_ENTRYPOINT],
)

hook_transporter = SuperKubernetesPodOperator(
    optimus_hostname="http://airflow.example.io",
    optimus_projectname="foo-project",
    optimus_namespacename="bar-namespace",
    optimus_jobname="foo",
    optimus_jobtype="hook",
    optimus_instancename="transporter",
    image_pull_policy=IMAGE_PULL_POLICY,
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "example.io/namespace/hook-image:latest",
    cmds=[],
    name="hook_transporter",
    task_id="hook_transporter",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    depends_on_past=False,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=executor_env_vars,
    reattach_on_restart=True,
    volume_mounts=asset_volume_mounts,
    volumes=[volume],
    init_containers=[init_container_transporter],
)
init_container_predator = k8s.V1Container(
    name="init-container",
    image=INIT_CONTAINER_IMAGE,
    image_pull_policy=IMAGE_PULL_POLICY,
    env= init_env_vars + [
        k8s.V1EnvVar(name="INSTANCE_TYPE",value='hook'),
        k8s.V1EnvVar(name="INSTANCE_NAME",value='predator'),
    ],
    security_context=k8s.V1PodSecurityContext(run_as_user=0),
    volume_mounts=asset_volume_mounts,
    command=["/bin/sh", INIT_CONTAINER_ENTRYPOINT],
)

hook_predator = SuperKubernetesPodOperator(
    optimus_hostname="http://airflow.example.io",
    optimus_projectname="foo-project",
    optimus_namespacename="bar-namespace",
    optimus_jobname="foo",
    optimus_jobtype="hook",
    optimus_instancename="predator",
    image_pull_policy=IMAGE_PULL_POLICY,
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "example.io/namespace/predator-image:latest",
    cmds=[],
    name="hook_predator",
    task_id="hook_predator",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    depends_on_past=False,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=executor_env_vars,
    reattach_on_restart=True,
    volume_mounts=asset_volume_mounts,
    volumes=[volume],
    init_containers=[init_container_predator],
)
init_container_hook__dash__for__dash__fail = k8s.V1Container(
    name="init-container",
    image=INIT_CONTAINER_IMAGE,
    image_pull_policy=IMAGE_PULL_POLICY,
    env= init_env_vars + [
        k8s.V1EnvVar(name="INSTANCE_TYPE",value='hook'),
        k8s.V1EnvVar(name="INSTANCE_NAME",value='hook-for-fail'),
    ],
    security_context=k8s.V1PodSecurityContext(run_as_user=0),
    volume_mounts=asset_volume_mounts,
    command=["/bin/sh", INIT_CONTAINER_ENTRYPOINT],
)

hook_hook__dash__for__dash__fail = SuperKubernetesPodOperator(
    optimus_hostname="http://airflow.example.io",
    optimus_projectname="foo-project",
    optimus_namespacename="bar-namespace",
    optimus_jobname="foo",
    optimus_jobtype="hook",
    optimus_instancename="hook-for-fail",
    image_pull_policy=IMAGE_PULL_POLICY,
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "example.io/namespace/fail-image:latest",
    cmds=[],
    name="hook_hook-for-fail",
    task_id="hook_hook-for-fail",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    depends_on_past=False,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    env_vars=executor_env_vars,
    trigger_rule="one_failed",
    reattach_on_restart=True,
    volume_mounts=asset_volume_mounts,
    volumes=[volume],
    init_containers=[init_container_hook__dash__for__dash__fail],
)
# hooks loop ends


# create upstream sensors

wait_foo__dash__intra__dash__dep__dash__job = SuperExternalTaskSensor(
    optimus_hostname="http://airflow.example.io",
    upstream_optimus_hostname="http://airflow.example.io",
    upstream_optimus_project="foo-project",
    upstream_optimus_namespace="bar-namespace",
    upstream_optimus_job="foo-intra-dep-job",
    window_size="1h",
    window_version=int("1"),
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait_foo-intra-dep-job-bq",
    depends_on_past=False,
    dag=dag
)
wait_foo__dash__inter__dash__dep__dash__job = SuperExternalTaskSensor(
    optimus_hostname="http://airflow.example.io",
    upstream_optimus_hostname="http://airflow.example.io",
    upstream_optimus_project="foo-external-project",
    upstream_optimus_namespace="bar-namespace",
    upstream_optimus_job="foo-inter-dep-job",
    window_size="1h",
    window_version=int("1"),
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait_foo-inter-dep-job-bq",
    depends_on_past=False,
    dag=dag
)

wait_external__dash__optimus__dash__foo__dash__external__dash__optimus__dash__project__dash__foo__dash__external__dash__optimus__dash__dep__dash__job = SuperExternalTaskSensor(
    optimus_hostname="http://airflow.example.io",
    upstream_optimus_hostname="http://optimus.external.io",
    upstream_optimus_project="foo-external-optimus-project",
    upstream_optimus_namespace="bar-external-optimus-namespace",
    upstream_optimus_job="foo-external-optimus-dep-job",
    window_size="1h",
    window_version=int("1"),
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    depends_on_past=False,
    task_id="wait_foo-external-optimus-dep-job-bq",
    dag=dag
)

# arrange inter task dependencies
####################################

# upstream sensors -> base transformation task
publish_job_start_event >> wait_foo__dash__intra__dash__dep__dash__job >> transformation_bq
publish_job_start_event >> wait_foo__dash__inter__dash__dep__dash__job >> transformation_bq

publish_job_start_event >> wait_external__dash__optimus__dash__foo__dash__external__dash__optimus__dash__project__dash__foo__dash__external__dash__optimus__dash__dep__dash__job >> transformation_bq

# post completion hook
transformation_bq >> publish_job_end_event

# set inter-dependencies between task and hooks
publish_job_start_event >> hook_transporter >> transformation_bq
transformation_bq >> hook_predator >> publish_job_end_event
transformation_bq >> hook_hook__dash__for__dash__fail >> publish_job_end_event

# set inter-dependencies between hooks and hooks
hook_transporter >> hook_predator >> publish_job_end_event

# arrange failure hook after post hooks

hook_predator >> [ hook_hook__dash__for__dash__fail,] >> publish_job_end_event