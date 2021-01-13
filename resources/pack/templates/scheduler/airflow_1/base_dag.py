import os
import json
import calendar
import re
from typing import Any, Callable, Dict, Optional
from datetime import datetime, timedelta, timezone
from string import Template
from croniter import croniter

from airflow.kubernetes import kube_client, pod_launcher
from airflow.models import DAG, Variable, DagRun, DagModel, TaskInstance, BaseOperator, XCom, XCOM_RETURN_KEY
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.utils.db import provide_session
from airflow.configuration import conf
from airflow.utils.state import State
from airflow.hooks.base_hook import BaseHook
from airflow.utils.weight_rule import WeightRule

from __lib import alert_failed_to_slack, SuperKubernetesPodOperator, SuperExternalTaskSensor, \
    SlackWebhookOperator


SECRET_NAME = Variable.get("secret_name", "optimus-google-credentials")
SECRET_KEY = Variable.get("secret_key", "auth.json")
SECRET_VOLUME_PATH = '/opt/optimus/secrets/'
SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS = int(Variable.get("sensor_poke_interval_in_secs", default_var=15 * 60))
SENSOR_DEFAULT_TIMEOUT_IN_SECS = int(Variable.get("sensor_timeout_in_secs", default_var=15 * 60 * 60))

gcloud_credentials_path = '{}{}'.format(SECRET_VOLUME_PATH, SECRET_KEY)
gcloud_secret = Secret(
    'volume', 
    SECRET_VOLUME_PATH,
    SECRET_NAME, 
    SECRET_KEY)


default_args = {
    "owner": "{{.Owner}}",
    "depends_on_past": {{- if .Behavior.DependsOnPast }} True {{ else }} False {{ end -}},
    "retries": 3,
    "retry_delay": timedelta(seconds=300),
    "start_date": datetime.strptime({{ .Schedule.StartDate.Format "2006-01-02" | quote }}, "%Y-%m-%d"),
    "on_failure_callback": alert_failed_to_slack,
    "priority_weight": {{.Task.Priority}},
    "weight_rule": WeightRule.ABSOLUTE
}

dag = DAG(
    dag_id="{{.Name}}",
    default_args=default_args,
    schedule_interval="{{.Schedule.Interval}}",
    catchup = {{ if .Behavior.CatchUp }} True {{ else }} False {{ end }}
)


transformation_{{.Task.Unit.GetName | replace "-" "__dash__" | replace "." "__dot__"}} = SuperKubernetesPodOperator(
    image_pull_policy="Always",
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "{}".format("{{.Task.Unit.GetImage}}"),
    cmds=[],
    name="{{.Task.Unit.GetName | replace "_" "-" }}",
    task_id="{{.Task.Unit.GetName}}",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=True,
    secrets=[gcloud_secret],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": gcloud_credentials_path,
        {{range $key,$value := .Task.Config}}"{{$key}}":'{{$value}}',{{- end}}
    },
    reattach_on_restart=True
)

# hooks loop start
{{ range $_, $t := .Hooks }}
{{- end -}}
# hooks loop ends

# create upstream sensors
{{- range $i, $t := $.Dependencies}}
wait_{{$t.Job.Name | replace "-" "__dash__" | replace "." "__dot__"}} = SuperExternalTaskSensor(
    external_dag_id = "{{$t.Job.Name}}",
    window_size = {{$t.Job.Task.Window.Size.Hours }},
    window_offset = {{$t.Job.Task.Window.Offset.Hours }},
    window_truncate_upto = "{{$t.Job.Task.Window.TruncateTo}}",
    task_id = "wait-{{$t.Job.Name}}-{{$t.Job.Task.Unit.GetName}}",
    poke_interval = SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout = SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    dag=dag
)
{{- end}}

# arrange inter task dependencies
####################################

# upstream sensors -> base transformation task
{{- range $i, $t := $.Dependencies }}
wait_{{ $t.Job.Name | replace "-" "__dash__" | replace "." "__dot__" }} >> transformation_{{$.Task.Unit.GetName | replace "-" "__dash__" | replace "." "__dot__"}}
{{- end}}