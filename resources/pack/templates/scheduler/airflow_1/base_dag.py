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
    "owner": "{{.Job.Owner}}",
    "depends_on_past": {{- if .Job.Behavior.DependsOnPast }} True {{ else }} False {{ end -}},
    "retries": 3,
    "retry_delay": timedelta(seconds=300),
    "start_date": datetime.strptime({{ .Job.Schedule.StartDate.Format "2006-01-02" | quote }}, "%Y-%m-%d"),
    "on_failure_callback": alert_failed_to_slack,
    "priority_weight": {{.Job.Task.Priority}},
    "weight_rule": WeightRule.ABSOLUTE
}

dag = DAG(
    dag_id="{{.Job.Name}}",
    default_args=default_args,
    schedule_interval="{{.Job.Schedule.Interval}}",
    catchup ={{ if .Job.Behavior.CatchUp }} True{{ else }} False{{ end }}
)

transformation_{{.Job.Task.Unit.GetName | replace "-" "__dash__" | replace "." "__dot__"}} = SuperKubernetesPodOperator(
    image_pull_policy="Always",
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "{}".format("{{.Job.Task.Unit.GetImage}}"),
    cmds=[],
    name="{{.Job.Task.Unit.GetName | replace "_" "-" }}",
    task_id="{{.Job.Task.Unit.GetName}}",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    secrets=[gcloud_secret],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": gcloud_credentials_path,
        "JOB_NAME":'{{.Job.Name}}', "OPTIMUS_HOSTNAME": '{{.Hostname}}',
        "JOB_DIR":'/data', "PROJECT":'{{.Project.Name}}',
        "TASK_TYPE":'base', "TASK_NAME": "{{.Job.Task.Unit.GetName}}",
        "SCHEDULED_AT":'{{ "{{ next_execution_date }}" }}',
    },
    reattach_on_restart=True,
)

# hooks loop start
{{ range $_, $t := .Job.Hooks }}
hook_{{$t.Unit.GetName}} =  SuperKubernetesPodOperator(
    image_pull_policy="Always",
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "{{$t.Unit.GetImage}}",
    cmds=[],
    name="hook_{{$t.Unit.GetName}}",
    task_id="hook_{{$t.Unit.GetName}}",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    secrets=[gcloud_secret],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": gcloud_credentials_path,
        "JOB_NAME":'{{$.Job.Name}}', "OPTIMUS_HOSTNAME": '{{$.Hostname}}',
        "JOB_DIR":'/data', "PROJECT":'{{$.Project.Name}}',
        "TASK_TYPE":'hook', "TASK_NAME": "{{$t.Unit.GetName}}",
        "SCHEDULED_AT":'{{ "{{ next_execution_date }}" }}',
        # rest of the env vars are pulled from the container by making a GRPC call to optimus
   },
   reattach_on_restart=True,
)
{{- end }}


# set inter-dependencies of task and hooks
{{- range $_, $t := .Job.Hooks }}
{{- if eq $t.Type $.HookTypePre }}
hook_{{$t.Unit.GetName}} >> transformation_{{$.Job.Task.Unit.GetName | replace "-" "__dash__" | replace "." "__dot__"}}
{{- end -}}
{{- if eq $t.Type $.HookTypePost }}
transformation_{{$.Job.Task.Unit.GetName | replace "-" "__dash__" | replace "." "__dot__"}} >> hook_{{$t.Unit.GetName}}
{{- end -}}
{{- end }}
# hooks loop ends

# create upstream sensors
{{- range $i, $t := $.Job.Dependencies}}
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
{{- range $i, $t := $.Job.Dependencies }}
wait_{{ $t.Job.Name | replace "-" "__dash__" | replace "." "__dot__" }} >> transformation_{{$.Job.Task.Unit.GetName | replace "-" "__dash__" | replace "." "__dot__"}}
{{- end}}
