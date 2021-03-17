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
    "owner": {{.Job.Owner | quote}},
    "depends_on_past": {{- if .Job.Behavior.DependsOnPast }} True {{ else }} False {{ end -}},
    "retries": DAG_RETRIES,
    "retry_delay": timedelta(seconds=DAG_RETRY_DELAY),
    "priority_weight": {{.Job.Task.Priority}},
    "start_date": datetime.strptime({{ .Job.Schedule.StartDate.Format "2006-01-02" | quote }}, "%Y-%m-%d"),
    {{if .Job.Schedule.EndDate -}}"end_date": datetime.strptime({{ .Job.Schedule.EndDate.Format "2006-01-02" | quote}},"%Y-%m-%d"),{{- else -}}{{- end}}
    "on_failure_callback": alert_failed_to_slack,
    "weight_rule": WeightRule.ABSOLUTE
}

dag = DAG(
    dag_id={{.Job.Name | quote}},
    default_args=default_args,
    schedule_interval={{.Job.Schedule.Interval | quote}},
    catchup ={{ if .Job.Behavior.CatchUp }} True{{ else }} False{{ end }}
)

transformation_{{.Job.Task.Unit.GetName | replace "-" "__dash__" | replace "." "__dot__"}} = SuperKubernetesPodOperator(
    image_pull_policy="Always",
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "{}".format("{{.Job.Task.Unit.GetImage}}"),
    cmds=[],
    name="{{.Job.Task.Unit.GetName | replace "_" "-" }}",
    task_id={{.Job.Task.Unit.GetName | quote}},
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    secrets=[gcloud_secret],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": gcloud_credentials_path,
        "JOB_NAME":'{{.Job.Name}}', "OPTIMUS_HOSTNAME":'{{.Hostname}}',
        "JOB_LABELS":'{{.Job.GetLabelsAsString}}',
        "JOB_DIR":'/data', "PROJECT":'{{.Project.Name}}',
        "TASK_TYPE":'{{$.InstanceTypeTransformation}}', "TASK_NAME":'{{.Job.Task.Unit.GetName}}',
        "SCHEDULED_AT":'{{ "{{ next_execution_date }}" }}',
    },
    reattach_on_restart=True,
)

# hooks loop start
{{ range $_, $t := .Job.Hooks }}
hook_{{$t.Unit.GetName}} = SuperKubernetesPodOperator(
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
        "JOB_NAME":'{{$.Job.Name}}', "OPTIMUS_HOSTNAME":'{{$.Hostname}}',
        "JOB_LABELS":'{{$.Job.GetLabelsAsString}}',
        "JOB_DIR":'/data', "PROJECT":'{{$.Project.Name}}',
        "TASK_TYPE":'{{$.InstanceTypeHook}}', "TASK_NAME":'{{$t.Unit.GetName}}',
        "SCHEDULED_AT":'{{ "{{ next_execution_date }}" }}',
        # rest of the env vars are pulled from the container by making a GRPC call to optimus
   },
   reattach_on_restart=True,
)
{{- end }}
# hooks loop ends


# create upstream sensors
{{- range $_, $dependency := $.Job.Dependencies}}
{{- if eq $dependency.Type $.JobSpecDependencyTypeIntra }}
wait_{{$dependency.Job.Name | replace "-" "__dash__" | replace "." "__dot__"}} = SuperExternalTaskSensor(
    external_dag_id = "{{$dependency.Job.Name}}",
    window_size = {{$dependency.Job.Task.Window.Size.Hours }},
    window_offset = {{$dependency.Job.Task.Window.Offset.Hours }},
    window_truncate_upto = "{{$dependency.Job.Task.Window.TruncateTo}}",
    task_id = "wait_{{$dependency.Job.Name | trunc 200}}-{{$dependency.Job.Task.Unit.GetName}}",
    poke_interval = SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout = SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    dag=dag
)
{{- end -}}

{{- if eq $dependency.Type $.JobSpecDependencyTypeInter }}
wait_{{$dependency.Job.Name | replace "-" "__dash__" | replace "." "__dot__"}} = CrossTenantDependencySensor(
    optimus_host="{{$.Hostname}}",
    optimus_project="{{$dependency.Project.Name}}",
    optimus_job="{{$dependency.Job.Name}}",
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait_{{$dependency.Job.Name | trunc 200}}-{{$dependency.Job.Task.Unit.GetName}}",
    dag=dag
)
{{- end -}}
{{- end}}

# arrange inter task dependencies
####################################

# upstream sensors -> base transformation task
{{- range $i, $t := $.Job.Dependencies }}
wait_{{ $t.Job.Name | replace "-" "__dash__" | replace "." "__dot__" }} >> transformation_{{$.Job.Task.Unit.GetName | replace "-" "__dash__" | replace "." "__dot__"}}
{{- end}}

# set inter-dependencies between task and hooks
{{- range $_, $task := .Job.Hooks }}
{{- if eq $task.Unit.GetType $.HookTypePre }}
hook_{{$task.Unit.GetName}} >> transformation_{{$.Job.Task.Unit.GetName | replace "-" "__dash__" | replace "." "__dot__"}}
{{- end -}}
{{- if eq $task.Unit.GetType $.HookTypePost }}
transformation_{{$.Job.Task.Unit.GetName | replace "-" "__dash__" | replace "." "__dot__"}} >> hook_{{$task.Unit.GetName}}
{{- end -}}
{{- end }}

# set inter-dependencies between hooks and hooks
{{- range $_, $t := .Job.Hooks }}
{{- range $_, $depend := $t.DependsOn }}
hook_{{$depend.Unit.GetName}} >> hook_{{$t.Unit.GetName}}
{{- end }}
{{- end }}
