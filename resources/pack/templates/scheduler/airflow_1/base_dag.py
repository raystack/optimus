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

SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS = int(Variable.get("sensor_poke_interval_in_secs", default_var=15 * 60))
SENSOR_DEFAULT_TIMEOUT_IN_SECS = int(Variable.get("sensor_timeout_in_secs", default_var=15 * 60 * 60))
DAG_RETRIES = int(Variable.get("dag_retries", default_var=3))
DAG_RETRY_DELAY = int(Variable.get("dag_retry_delay_in_secs", default_var=5 * 60))

default_args = {
    "owner": {{.Job.Owner | quote}},
    "depends_on_past": {{- if .Job.Behavior.DependsOnPast }} True {{ else }} False {{ end -}},
    "retries": DAG_RETRIES,
    "retry_delay": timedelta(seconds=DAG_RETRY_DELAY),
    "priority_weight": {{.Job.Task.Priority}},
    "start_date": datetime.strptime({{ .Job.Schedule.StartDate.Format "2006-01-02T15:04:05" | quote }}, "%Y-%m-%dT%H:%M:%S"),
    {{if .Job.Schedule.EndDate -}}"end_date": datetime.strptime({{ .Job.Schedule.EndDate.Format "2006-01-02T15:04:05" | quote}},"%Y-%m-%dT%H:%M:%S"),{{- else -}}{{- end}}
    "on_failure_callback": alert_failed_to_slack,
    "weight_rule": WeightRule.ABSOLUTE
}

dag = DAG(
    dag_id={{.Job.Name | quote}},
    default_args=default_args,
    schedule_interval={{.Job.Schedule.Interval | quote}},
    catchup ={{ if .Job.Behavior.CatchUp }} True{{ else }} False{{ end }}
)

{{$baseTaskSchema := .Job.Task.Unit.GetTaskSchema $.Context $.TaskSchemaRequest -}}
{{ if ne $baseTaskSchema.SecretPath "" -}}
transformation_secret = Secret(
    "volume",
    {{ dir $baseTaskSchema.SecretPath | quote }},
    "optimus-task-{{ $baseTaskSchema.Name }}",
    {{ base $baseTaskSchema.SecretPath | quote }}
)
{{- end }}
transformation_{{$baseTaskSchema.Name | replace "-" "__dash__" | replace "." "__dot__"}} = SuperKubernetesPodOperator(
    image_pull_policy="Always",
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = {{ $baseTaskSchema.Image | quote}},
    cmds=[],
    name="{{ $baseTaskSchema.Name | replace "_" "-" }}",
    task_id={{$baseTaskSchema.Name | quote}},
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    secrets=[{{ if ne $baseTaskSchema.SecretPath "" -}} transformation_secret {{- end }}],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": '{{ $baseTaskSchema.SecretPath }}',
        "JOB_NAME":'{{.Job.Name}}', "OPTIMUS_HOSTNAME":'{{.Hostname}}',
        "JOB_LABELS":'{{.Job.GetLabelsAsString}}',
        "JOB_DIR":'/data', "PROJECT":'{{.Project.Name}}',
        "TASK_TYPE":'{{$.InstanceTypeTask}}', "TASK_NAME":'{{$baseTaskSchema.Name}}',
        "SCHEDULED_AT":'{{ "{{ next_execution_date }}" }}',
    },
    reattach_on_restart=True,
)

# hooks loop start
{{ range $_, $t := .Job.Hooks }}
{{ $hookSchema := $t.Unit.GetHookSchema $.Context $.HookSchemaRequest -}}

{{ if ne $hookSchema.SecretPath "" -}}
hook_{{$hookSchema.Name | replace "-" "_"}}_secret = Secret(
    "volume",
    {{ dir $hookSchema.SecretPath | quote }},
    "optimus-hook-{{ $hookSchema.Name }}",
    {{ base $hookSchema.SecretPath | quote }}
)
{{- end -}}

hook_{{$hookSchema.Name}} = SuperKubernetesPodOperator(
    image_pull_policy="Always",
    namespace = conf.get('kubernetes', 'namespace', fallback="default"),
    image = "{{ $hookSchema.Image }}",
    cmds=[],
    name="hook_{{ $hookSchema.Name | replace "_" "-"}}",
    task_id="hook_{{ $hookSchema.Name }}",
    get_logs=True,
    dag=dag,
    in_cluster=True,
    is_delete_operator_pod=True,
    do_xcom_push=False,
    secrets=[{{ if ne $hookSchema.SecretPath "" -}} hook_{{$hookSchema.Name | replace "-" "_"}}_secret {{- end }}],
    env_vars={
        "GOOGLE_APPLICATION_CREDENTIALS": '{{ $hookSchema.SecretPath }}',
        "JOB_NAME":'{{$.Job.Name}}', "OPTIMUS_HOSTNAME":'{{$.Hostname}}',
        "JOB_LABELS":'{{$.Job.GetLabelsAsString}}',
        "JOB_DIR":'/data', "PROJECT":'{{$.Project.Name}}',
        "TASK_TYPE":'{{$.InstanceTypeHook}}', "TASK_NAME":'{{$hookSchema.Name}}',
        "SCHEDULED_AT":'{{ "{{ next_execution_date }}" }}',
        # rest of the env vars are pulled from the container by making a GRPC call to optimus
   },
   reattach_on_restart=True,
)
{{- end }}
# hooks loop ends


# create upstream sensors
{{ $baseWindow := $.Job.Task.Window }}
{{- range $_, $dependency := $.Job.Dependencies}}
{{- $dependencySchema := $dependency.Job.Task.Unit.GetTaskSchema $.Context $.TaskSchemaRequest }}

{{- if eq $dependency.Type $.JobSpecDependencyTypeIntra }}
wait_{{$dependency.Job.Name | replace "-" "__dash__" | replace "." "__dot__"}} = SuperExternalTaskSensor(
    external_dag_id = "{{$dependency.Job.Name}}",
    window_size = {{$baseWindow.Size.String | quote}},
    window_offset = {{$baseWindow.Offset.String | quote}},
    window_truncate_to = {{$baseWindow.TruncateTo | quote}},
    optimus_hostname = "{{$.Hostname}}",
    task_id = "wait_{{$dependency.Job.Name | trunc 200}}-{{$dependencySchema.Name}}",
    poke_interval = SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout = SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    dag=dag
)
{{- end -}}

{{- if eq $dependency.Type $.JobSpecDependencyTypeInter }}
wait_{{$dependency.Job.Name | replace "-" "__dash__" | replace "." "__dot__"}} = CrossTenantDependencySensor(
    optimus_hostname="{{$.Hostname}}",
    optimus_project="{{$dependency.Project.Name}}",
    optimus_job="{{$dependency.Job.Name}}",
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    task_id="wait_{{$dependency.Job.Name | trunc 200}}-{{$dependencySchema.Name}}",
    dag=dag
)
{{- end -}}
{{- end}}

# arrange inter task dependencies
####################################

# upstream sensors -> base transformation task
{{- range $i, $t := $.Job.Dependencies }}
wait_{{ $t.Job.Name | replace "-" "__dash__" | replace "." "__dot__" }} >> transformation_{{$baseTaskSchema.Name | replace "-" "__dash__" | replace "." "__dot__"}}
{{- end}}

# set inter-dependencies between task and hooks
{{- range $_, $task := .Job.Hooks }}
{{- $hookSchema := $task.Unit.GetHookSchema $.Context $.HookSchemaRequest }}
{{- if eq $hookSchema.Type $.HookTypePre }}
hook_{{$hookSchema.Name}} >> transformation_{{$baseTaskSchema.Name | replace "-" "__dash__" | replace "." "__dot__"}}
{{- end -}}
{{- if eq $hookSchema.Type $.HookTypePost }}
transformation_{{$baseTaskSchema.Name | replace "-" "__dash__" | replace "." "__dot__"}} >> hook_{{$hookSchema.Name}}
{{- end -}}
{{- end }}

# set inter-dependencies between hooks and hooks
{{- range $_, $t := .Job.Hooks }}
{{- $hookSchema := $t.Unit.GetHookSchema $.Context $.HookSchemaRequest }}
{{- range $_, $depend := $t.DependsOn }}
{{- $dependHookSchema := $depend.Unit.GetHookSchema $.Context $.HookSchemaRequest }}
hook_{{$dependHookSchema.Name}} >> hook_{{$hookSchema.Name}}
{{- end }}
{{- end }}
