# Code generated by optimus {{.Version}}. DO NOT EDIT.

from typing import Any, Callable, Dict, Optional
from datetime import datetime, timedelta, timezone

from airflow.models import DAG, Variable, DagRun, DagModel, TaskInstance, BaseOperator, XCom, XCOM_RETURN_KEY
from airflow.kubernetes.secret import Secret
from airflow.configuration import conf
from airflow.utils.weight_rule import WeightRule
from kubernetes.client import models as k8s

from __lib import optimus_failure_notify, optimus_sla_miss_notify, SuperKubernetesPodOperator, \
    SuperExternalTaskSensor, CrossTenantDependencySensor

SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS = int(Variable.get("sensor_poke_interval_in_secs", default_var=15 * 60))
SENSOR_DEFAULT_TIMEOUT_IN_SECS = int(Variable.get("sensor_timeout_in_secs", default_var=15 * 60 * 60))
DAG_RETRIES = int(Variable.get("dag_retries", default_var=3))
DAG_RETRY_DELAY = int(Variable.get("dag_retry_delay_in_secs", default_var=5 * 60))
DAGRUN_TIMEOUT_IN_SECS = int(Variable.get("dagrun_timeout_in_secs", default_var=3 * 24 * 60 * 60))

default_args = {
    "params": {
        "project_name": {{.Namespace.ProjectSpec.Name | quote}},
        "namespace": {{.Namespace.Name | quote}},
        "job_name": {{.Job.Name | quote}},
        "optimus_hostname": {{.Hostname | quote}}
    },
    "owner": {{.Job.Owner | quote}},
    "depends_on_past": {{ if .Job.Behavior.DependsOnPast }} True {{- else -}} False {{- end -}},
    "retries": {{ if gt .Job.Behavior.Retry.Count 0 -}} {{.Job.Behavior.Retry.Count}} {{- else -}} DAG_RETRIES {{- end}},
    "retry_delay": {{ if gt .Job.Behavior.Retry.Delay.Nanoseconds 0 -}} timedelta(seconds={{.Job.Behavior.Retry.Delay.Seconds}}) {{- else -}} timedelta(seconds=DAG_RETRY_DELAY) {{- end}},
    "retry_exponential_backoff": {{if .Job.Behavior.Retry.ExponentialBackoff -}}True{{- else -}}False{{- end -}},
    "priority_weight": {{.Job.Task.Priority}},
    "start_date": datetime.strptime({{ .Job.Schedule.StartDate.Format "2006-01-02T15:04:05" | quote }}, "%Y-%m-%dT%H:%M:%S"),
    {{if .Job.Schedule.EndDate -}}"end_date": datetime.strptime({{ .Job.Schedule.EndDate.Format "2006-01-02T15:04:05" | quote}},"%Y-%m-%dT%H:%M:%S"),{{- else -}}{{- end}}
    "on_failure_callback": optimus_failure_notify,
    "weight_rule": WeightRule.ABSOLUTE
}

dag = DAG(
    dag_id={{.Job.Name | quote}},
    default_args=default_args,
    schedule_interval={{ if eq .Job.Schedule.Interval "" }}None{{- else -}} {{ .Job.Schedule.Interval | quote}}{{end}},
    sla_miss_callback=optimus_sla_miss_notify,
    catchup={{ if .Job.Behavior.CatchUp }}True{{ else }}False{{ end }},
    dagrun_timeout=timedelta(seconds=DAGRUN_TIMEOUT_IN_SECS)
)

{{$baseTaskSchema := .Job.Task.Unit.Info -}}
{{ if ne $baseTaskSchema.SecretPath "" -}}
transformation_secret = Secret(
    "volume",
    {{ dir $baseTaskSchema.SecretPath | quote }},
    "optimus-task-{{ $baseTaskSchema.Name }}",
    {{ base $baseTaskSchema.SecretPath | quote }}
)
{{- end }}

{{- $setCPURequest := not (empty .Metadata.Resource.Request.CPU) -}}
{{- $setMemoryRequest := not (empty .Metadata.Resource.Request.Memory) -}}
{{- $setCPULimit := not (empty .Metadata.Resource.Limit.CPU) -}}
{{- $setMemoryLimit := not (empty .Metadata.Resource.Limit.Memory) -}}
{{- $setResourceConfig := or $setCPURequest $setMemoryRequest $setCPULimit $setMemoryLimit }}

{{- if $setResourceConfig -}}
resources = k8s.V1ResourceRequirements (
    {{- if or $setCPURequest $setMemoryRequest }}
    requests = {
        {{- if $setMemoryRequest }}
        'memory': '{{.Metadata.Resource.Request.Memory}}',
        {{- end }}
        {{- if $setCPURequest }}
        'cpu': '{{.Metadata.Resource.Request.CPU}}',
        {{- end }}
    },
    {{- end }}
    {{- if or $setCPULimit $setMemoryLimit }}
    limits = {
        {{- if $setMemoryLimit }}
        'memory': '{{.Metadata.Resource.Limit.Memory}}',
        {{- end }}
        {{- if $setCPULimit }}
        'cpu': '{{.Metadata.Resource.Limit.CPU}}',
        {{- end }}
    },
    {{- end }}
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
        "JOB_NAME":'{{.Job.Name}}', "OPTIMUS_HOSTNAME":'{{.Hostname}}',
        "JOB_LABELS":'{{.Job.GetLabelsAsString}}', "NAMESPACE":'{{.Namespace.Name}}',
        "JOB_DIR":'/data', "PROJECT":'{{.Namespace.ProjectSpec.Name}}',
        "INSTANCE_TYPE":'{{$.InstanceTypeTask}}', "INSTANCE_NAME":'{{$baseTaskSchema.Name}}',
        "SCHEDULED_AT":'{{ "{{ next_execution_date }}" }}',
    },
{{ if gt .SLAMissDurationInSec 0 -}}
    sla=timedelta(seconds={{ .SLAMissDurationInSec }}),
{{- end }}
{{- if $setResourceConfig }}
    resources = resources,
{{- end }}
    reattach_on_restart=True
)

# hooks loop start
{{ range $_, $t := .Job.Hooks }}
{{ $hookSchema := $t.Unit.Info -}}
{{- if ne $hookSchema.SecretPath "" -}}
hook_{{$hookSchema.Name | replace "-" "_"}}_secret = Secret(
    "volume",
    {{ dir $hookSchema.SecretPath | quote }},
    "optimus-hook-{{ $hookSchema.Name }}",
    {{ base $hookSchema.SecretPath | quote }}
)
{{- end}}

hook_{{$hookSchema.Name | replace "-" "__dash__"}} = SuperKubernetesPodOperator(
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
        "JOB_NAME":'{{$.Job.Name}}', "OPTIMUS_HOSTNAME":'{{$.Hostname}}',
        "JOB_LABELS":'{{$.Job.GetLabelsAsString}}', "NAMESPACE":'{{$.Namespace.Name}}',
        "JOB_DIR":'/data', "PROJECT":'{{$.Namespace.ProjectSpec.Name}}',
        "INSTANCE_TYPE":'{{$.InstanceTypeHook}}', "INSTANCE_NAME":'{{$hookSchema.Name}}',
        "SCHEDULED_AT":'{{ "{{ next_execution_date }}" }}',
        # rest of the env vars are pulled from the container by making a GRPC call to optimus
    },
{{- if eq $hookSchema.HookType $.HookTypeFail }}
    trigger_rule="one_failed",
{{- end }}
{{- if $setResourceConfig }}
    resources = resources,
{{- end }}
    reattach_on_restart=True
)
{{- end }}
# hooks loop ends


# create upstream sensors
{{ $baseWindow := $.Job.Task.Window }}
{{- range $_, $dependency := $.Job.Dependencies}}
{{- $dependencySchema := $dependency.Job.Task.Unit.Info }}

{{- if eq $dependency.Type $.JobSpecDependencyTypeIntra }}
wait_{{$dependency.Job.Name | replace "-" "__dash__" | replace "." "__dot__"}} = SuperExternalTaskSensor(
    external_dag_id="{{$dependency.Job.Name}}",
    window_size={{$baseWindow.Size.String | quote}},
    window_offset={{$baseWindow.Offset.String | quote}},
    window_truncate_to={{$baseWindow.TruncateTo | quote}},
    optimus_hostname="{{$.Hostname}}",
    task_id="wait_{{$dependency.Job.Name | trunc 200}}-{{$dependencySchema.Name}}",
    poke_interval=SENSOR_DEFAULT_POKE_INTERVAL_IN_SECS,
    timeout=SENSOR_DEFAULT_TIMEOUT_IN_SECS,
    dag=dag
)
{{- end -}}

{{- if eq $dependency.Type $.JobSpecDependencyTypeInter }}
wait_{{$dependency.Job.Name | replace "-" "__dash__" | replace "." "__dot__"}} = CrossTenantDependencySensor(
    optimus_hostname="{{$.Hostname}}",
    upstream_optimus_project="{{$dependency.Project.Name}}",
    upstream_optimus_job="{{$dependency.Job.Name}}",
    window_size="{{ $baseWindow.Size.String }}",
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
{{- $hookSchema := $task.Unit.Info }}
{{- if eq $hookSchema.HookType $.HookTypePre }}
hook_{{$hookSchema.Name | replace "-" "__dash__"}} >> transformation_{{$baseTaskSchema.Name | replace "-" "__dash__" | replace "." "__dot__"}}
{{- end -}}
{{- if eq $hookSchema.HookType $.HookTypePost }}
transformation_{{$baseTaskSchema.Name | replace "-" "__dash__" | replace "." "__dot__"}} >> hook_{{$hookSchema.Name | replace "-" "__dash__"}}
{{- end -}}
{{- if eq $hookSchema.HookType $.HookTypeFail }}
transformation_{{$baseTaskSchema.Name | replace "-" "__dash__" | replace "." "__dot__"}} >> hook_{{$hookSchema.Name | replace "-" "__dash__"}}
{{- end -}}
{{- end }}

# set inter-dependencies between hooks and hooks
{{- range $_, $t := .Job.Hooks }}
{{- $hookSchema := $t.Unit.Info }}
{{- range $_, $depend := $t.DependsOn }}
{{- $dependHookSchema := $depend.Unit.Info }}
hook_{{$dependHookSchema.Name | replace "-" "__dash__"}} >> hook_{{$hookSchema.Name | replace "-" "__dash__"}}
{{- end }}
{{- end }}

# arrange failure hook after post hooks
{{- range $_, $task := .Job.Hooks -}}
{{- $hookSchema := $task.Unit.Info }}

{{- if eq $hookSchema.HookType $.HookTypePost }}

hook_{{$hookSchema.Name | replace "-" "__dash__"}} >> [
{{- range $_, $ftask := $.Job.Hooks }}
{{- $fhookSchema := $ftask.Unit.Info }}
{{- if eq $fhookSchema.HookType $.HookTypeFail }} hook_{{$fhookSchema.Name | replace "-" "__dash__"}}, {{- end -}}
{{- end -}}
]

{{- end -}}

{{- end -}}