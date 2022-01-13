import json
import logging
import os
from datetime import datetime
from typing import List
import pendulum

import requests
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils import pod_launcher
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.kubernetes import kube_client
from airflow.models import (XCOM_RETURN_KEY, DagModel,
                            DagRun, Variable, XCom)
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from croniter import croniter
from airflow.configuration import conf

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# UTC time zone as a tzinfo instance.
utc = pendulum.timezone('UTC')


def lookup_non_standard_cron_expression(expr: str) -> str:
    expr_mapping = {
        '@yearly': '0 0 1 1 *',
        '@annually': '0 0 1 1 *',
        '@monthly': '0 0 1 * *',
        '@weekly': '0 0 * * 0',
        '@daily': '0 0 * * *',
        '@midnight': '0 0 * * *',
        '@hourly': '0 * * * *',
    }
    try:
        return expr_mapping[expr]
    except KeyError:
        return expr


class SuperKubernetesPodOperator(KubernetesPodOperator):
    """
    ** SAME AS KubernetesPodOperator: Execute a task in a Kubernetes Pod **
    Wrapper to push xcom as a return value key even if container completes with non success status

    .. note: keep this up to date if there is any change in KubernetesPodOperator execute method
    """
    template_fields = ('image', 'cmds', 'arguments', 'env_vars', 'config_file', 'pod_template_file')

    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs):
        super(SuperKubernetesPodOperator, self).__init__(*args, **kwargs)

        self.do_xcom_push = kwargs.get('do_xcom_push')
        self.namespace = kwargs.get('namespace')
        self.in_cluster = kwargs.get('in_cluster')
        self.cluster_context = kwargs.get('cluster_context')
        self.reattach_on_restart = kwargs.get('reattach_on_restart')
        self.config_file = kwargs.get('config_file')

    def execute(self, context):
        try:
            if self.in_cluster is not None:
                client = kube_client.get_kube_client(in_cluster=self.in_cluster,
                                                     cluster_context=self.cluster_context,
                                                     config_file=self.config_file)
            else:
                client = kube_client.get_kube_client(cluster_context=self.cluster_context,
                                                     config_file=self.config_file)

            self.pod = self.create_pod_request_obj()
            self.namespace = self.pod.metadata.namespace
            self.client = client

            # Add combination of labels to uniquely identify a running pod
            labels = self.create_labels_for_pod(context)

            label_selector = self._get_pod_identifying_label_string(labels)

            pod_list = client.list_namespaced_pod(self.namespace, label_selector=label_selector)

            if len(pod_list.items) > 1 and self.reattach_on_restart:
                raise AirflowException(
                    'More than one pod running with labels: '
                    '{label_selector}'.format(label_selector=label_selector))

            launcher = pod_launcher.PodLauncher(kube_client=client, extract_xcom=self.do_xcom_push)

            if len(pod_list.items) == 1:
                try_numbers_match = self._try_numbers_match(context, pod_list.items[0])
                final_state, result = self.handle_pod_overlap(labels, try_numbers_match, launcher, pod_list.items[0])
            else:
                final_state, _, result = self.create_new_pod_for_operator(labels, launcher)

            if final_state != State.SUCCESS:
                # push xcom value even if pod fails
                context.get('task_instance').xcom_push(key=XCOM_RETURN_KEY, value=result)
                raise AirflowException(
                    'Pod returned a failure: {state}'.format(state=final_state))
            return result
        except AirflowException as ex:
            raise AirflowException('Pod Launching failed: {error}'.format(error=ex))


class SuperExternalTaskSensor(BaseSensorOperator):
    """
    Waits for a different DAG or a task in a different DAG to complete for a
    specific execution window

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: str
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param window_size: size of the window in hours to look for successful 
        runs in upstream dag. E.g, "24h" will check for last 24 hours from
        current execution date of this dag. It checks for number of successful
        iterations of upstream dag in provided window. All of them needs to be
        successful for this sensor to complete. Defaults to a day of window(24)
    :type window_size: str
    """

    @apply_defaults
    def __init__(self,
                 external_dag_id,
                 window_size: str,
                 window_offset: str,
                 window_truncate_to: str,
                 optimus_hostname: str,
                 *args,
                 **kwargs):

        # Sensor's have two mode of operations: 'poke' and 'reschedule'. 'poke'
        # mode is like having a while loop. when the scheduler runs the task, the
        # sensor keeps checking for predicate condition until it becomes true. This
        # has the effect that once a sensor starts, it keeps taking resources until 
        # it senses that the predicate has been met. when set to 'reschedule' it exits
        # immediately if the predicate is false and is scheduled at a later time.
        # see the documentation for BaseSensorOperator for more information
        kwargs['mode'] = kwargs.get('mode', 'reschedule')

        self.upstream_dag = external_dag_id
        self.window_size = window_size
        self.window_offset = window_offset
        self.window_truncate_to = window_truncate_to
        self.allowed_upstream_states = [State.SUCCESS]
        self._optimus_client = OptimusAPIClient(optimus_hostname)

        super(SuperExternalTaskSensor, self).__init__(*args, **kwargs)

    @provide_session
    def poke(self, context, session=None):

        dag_to_wait = session.query(DagModel).filter(
            DagModel.dag_id == self.upstream_dag
        ).first()

        # check if valid upstream dag
        if not dag_to_wait:
            raise AirflowException('The external DAG '
                                   '{} does not exist.'.format(self.upstream_dag))
        else:
            if not os.path.exists(dag_to_wait.fileloc):
                raise AirflowException('The external DAG '
                                       '{} was deleted.'.format(self.upstream_dag))

        # calculate windows
        execution_date = context['execution_date']
        window_start, window_end = self.generate_window(execution_date, self.window_size)
        self.log.info(
            "consuming upstream window between: {} - {}".format(window_start.isoformat(), window_end.isoformat()))
        self.log.info("upstream interval: {}".format(dag_to_wait.schedule_interval))

        # find success iterations we need in window
        expected_upstream_executions = self._get_expected_upstream_executions(dag_to_wait.schedule_interval,
                                                                              window_start, window_end)
        self.log.info("expected upstream executions ({}): {}".format(len(expected_upstream_executions),
                                                                     expected_upstream_executions))

        # upstream dag runs between input window with success state
        actual_upstream_executions = [r.execution_date for r in session.query(DagRun.execution_date)
            .filter(
            DagRun.dag_id == self.upstream_dag,
            DagRun.execution_date > window_start.replace(tzinfo=utc),
            DagRun.execution_date <= window_end.replace(tzinfo=utc),
            DagRun.external_trigger == False,
            DagRun.state.in_(self.allowed_upstream_states)
        ).order_by(DagRun.execution_date).all()]
        self.log.info(
            "actual upstream executions ({}): {}".format(len(actual_upstream_executions), actual_upstream_executions))

        missing_upstream_executions = set(expected_upstream_executions) - set(actual_upstream_executions)
        if len(missing_upstream_executions) > 0:
            self.log.info("missing upstream executions : {}".format(missing_upstream_executions))
            self.log.warning(
                "unable to find enough DagRun instances for upstream '{}' dated between {} and {}(inclusive), rescheduling sensor"
                    .format(self.upstream_dag, window_start.isoformat(), window_end.isoformat()))
            return False

        return True

    def generate_window(self, execution_date, window_size):
        format_rfc3339 = "%Y-%m-%dT%H:%M:%SZ"
        execution_date_str = execution_date.strftime(format_rfc3339)
        # ignore offset & truncateto
        task_window = JobSpecTaskWindow(window_size, 0, "m", self._optimus_client)
        return task_window.get(execution_date_str)

    def _get_expected_upstream_executions(self, schedule_interval, window_start, window_end):
        expected_upstream_executions = []
        cron_schedule = lookup_non_standard_cron_expression(schedule_interval)
        dag_cron = croniter(cron_schedule, window_start.replace(tzinfo=None))
        while True:
            next_run = dag_cron.get_next(datetime)
            if next_run > window_end.replace(tzinfo=None):
                break
            expected_upstream_executions.append(next_run.replace(tzinfo=utc))
        return expected_upstream_executions


class OptimusAPIClient:
    def __init__(self, optimus_host):
        self.host = self._add_connection_adapter_if_absent(optimus_host)

    def _add_connection_adapter_if_absent(self, host):
        if host.startswith("http://") or host.startswith("https://"):
            return host
        return "http://" + host

    def get_job_run_status(self, optimus_project: str, optimus_job: str) -> dict:
        url = '{optimus_host}/api/v1beta1/project/{optimus_project}/job/{optimus_job}/status'.format(
            optimus_host=self.host,
            optimus_project=optimus_project,
            optimus_job=optimus_job,
        )
        response = requests.get(url)
        self._raise_error_if_request_failed(response)
        return response.json()

    def get_task_window(self, scheduled_at: str, window_size: str, window_offset: str,
                        window_truncate_upto: str) -> dict:
        url = '{optimus_host}/api/v1beta1/window?scheduledAt={scheduled_at}&size={window_size}&offset={window_offset}&truncate_to={window_truncate_upto}'.format(
            optimus_host=self.host,
            scheduled_at=scheduled_at,
            window_size=window_size,
            window_offset=window_offset,
            window_truncate_upto=window_truncate_upto,
        )
        response = requests.get(url)
        self._raise_error_if_request_failed(response)
        return response.json()

    def get_job_metadata(self, execution_date, project, job) -> dict:
        url = '{optimus_host}/api/v1beta1/project/{project_name}/job/{job_name}/instance'.format(optimus_host=self.host,
                                                                                            project_name=project,
                                                                                            job_name=job)
        request_data = {
            "scheduled_at": execution_date,
            "instance_type": "TYPE_TASK",
            "instance_name": "none"
        }
        response = requests.post(url, data=json.dumps(request_data))
        self._raise_error_if_request_failed(response)
        return response.json()

    def notify_event(self, project, namespace, job, event) -> dict:
        url = '{optimus_host}/api/v1beta1/project/{project_name}/namespace/{namespace}/job/{job_name}/event'.format(
            optimus_host=self.host,
            project_name=project,
            namespace=namespace,
            job_name=job,
        )
        request_data = {
            "event": event
        }
        response = requests.post(url, data=json.dumps(request_data))
        self._raise_error_if_request_failed(response)
        return response.json()

    def _raise_error_if_request_failed(self, response):
        if response.status_code != 200:
            log.error("Request to optimus returned non-200 status code. Server response:\n")
            log.error(response.json())
            raise AssertionError("request to optimus returned non-200 status code. url: " + response.url)


class JobSpecTaskWindow:
    def __init__(self, size: str, offset: str, truncate_to: str, optimus_client: OptimusAPIClient):
        self.size = size
        self.offset = offset
        self.truncate_to = truncate_to
        self._optimus_client = optimus_client

    def get(self, scheduled_at: str) -> (datetime, datetime):
        api_response = self._fetch_task_window(scheduled_at)
        return (
            self._parse_datetime(api_response['start']),
            self._parse_datetime(api_response['end']),
        )

    def _parse_datetime(self, timestamp):
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")

    def _fetch_task_window(self, scheduled_at: str) -> dict:
        return self._optimus_client.get_task_window(scheduled_at, self.size, self.offset, self.truncate_to)


class CrossTenantDependencySensor(BaseSensorOperator):
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    TIMESTAMP_MS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    @apply_defaults
    def __init__(
            self,
            optimus_hostname: str,
            upstream_optimus_project: str,
            upstream_optimus_job: str,
            window_size: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.optimus_project = upstream_optimus_project
        self.optimus_job = upstream_optimus_job
        self.window_size = window_size
        self._optimus_client = OptimusAPIClient(optimus_hostname)

    def poke(self, context):
        execution_date = context['execution_date']
        execution_date_str = execution_date.strftime(self.TIMESTAMP_FORMAT)

        # parse relevant metadata from the job metadata to build the task window
        job_metadata = self._optimus_client.get_job_metadata(execution_date_str, self.optimus_project, self.optimus_job)
        cron_schedule = lookup_non_standard_cron_expression(job_metadata['job']['interval'])

        window_start, window_end = self.generate_window(execution_date, self.window_size)
        self.log.info(
            "consuming upstream window between: {} - {}".format(window_start.isoformat(), window_end.isoformat()))
        expected_upstream_executions = self._get_expected_upstream_executions(cron_schedule, window_start, window_end)
        self.log.info("expected upstream executions ({}): {}".format(len(expected_upstream_executions),
                                                                     expected_upstream_executions))

        actual_upstream_success_executions = self._get_successful_job_executions()
        self.log.info("actual upstream executions ({}): {}".format(len(actual_upstream_success_executions),
                                                                   actual_upstream_success_executions))

        # determine if all expected are present in actual
        missing_upstream_executions = set(expected_upstream_executions) - set(actual_upstream_success_executions)
        if len(missing_upstream_executions) > 0:
            self.log.info("missing upstream executions : {}".format(missing_upstream_executions))
            self.log.warning("unable to find enough successful executions for upstream '{}' in "
                             "'{}' dated between {} and {}(inclusive), rescheduling sensor".format(
                self.optimus_job, self.optimus_project, window_start.isoformat(), window_end.isoformat()))
            return False

        return True

    def _get_expected_upstream_executions(self, cron_schedule, window_start, window_end):
        expected_upstream_executions = []
        dag_cron = croniter(cron_schedule, window_start.replace(tzinfo=None))
        while True:
            next_run = dag_cron.get_next(datetime)
            if next_run > window_end.replace(tzinfo=None):
                break
            expected_upstream_executions.append(next_run)
        return expected_upstream_executions

    def _get_successful_job_executions(self) -> List[datetime]:
        api_response = self._optimus_client.get_job_run_status(self.optimus_project, self.optimus_job)
        actual_upstream_success_executions = []
        for job_run in api_response['statuses']:
            if job_run['state'] == 'success':
                actual_upstream_success_executions.append(self._parse_datetime(job_run['scheduledAt']))
        return actual_upstream_success_executions

    def _parse_datetime(self, timestamp) -> datetime:
        try:
            return datetime.strptime(timestamp, self.TIMESTAMP_FORMAT)
        except ValueError:
            return datetime.strptime(timestamp, self.TIMESTAMP_MS_FORMAT)

    def generate_window(self, execution_date, window_size):
        format_rfc3339 = "%Y-%m-%dT%H:%M:%SZ"
        execution_date_str = execution_date.strftime(format_rfc3339)
        # ignore offset & truncate to
        task_window = JobSpecTaskWindow(window_size, 0, "m", self._optimus_client)
        return task_window.get(execution_date_str)


def optimus_failure_notify(context):
    params = context.get("params")
    optimus_client = OptimusAPIClient(params["optimus_hostname"])

    taskfail_alert = int(Variable.get("taskfail_alert", default_var=1))
    if taskfail_alert != 1:
        return "suppressed failure alert"

    current_dag_id = context.get('task_instance').dag_id
    current_execution_date = context.get('execution_date')

    # failure message pushed by failed tasks
    failure_messages = []

    def _xcom_value_has_error(_xcom) -> bool:
        return _xcom.key == XCOM_RETURN_KEY and isinstance(_xcom.value, dict) and 'error' in _xcom.value and \
               _xcom.value['error'] is not None

    for xcom in XCom.get_many(
            current_execution_date,
            key=None,
            task_ids=None,
            dag_ids=current_dag_id,
            include_prior_dates=False,
            limit=10):
        if xcom.key == 'error':
            failure_messages.append(xcom.value)
        if _xcom_value_has_error(xcom):
            failure_messages.append(xcom.value['error'])
    failure_message = ", ".join(failure_messages)
    print("failures: {}".format(failure_message))

    message = {
        "log_url": context.get('task_instance').log_url,
        "task_id": context.get('task_instance').task_id,
        "run_id": context.get('run_id'),
        "duration": str(context.get('task_instance').duration),
        "message": failure_message,
        "exception": str(context.get('exception')),
        "scheduled_at": current_execution_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    event = {
        "type": "TYPE_FAILURE",
        "value": message,
    }
    # post event
    resp = optimus_client.notify_event(params["project_name"], params["namespace"], params["job_name"], event)
    print("posted event ", params, event, resp)
    return


def optimus_sla_miss_notify(dag, task_list, blocking_task_list, slas, blocking_tis):
    params = dag.params
    optimus_client = OptimusAPIClient(params["optimus_hostname"])

    slamiss_alert = int(Variable.get("slamiss_alert", default_var=1))
    if slamiss_alert != 1:
        return "suppressed slamiss alert"

    sla_list = []
    for sla in slas:
        sla_list.append({
            'task_id': sla.task_id,
            'dag_id': sla.dag_id,
            'scheduled_at': sla.execution_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            'timestamp': sla.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        })

    current_dag_id = dag.dag_id
    webserver_url = conf.get(section='webserver', key='base_url')
    message = {
        "slas": sla_list,
        "job_url": "{}/tree?dag_id={}".format(webserver_url, current_dag_id),
    }

    event = {
        "type": "TYPE_SLA_MISS",
        "value": message,
    }
    # post event
    resp = optimus_client.notify_event(params["project_name"], params["namespace"], params["job_name"], event)
    print("posted event ", params, event, resp)
    return


# everything below this is here for legacy reasons, should be cleaned up in future

def alert_failed_to_slack(context):
    SLACK_CONN_ID = "slack_alert"
    TASKFAIL_ALERT = int(Variable.get("taskfail_alert", default_var=1))
    SLACK_CHANNEL = Variable.get("slack_channel")

    def _xcom_value_has_error(_xcom) -> bool:
        return _xcom.key == XCOM_RETURN_KEY and isinstance(_xcom.value, dict) and 'error' in _xcom.value and \
               _xcom.value['error'] != None

    if TASKFAIL_ALERT != 1:
        return "suppressed failure alert"

    slack_token = ""
    try:
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    except:
        print("no slack connection variable set")
        return "{connection} connection variable not defined, unable to send alerts".format(connection=SLACK_CONN_ID)

    if not SLACK_CHANNEL:
        return "no slack channel variable set"

    current_dag_id = context.get('task_instance').dag_id
    current_task_id = context.get('task_instance').task_id
    current_execution_date = context.get('execution_date')

    # failure message pushed by failed tasks
    failure_messages = []
    for xcom in XCom.get_many(
            current_execution_date,
            key=None,
            task_ids=None,
            dag_ids=current_dag_id,
            include_prior_dates=False,
            limit=10):
        if xcom.key == 'error':
            failure_messages.append(xcom.value)
        if _xcom_value_has_error(xcom):
            failure_messages.append(xcom.value['error'])
    failure_message = ", ".join(failure_messages)
    print("failures: {}".format(failure_message))

    message_body = "\n".join([
        "• *DAG*: {}".format(current_dag_id),
        "• *Task*: {}".format(current_task_id),
        "• *Execution Time*: {}".format(current_execution_date),
        "• *Run ID*: {}".format(context.get('run_id'))
    ])

    message_footer = "\n".join([
        ":blob-facepalm: Owner: {}".format(context.get('dag').owner),
        ":hourglass: Duration: {} sec".format(context.get('task_instance').duration),
        ":memo: Details: {}".format(failure_message)
    ])

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Task failed :fire:"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message_body
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "style": "danger",
                    "text": {
                        "type": "plain_text",
                        "text": "View log :airflow:",
                    },
                    "url": context.get('task_instance').log_url,
                    "action_id": "view_log",
                }
            ]
        },
        {
            "type": "divider"
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": message_footer
                },
            ]
        },
    ]
    failed_alert = SlackAPIPostOperator(
        slack_conn_id=SLACK_CONN_ID,
        token=slack_token,
        blocks=blocks,
        task_id='slack_failed_alert',
        channel=SLACK_CHANNEL
    )
    return failed_alert.execute(context=context)
