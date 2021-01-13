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


class XWindow():
    """
    Generate window based on user config inputs
    """
    def __init__(self, end_time: datetime, window_size: str, window_offset: str, window_truncate_upto: str):
        floating_end = end_time

        # apply truncation
        if window_truncate_upto == "h":
            # remove time upto hours
            floating_end = floating_end.replace(minute=0, second=0, microsecond=0)
        elif window_truncate_upto == "d":
            # remove time upto days
            floating_end = floating_end.replace(hour=0, minute=0, second=0, microsecond=0)
        elif window_truncate_upto == "w":
            # remove time upto days
            # get week lists for current month
            week_matrix_per_month = calendar.Calendar().monthdatescalendar(end_time.year, end_time.month)
            # find week where current day lies
            current_week = None
            for week in week_matrix_per_month:
                for day in week:
                    if day == end_time.date():
                        current_week = week

            floating_end = datetime.combine(current_week[6], end_time.min.time())
            floating_end = floating_end.replace(tzinfo=end_time.tzinfo)
        elif window_truncate_upto == "" or window_truncate_upto == "0":
            # do nothing
            floating_end = floating_end
        else:
            raise Exception("unsupported truncate method: {}".format(window_truncate_upto))

        # generate shift & length
        self._offset = self.time_parse_util(window_offset)
        self._size = self.time_parse_util(window_size)

        self._end = floating_end + self._offset
        self._start = self._end - self._size
        self._truncate_upto = window_truncate_upto
        pass

    def time_parse_util(self, time_str: str):
        """
        :param time_str: example 1d, 2h
        :return: timedelta object
        """

        SIGN        = r'(?P<sign>[+|-])?'
        #YEARS      = r'(?P<years>\d+)\s*(?:ys?|yrs?.?|years?)'
        #MONTHS     = r'(?P<months>\d+)\s*(?:mos?.?|mths?.?|months?)'
        WEEKS       = r'(?P<weeks>[\d.]+)\s*(?:w|wks?|weeks?)'
        DAYS        = r'(?P<days>[\d.]+)\s*(?:d|dys?|days?)'
        HOURS       = r'(?P<hours>[\d.]+)\s*(?:h|hrs?|hours?)'
        MINS        = r'(?P<mins>[\d.]+)\s*(?:m|(mins?)|(minutes?))'
        SECS        = r'(?P<secs>[\d.]+)\s*(?:s|secs?|seconds?)'
        SEPARATORS  = r'[,/]'
        SECCLOCK    = r':(?P<secs>\d{2}(?:\.\d+)?)'
        MINCLOCK    = r'(?P<mins>\d{1,2}):(?P<secs>\d{2}(?:\.\d+)?)'
        HOURCLOCK   = r'(?P<hours>\d+):(?P<mins>\d{2}):(?P<secs>\d{2}(?:\.\d+)?)'
        DAYCLOCK    = (r'(?P<days>\d+):(?P<hours>\d{2}):'
                    r'(?P<mins>\d{2}):(?P<secs>\d{2}(?:\.\d+)?)')

        OPT         = lambda x: r'(?:{x})?'.format(x=x, SEPARATORS=SEPARATORS)
        OPTSEP      = lambda x: r'(?:{x}\s*(?:{SEPARATORS}\s*)?)?'.format(
            x=x, SEPARATORS=SEPARATORS)

        TIMEFORMATS = [
            r'{WEEKS}\s*{DAYS}\s*{HOURS}\s*{MINS}\s*{SECS}'.format(
                #YEARS=OPTSEP(YEARS),
                #MONTHS=OPTSEP(MONTHS),
                WEEKS=OPTSEP(WEEKS),
                DAYS=OPTSEP(DAYS),
                HOURS=OPTSEP(HOURS),
                MINS=OPTSEP(MINS),
                SECS=OPT(SECS)),
            r'{MINCLOCK}'.format(
                MINCLOCK=MINCLOCK),
            r'{WEEKS}\s*{DAYS}\s*{HOURCLOCK}'.format(
                WEEKS=OPTSEP(WEEKS),
                DAYS=OPTSEP(DAYS),
                HOURCLOCK=HOURCLOCK),
            r'{DAYCLOCK}'.format(
                DAYCLOCK=DAYCLOCK),
            r'{SECCLOCK}'.format(
                SECCLOCK=SECCLOCK),
            #r'{YEARS}'.format(
                #YEARS=YEARS),
            #r'{MONTHS}'.format(
                #MONTHS=MONTHS),
            ]

        COMPILED_SIGN = re.compile(r'\s*' + SIGN + r'\s*(?P<unsigned>.*)$')
        COMPILED_TIMEFORMATS = [re.compile(r'\s*' + timefmt + r'\s*$', re.I)
                                for timefmt in TIMEFORMATS]

        MULTIPLIERS = dict([
                #('years',  60 * 60 * 24 * 365),
                #('months', 60 * 60 * 24 * 30),
                ('weeks',   60 * 60 * 24 * 7),
                ('days',    60 * 60 * 24),
                ('hours',   60 * 60),
                ('mins',    60),
                ('secs',    1)
                ])

        def _interpret_as_minutes(sval, mdict):
            """
            Times like "1:22" are ambiguous; do they represent minutes and seconds
            or hours and minutes?  By default, timeparse assumes the latter.  Call
            this function after parsing out a dictionary to change that assumption.

            >>> import pprint
            >>> pprint.pprint(_interpret_as_minutes('1:24', {'secs': '24', 'mins': '1'}))
            {'hours': '1', 'mins': '24'}
            """
            if (    sval.count(':') == 1
                and '.' not in sval
                and (('hours' not in mdict) or (mdict['hours'] is None))
                and (('days' not in mdict) or (mdict['days'] is None))
                and (('weeks' not in mdict) or (mdict['weeks'] is None))
                #and (('months' not in mdict) or (mdict['months'] is None))
                #and (('years' not in mdict) or (mdict['years'] is None))
                ):
                mdict['hours'] = mdict['mins']
                mdict['mins'] = mdict['secs']
                mdict.pop('secs')
                pass
            return mdict

        def _timeparse(sval, granularity='seconds'):
            '''
            Parse a time expression, returning it as a number of seconds.  If
            possible, the return value will be an `int`; if this is not
            possible, the return will be a `float`.  Returns `None` if a time
            expression cannot be parsed from the given string.
            Arguments:
            - `sval`: the string value to parse
            >>> timeparse('1:24')
            84
            >>> timeparse(':22')
            22
            >>> timeparse('1 minute, 24 secs')
            84
            >>> timeparse('1m24s')
            84
            >>> timeparse('1.2 minutes')
            72
            >>> timeparse('1.2 seconds')
            1.2
            Time expressions can be signed.
            >>> timeparse('- 1 minute')
            -60
            >>> timeparse('+ 1 minute')
            60

            If granularity is specified as ``minutes``, then ambiguous digits following
            a colon will be interpreted as minutes; otherwise they are considered seconds.

            >>> timeparse('1:30')
            90
            >>> timeparse('1:30', granularity='minutes')
            5400
            '''
            match = COMPILED_SIGN.match(sval)
            sign = -1 if match.groupdict()['sign'] == '-' else 1
            sval = match.groupdict()['unsigned']
            for timefmt in COMPILED_TIMEFORMATS:
                match = timefmt.match(sval)
                if match and match.group(0).strip():
                    mdict = match.groupdict()
                    if granularity == 'minutes':
                        mdict = _interpret_as_minutes(sval, mdict)
                    # if all of the fields are integer numbers
                    if all(v.isdigit() for v in list(mdict.values()) if v):
                        return sign * sum([MULTIPLIERS[k] * int(v, 10) for (k, v) in
                                    list(mdict.items()) if v is not None])
                    # if SECS is an integer number
                    elif ('secs' not in mdict or
                        mdict['secs'] is None or
                        mdict['secs'].isdigit()):
                        # we will return an integer
                        return (
                            sign * int(sum([MULTIPLIERS[k] * float(v) for (k, v) in
                                    list(mdict.items()) if k != 'secs' and v is not None])) +
                            (int(mdict['secs'], 10) if mdict['secs'] else 0))
                    else:
                        # SECS is a float, we will return a float
                        return sign * sum([MULTIPLIERS[k] * float(v) for (k, v) in
                                    list(mdict.items()) if v is not None])

        if time_str == "" or time_str == "0":
            return timedelta(seconds=0)
        return timedelta(seconds=_timeparse(time_str))


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
        dstart_regex = r"\bdstart\b"
        dend_regex = r"\bdend\b"

        # process filter expression window macros
        self.log.info("templatized env_vars: {}".format(self.env_vars))

        next_scheduled_at = self.env_vars.get('NEXT_SCHEDULED_AT')
        if next_scheduled_at is not None and self.env_vars.get('WINDOW_SIZE') is not None:
            window_end = datetime.fromisoformat(next_scheduled_at)
            window = XWindow(window_end, self.env_vars.get('WINDOW_SIZE'), self.env_vars.get('WINDOW_OFFSET'), self.env_vars.get('WINDOW_TRUNCATE_UPTO'))
            filter_expression = self.env_vars.get('FILTER_EXPRESSION')
            if filter_expression is not None:
                filter_expression = re.sub(dstart_regex, window._start.strftime("%Y-%m-%d %H:%M:%S"), filter_expression, 0, re.MULTILINE)
                filter_expression = re.sub(dend_regex, window._end.strftime("%Y-%m-%d %H:%M:%S"), filter_expression, 0, re.MULTILINE)
            self.env_vars['FILTER_EXPRESSION'] = filter_expression

        try:
            if self.in_cluster is not None:
                client = kube_client.get_kube_client(in_cluster=self.in_cluster,
                                                     cluster_context=self.cluster_context,
                                                     config_file=self.config_file)
            else:
                client = kube_client.get_kube_client(cluster_context=self.cluster_context,
                                                     config_file=self.config_file)

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
                final_state, result = self.handle_pod_overlap(labels, try_numbers_match, launcher, pod_list)
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
        runs in upstream dag. E.g, "24" will check for last 24 hours from
        current execution date of this dag. It checks for number of successful
        iterations of upstream dag in provided window. All of them needs to be
        successful for this sensor to complete. Defaults to a day of window(24)
    :type window_size: int
    """

    @apply_defaults
    def __init__(self, 
                external_dag_id,
                window_size: int,
                window_offset: int,
                window_truncate_upto: str,
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
        self.window_truncate_upto = window_truncate_upto
        self.allowed_upstream_states = [State.SUCCESS]

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
        window_start, window_end = self.generate_window(execution_date, self.window_size, self.window_offset, self.window_truncate_upto)
        self.log.info("consuming upstream window between: {} - {}".format(window_start.isoformat(), window_end.isoformat()))

        # find success iterations we need in window
        expected_upstream_executions = []
        dag_cron = croniter(dag_to_wait.schedule_interval, window_start.replace(tzinfo=None))
        while True:
            next_run = dag_cron.get_next(datetime)
            if next_run > window_end.replace(tzinfo=None):
                break
            expected_upstream_executions.append(next_run)
        self.log.info("expected upstream executions ({}): {}".format(len(expected_upstream_executions), expected_upstream_executions))

        # upstream dag runs between input window with success state
        actual_upstream_executions = [ r.execution_date for r in session.query(DagRun.execution_date)
            .filter(
                DagRun.dag_id == self.upstream_dag,
                DagRun.execution_date > window_start,
                DagRun.execution_date <= window_end,
                DagRun.external_trigger == False,
                DagRun.state.in_(self.allowed_upstream_states)
            ).order_by(DagRun.execution_date).all() ]
        self.log.info("actual upstream executions ({}): {}".format(len(actual_upstream_executions), actual_upstream_executions))

        missing_upstream_executions = set(expected_upstream_executions) - set(actual_upstream_executions)
        if len(missing_upstream_executions) > 0:
            self.log.info("missing upstream executions : {}".format(missing_upstream_executions))
            self.log.warning(
                "unable to find enough DagRun instances for upstream '{}' dated between {} and {}(inclusive), rescheduling sensor"
                    .format(self.upstream_dag, window_start.isoformat(), window_end.isoformat()))
            return False

        return True

    def generate_window(self, end_time, window_size, window_offset, window_truncate_upto):
        floating_end = end_time

        # apply truncation
        if window_truncate_upto == "w":
            # remove time upto days and find nearest week
            # get week lists for current month
            week_matrix_per_month = calendar.Calendar().monthdatescalendar(end_time.year, end_time.month)
            # find week where current day lies
            current_week = None
            for week in week_matrix_per_month:
                for day in week:
                    if day == end_time.date():
                        current_week = week

            floating_end = datetime.combine(current_week[6], end_time.timetz())
            floating_end = floating_end.replace(tzinfo=timezone.utc)

        end = floating_end #+ timedelta(seconds=window_offset * 60 * 60)
        start = end - timedelta(seconds=window_size * 60 * 60)
        return start, end


class SlackWebhookOperator(BaseOperator):
    """
    This operator allows you to post messages to Slack using incoming webhooks.
    Takes both Slack webhook token directly and connection that has Slack webhook token.
    If both supplied, http_conn_id will be used as base_url,
    and webhook_token will be taken as endpoint, the relative path of the url.
    Each Slack webhook token can be pre-configured to use a specific channel, username and
    icon. You can override these defaults in this hook.
    :param http_conn_id: connection that has Slack webhook token in the extra field
    :type http_conn_id: str
    :param webhook_token: Slack webhook token
    :type webhook_token: str
    :param message: The message you want to send on Slack
    :type message: str
    :param blocks: The blocks to send on Slack. Should be a list of
        dictionaries representing Slack blocks.
    :type blocks: list
    """

    template_fields = ['webhook_token', 'message', 'blocks']

    @apply_defaults
    def __init__(self,
                 http_conn_id=None,
                 webhook_token=None,
                 message="",
                 blocks=None,
                 *args,
                 **kwargs):
        super(SlackWebhookOperator, self).__init__(*args, **kwargs)

        self.http_conn_id = http_conn_id
        self.webhook_token = self._get_token(webhook_token, http_conn_id)
        self.message = message
        self.blocks = blocks
        self.hook = None

    def _get_token(self, token, http_conn_id):
        """
        Given either a manually set token or a conn_id, return the webhook_token to use.
        :param token: The manually provided token
        :type token: str
        :param http_conn_id: The conn_id provided
        :type http_conn_id: str
        :return: webhook_token to use
        :rtype: str
        """
        if token:
            return token
        elif http_conn_id:
            conn = self.get_connection(http_conn_id)
            extra = conn.extra_dejson
            return extra.get('webhook_token', '')
        else:
            raise AirflowException('Cannot get token: No valid Slack connection')

    def _build_slack_message(self):
        """
        Construct the Slack message. All relevant parameters are combined here to a valid
        Slack json message.
        :return: Slack message to send
        :rtype: str
        """
        cmd={}
        if self.blocks:
            cmd['blocks']=self.blocks
        cmd['text']=self.message
        return json.dumps(cmd)

    def execute(self, context):
        slack_message=self._build_slack_message()
        self.log.info("sending alert to slack")
        self.log.info(slack_message)

        self.hook=HttpHook(http_conn_id=self.http_conn_id)
        response=self.hook.run(
            self.webhook_token,
            data=slack_message,
            headers={'Content-type': 'application/json'}
        )

        if response.status_code == 200:
            return response.text
        raise AirflowException("failed to send slack alert: {}".format(response.text))


def alert_failed_to_slack(context):
    SLACK_CONN_ID = "slack_alert"
    TASKFAIL_ALERT = int(Variable.get("taskfail_alert", default_var=1))

    def _xcom_value_has_error(_xcom) -> bool:
        return _xcom.key == XCOM_RETURN_KEY and isinstance(_xcom.value, dict) and 'error' in _xcom.value and _xcom.value['error'] != None

    if TASKFAIL_ALERT != 1:
        return "suppressed failure alert"

    slack_token = ""
    try:
        slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    except:
        print("no slack connection variable set")
        return "{connection} connection variable not defined, unable to send alerts".format(connection=SLACK_CONN_ID)
    
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
    failed_alert = SlackWebhookOperator(
        task_id='slack_failed_alert',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_token,
        blocks=blocks,
    )
    return failed_alert.execute(context=context)
