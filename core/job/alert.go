package job

type Alert struct {
	on       EventType
	channels []string
	config   map[string]string
}

func (a Alert) On() EventType {
	return a.on
}

func (a Alert) Channels() []string {
	return a.channels
}

func (a Alert) Config() map[string]string {
	return a.config
}

func NewAlert(on EventType, channels []string, config map[string]string) *Alert {
	return &Alert{on: on, channels: channels, config: config}
}

type EventType string

const (
	SLAMissEvent EventType = "sla_miss"

	JobFailureEvent EventType = "failure"
	JobStartEvent   EventType = "job_start"
	JobFailEvent    EventType = "job_fail"
	JobSuccessEvent EventType = "job_success"
	JobRetryEvent   EventType = "retry"

	TaskStartEvent   EventType = "task_start"
	TaskRetryEvent   EventType = "task_retry"
	TaskFailEvent    EventType = "task_fail"
	TaskSuccessEvent EventType = "task_success"

	HookStartEvent   EventType = "hook_start"
	HookRetryEvent   EventType = "hook_retry"
	HookFailEvent    EventType = "hook_fail"
	HookSuccessEvent EventType = "hook_success"

	SensorStartEvent   EventType = "sensor_start"
	SensorRetryEvent   EventType = "sensor_retry"
	SensorFailEvent    EventType = "sensor_fail"
	SensorSuccessEvent EventType = "sensor_success"
)
