package job_run

import (
	"github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type EventName string
type JobEventType string
type JobEventCategory string

type Event struct {
	JobName JobName
	Type    JobEventType
	Values  map[string]any
}

const (
	EntityEvent = "event"

	SLAMiss    JobEventCategory = "sla_miss"
	JobFailure JobEventCategory = "failure"

	JobStartEvent   JobEventType = "job_start"
	JobFailEvent    JobEventType = "job_fail"
	JobSuccessEvent JobEventType = "job_success"
	SLAMissEvent    JobEventType = "sla_miss"

	TaskStartEvent   JobEventType = "task_start"
	TaskRetryEvent   JobEventType = "task_retry"
	TaskFailEvent    JobEventType = "task_fail"
	TaskSuccessEvent JobEventType = "task_success"

	HookStartEvent   JobEventType = "hook_start"
	HookRetryEvent   JobEventType = "hook_retry"
	HookFailEvent    JobEventType = "hook_fail"
	HookSuccessEvent JobEventType = "hook_success"

	SensorStartEvent   JobEventType = "sensor_start"
	SensorRetryEvent   JobEventType = "sensor_retry"
	SensorFailEvent    JobEventType = "sensor_fail"
	SensorSuccessEvent JobEventType = "sensor_success"

	JobRetryEvent JobEventType = "retry"
)

func EventFrom(event *pb.JobEvent, jobName JobName) (Event, error) {
	if event == nil {
		return Event{}, errors.InvalidArgument(EntityEvent, "bad event")
	}
	var eventValues map[string]any
	for k, v := range event.Value.GetFields() {
		eventValues[k] = v
	}
	eventObj := Event{
		JobName: jobName,
		Type:    JobEventType(event.Type.String()),
		Values:  eventValues,
	}
	return eventObj, nil
}
