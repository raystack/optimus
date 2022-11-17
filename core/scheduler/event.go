package scheduler

import (
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type EventName string
type JobEventType string
type JobEventCategory string

const (
	EntityEvent = "event"

	ISODateFormat = "2006-01-02T15:04:05Z"

	EventCategorySLAMiss    JobEventCategory = "sla_miss"
	EventCategoryJobFailure JobEventCategory = "failure"

	JobStartEvent   JobEventType = "job_start"
	JobFailEvent    JobEventType = "job_fail"
	JobSuccessEvent JobEventType = "job_success"
	SLAMissEvent    JobEventType = "sla_miss"
	JobFailureEvent JobEventType = "failure"

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

	// JobRetryEvent JobEventType = "retry"
	// todo: check if this is being used

	OperatorNameKey = "task_id"
)

func FromStringToEventType(name string) (JobEventType, error) {
	switch name {
	case string(JobStartEvent):
		return JobStartEvent, nil
	case string(JobFailEvent):
		return JobFailEvent, nil
	case string(JobSuccessEvent):
		return JobSuccessEvent, nil
	case string(SLAMissEvent):
		return SLAMissEvent, nil
	case string(TaskStartEvent):
		return TaskStartEvent, nil
	case string(TaskRetryEvent):
		return TaskRetryEvent, nil
	case string(TaskFailEvent):
		return TaskFailEvent, nil
	case string(TaskSuccessEvent):
		return TaskSuccessEvent, nil
	case string(HookStartEvent):
		return HookStartEvent, nil
	case string(HookRetryEvent):
		return HookRetryEvent, nil
	case string(HookFailEvent):
		return HookFailEvent, nil
	case string(HookSuccessEvent):
		return HookSuccessEvent, nil
	case string(SensorStartEvent):
		return SensorStartEvent, nil
	case string(SensorRetryEvent):
		return SensorRetryEvent, nil
	case string(SensorFailEvent):
		return SensorFailEvent, nil
	case string(SensorSuccessEvent):
		return SensorSuccessEvent, nil
	default:
		return "", errors.InvalidArgument(EntityEvent, "unknown event "+name)
	}
}

type Event struct {
	JobName JobName
	Tenant  tenant.Tenant
	Type    JobEventType
	Values  map[string]any
}

func (incomingEvent JobEventType) IsOfType(category JobEventCategory) bool {
	var failureEvents = []JobEventType{JobFailureEvent, JobFailEvent, TaskFailEvent, HookFailEvent, SensorFailEvent}

	switch category {
	case EventCategoryJobFailure:
		for _, event := range failureEvents {
			if incomingEvent == event {
				return true
			}
		}
	case EventCategorySLAMiss:
		if incomingEvent == SLAMissEvent {
			return true
		}
	}
	return false
}

func EventFrom(eventTypeName string, eventValues map[string]any, jobName JobName, tenent tenant.Tenant) (Event, error) {
	eventType, err := FromStringToEventType(eventTypeName)
	if err != nil {
		return Event{}, err
	}
	eventObj := Event{
		JobName: jobName,
		Type:    eventType,
		Values:  eventValues,
		Tenant:  tenent,
	}
	return eventObj, nil
}
