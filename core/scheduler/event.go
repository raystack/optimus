package scheduler

import (
	"strings"
	"time"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/utils"
)

type EventName string
type JobEventType string
type JobEventCategory string

const (
	EntityEvent = "event"

	ISODateFormat = "2006-01-02T15:04:05Z"

	EventCategorySLAMiss    JobEventCategory = "sla_miss"
	EventCategoryJobFailure JobEventCategory = "failure"

	SLAMissEvent    JobEventType = "sla_miss"
	JobFailureEvent JobEventType = "failure"
	// TODO: should they be event types anymore
	// TODO: test the notification flows end to end
	// JobRetryEvent JobEventType = "retry"
	// todo: check if this is being used

	JobStartEvent   JobEventType = "job_start"
	JobFailEvent    JobEventType = "job_fail"
	JobSuccessEvent JobEventType = "job_success"

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
)

func FromStringToEventType(name string) (JobEventType, error) {
	name = strings.TrimPrefix(strings.ToLower(name), strings.ToLower("TYPE_"))
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
	JobName        JobName
	Tenant         tenant.Tenant
	Type           JobEventType
	EventTime      time.Time
	OperatorName   string
	JobScheduledAt time.Time
	Values         map[string]any
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

	eventTimeFloat := utils.ConfigAs[float64](eventValues, "event_time")
	if eventTimeFloat == float64(0) {
		return Event{}, errors.InvalidArgument(EntityEvent, "property 'event_time'(number) is missing in event payload")
	}
	eventTime := time.Unix(int64(eventTimeFloat), 0)

	operatorName := utils.ConfigAs[string](eventValues, "task_id")
	if operatorName == "" {
		return Event{}, errors.InvalidArgument(EntityEvent, "property 'task_id'(string) is missing in event payload")
	}

	scheduledAtString := utils.ConfigAs[string](eventValues, "scheduled_at")
	if scheduledAtString == "" {
		return Event{}, errors.InvalidArgument(EntityEvent, "property 'scheduled_at'(string) is missing in event payload")
	}
	scheduledAtTimeStamp, err := time.Parse(ISODateFormat, scheduledAtString)
	if err != nil {
		return Event{}, errors.InvalidArgument(EntityEvent, "property 'scheduled_at' is not in appropriate format")
	}

	eventObj := Event{
		JobName:        jobName,
		Tenant:         tenent,
		Type:           eventType,
		OperatorName:   operatorName,
		EventTime:      eventTime,
		JobScheduledAt: scheduledAtTimeStamp,
		Values:         eventValues,
	}
	return eventObj, nil
}
