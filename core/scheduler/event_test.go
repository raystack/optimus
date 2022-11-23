package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/tenant"
)

func TestFromStringToEventType(t *testing.T) {
	t.Run("FromStringToEventType", func(t *testing.T) {
		expectationMap := map[string]JobEventType{
			"TYPE_JOB_START":   JobStartEvent,
			"TYPE_JOB_FAIL":    JobFailEvent,
			"TYPE_JOB_SUCCESS": JobSuccessEvent,

			"TYPE_SLA_MISS": SLAMissEvent,

			"TYPE_TASK_START":   TaskStartEvent,
			"TYPE_TASK_RETRY":   TaskRetryEvent,
			"TYPE_TASK_FAIL":    TaskFailEvent,
			"TYPE_TASK_SUCCESS": TaskSuccessEvent,

			"TYPE_SENSOR_START":   SensorStartEvent,
			"TYPE_SENSOR_RETRY":   SensorRetryEvent,
			"TYPE_SENSOR_FAIL":    SensorFailEvent,
			"TYPE_SENSOR_SUCCESS": SensorSuccessEvent,

			"TYPE_HOOK_START":   HookStartEvent,
			"TYPE_HOOK_RETRY":   HookRetryEvent,
			"TYPE_HOOK_FAIL":    HookFailEvent,
			"TYPE_HOOK_SUCCESS": HookSuccessEvent,

			"UNREGISTERED_EVENT": "",
		}

		for input, expectation := range expectationMap {
			output, err := FromStringToEventType(input)

			if input != "UNREGISTERED_EVENT" {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
			}

			assert.Equal(t, expectation, output)
		}
	})
	t.Run("EventFrom", func(t *testing.T) {
		eventValues := map[string]any{
			"someKey":      "someValue",
			"event_time":   16000631600.0,
			"task_id":      "some_txbq",
			"scheduled_at": "2022-01-02T15:04:05Z",
		}
		jobName := JobName("some_job")
		tnnt, err := tenant.NewTenant("someProject", "someNamespace")
		eventTypeName := "TYPE_TASK_RETRY"
		assert.Nil(t, err)

		outputObj := Event{
			JobName:        jobName,
			Tenant:         tnnt,
			Type:           TaskRetryEvent,
			EventTime:      time.Date(2477, time.January, 14, 17, 23, 20, 0, time.Local),
			OperatorName:   "some_txbq",
			JobScheduledAt: time.Date(2022, time.January, 2, 15, 04, 05, 0, time.UTC),
			Values:         eventValues,
		}

		output, err := EventFrom(eventTypeName, eventValues, jobName, tnnt)
		assert.Nil(t, err)
		assert.Equal(t, outputObj.JobScheduledAt, output.JobScheduledAt)
		assert.Equal(t, outputObj, output)
	})
	t.Run("IsOfType JobEventCategory", func(t *testing.T) {
		positiveExpectationMap := map[JobEventType]JobEventCategory{
			JobFailureEvent: EventCategoryJobFailure,
			JobFailEvent:    EventCategoryJobFailure,
			TaskFailEvent:   EventCategoryJobFailure,
			HookFailEvent:   EventCategoryJobFailure,
			SensorFailEvent: EventCategoryJobFailure,

			SLAMissEvent: EventCategorySLAMiss,
		}
		for eventType, category := range positiveExpectationMap {
			assert.True(t, eventType.IsOfType(category))
		}
		NegativeExpectationMap := map[JobEventType]JobEventCategory{
			SLAMissEvent:       EventCategoryJobFailure,
			SensorRetryEvent:   EventCategoryJobFailure,
			JobFailureEvent:    EventCategorySLAMiss,
			SensorSuccessEvent: EventCategorySLAMiss,
		}
		for eventType, category := range NegativeExpectationMap {
			assert.False(t, eventType.IsOfType(category))
		}
	})
}
