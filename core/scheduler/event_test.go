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
			"TYPE_SLA_MISS":    SLAMissEvent,
			"TYPE_FAILURE":     JobFailureEvent,
			"TYPE_JOB_SUCCESS": JobSuccessEvent,

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
		t.Run("Should return error if scheduled_at is incorrect format", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":      "someValue",
				"event_time":   16000631600.0,
				"task_id":      "some_txbq",
				"status":       "running",
				"scheduled_at": "2022--01-02T15:04:05Z",
			}
			jobName := JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_TASK_RETRY"
			eventObj, err := EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: property 'scheduled_at' is not in appropriate format")
			assert.Equal(t, eventObj, Event{})
		})
		t.Run("Should return error if scheduled_at is not provided in event payload", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":    "someValue",
				"event_time": 16000631600.0,
				"task_id":    "some_txbq",
				"status":     "running",
			}
			jobName := JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_TASK_RETRY"
			eventObj, err := EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: property 'scheduled_at'(string) is missing in event payload")
			assert.Equal(t, eventObj, Event{})
		})
		t.Run("Should return error if task_id is not provided in event payload", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":      "someValue",
				"event_time":   16000631600.0,
				"status":       "running",
				"scheduled_at": "2022-01-02T15:04:05Z",
			}
			jobName := JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_TASK_RETRY"
			eventObj, err := EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: property 'task_id'(string) is missing in event payload")
			assert.Equal(t, eventObj, Event{})
		})
		t.Run("Should return error if event is notvalid number", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":      "someValue",
				"event_time":   "16000631600.0",
				"task_id":      "some_txbq",
				"status":       "running",
				"scheduled_at": "2022-01-02T15:04:05Z",
			}
			jobName := JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_TASK_RETRY"
			eventObj, err := EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: property 'event_time'(number) is missing in event payload")
			assert.Equal(t, eventObj, Event{})
		})
		t.Run("Should parse events of type slaMiss", func(t *testing.T) {
			sla := []map[string]string{
				{
					"dag_id":       "sample_select",
					"scheduled_at": "2006-01-02T15:04:05Z",
				},
				{
					"dag_id":       "sample_select1",
					"scheduled_at": "2006-01-02T15:04:05Z",
				},
			}
			eventValues := map[string]any{
				"event_time": "16000631600.0",
				"slas":       sla,
			}
			jobName := JobName("")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_SLA_MISS"
			eventObj, err := EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.Nil(t, err)

			scheduledAt, _ := time.Parse(ISODateFormat, "2006-01-02T15:04:05Z")

			assert.Equal(t, eventObj, Event{
				Tenant: tnnt,
				Type:   SLAMissEvent,
				Values: eventValues,
				SLAObjectList: []*SLAObject{
					{
						JobName:        "sample_select",
						JobScheduledAt: scheduledAt,
					},
					{
						JobName:        "sample_select1",
						JobScheduledAt: scheduledAt,
					},
				},
			})
		})
		t.Run("Should return error if event is unregistered type", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":      "someValue",
				"event_time":   16000631600.0,
				"task_id":      "some_txbq",
				"status":       "running",
				"scheduled_at": "2022-01-02T15:04:05Z",
			}
			jobName := JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_TASK_RETRY_UNREGISTERED"
			eventObj, err := EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: unknown event task_retry_unregistered")
			assert.Equal(t, eventObj, Event{})
		})
		t.Run("Should Successfully parse an event", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":      "someValue",
				"event_time":   16000631600.0,
				"task_id":      "some_txbq",
				"status":       "running",
				"scheduled_at": "2022-01-02T15:04:05Z",
			}
			jobName := JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			eventTypeName := "TYPE_TASK_RETRY"
			assert.Nil(t, err)

			var outputObj = Event{
				JobName:        jobName,
				Tenant:         tnnt,
				Type:           TaskRetryEvent,
				EventTime:      time.Date(2477, time.January, 14, 11, 53, 20, 0, time.UTC),
				OperatorName:   "some_txbq",
				Status:         StateRunning,
				JobScheduledAt: time.Date(2022, time.January, 2, 15, 04, 05, 0, time.UTC),
				Values:         eventValues,
			}
			output, err := EventFrom(eventTypeName, eventValues, jobName, tnnt)
			assert.Nil(t, err)
			assert.Equal(t, outputObj.JobScheduledAt, output.JobScheduledAt)
			assert.Equal(t, outputObj, output)
		})
	})
	t.Run("IsOfType JobEventCategory", func(t *testing.T) {
		positiveExpectationMap := map[JobEventType]JobEventCategory{
			JobFailureEvent: EventCategoryJobFailure,
			SLAMissEvent:    EventCategorySLAMiss,
		}
		for eventType, category := range positiveExpectationMap {
			assert.True(t, eventType.IsOfType(category))
		}
		NegativeExpectationMap := map[JobEventType]JobEventCategory{
			SLAMissEvent:       EventCategoryJobFailure,
			SensorRetryEvent:   EventCategoryJobFailure,
			SensorSuccessEvent: EventCategorySLAMiss,
		}
		for eventType, category := range NegativeExpectationMap {
			assert.False(t, eventType.IsOfType(category))
		}
	})
}
