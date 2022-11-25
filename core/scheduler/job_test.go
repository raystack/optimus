package scheduler_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
)

func TestJob(t *testing.T) {
	t.Run("JobRunIDFromString", func(t *testing.T) {
		newUUID := uuid.New()
		newUUIDString := newUUID.String()
		parsedUUID, err := scheduler.JobRunIDFromString(newUUIDString)
		assert.Nil(t, err)
		assert.Equal(t, scheduler.JobRunID(newUUID), parsedUUID)
	})
	t.Run("IsEmpty", func(t *testing.T) {
		newUUID := uuid.New()
		jobRunID := scheduler.JobRunID(newUUID)
		assert.False(t, jobRunID.IsEmpty())
		jobRunID = scheduler.JobRunID{}
		assert.True(t, jobRunID.IsEmpty())
	})
	t.Run("JobNameFrom", func(t *testing.T) {
		jobName, err := scheduler.JobNameFrom("someJob")
		assert.Nil(t, err)
		assert.Equal(t, scheduler.JobName("someJob"), jobName)

		jobName1, err := scheduler.JobNameFrom("")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity jobRun: job name is empty")
		assert.Equal(t, scheduler.JobName(""), jobName1)
	})
	t.Run("OperatorType to String", func(t *testing.T) {
		assert.Equal(t, "sensor", scheduler.OperatorSensor.String())
		assert.Equal(t, "hook", scheduler.OperatorHook.String())
		assert.Equal(t, "task", scheduler.OperatorTask.String())
	})
	t.Run("GetHook", func(t *testing.T) {
		t.Run(" get someHook1", func(t *testing.T) {
			dummyHook := scheduler.Hook{Name: "someHook1", Config: nil}
			dummyHook2 := scheduler.Hook{Name: "someHook2", Config: nil}
			job := scheduler.Job{
				Name:  "jobName",
				Hooks: []*scheduler.Hook{&dummyHook, &dummyHook2},
			}
			hook, err := job.GetHook("someHook1")
			assert.Nil(t, err)
			assert.Equal(t, &dummyHook, hook)
		})
		t.Run(" should return error when hook not found", func(t *testing.T) {
			dummyHook := scheduler.Hook{Name: "someHook1", Config: nil}
			dummyHook2 := scheduler.Hook{Name: "someHook2", Config: nil}
			job := scheduler.Job{
				Name:  "jobName",
				Hooks: []*scheduler.Hook{&dummyHook, &dummyHook2},
			}
			hook, err := job.GetHook("someHook13")

			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found for entity jobRun: hook not found in job someHook13")
			assert.Nil(t, hook)
		})
	})
	t.Run("GetName", func(t *testing.T) {
		jobWithDetails := scheduler.JobWithDetails{
			Name: "jobName",
		}
		assert.Equal(t, "jobName", jobWithDetails.GetName())
	})
	t.Run("SLADuration", func(t *testing.T) {
		t.Run("should return 0 and error if duration is incorrect format", func(t *testing.T) {
			jobWithDetails := scheduler.JobWithDetails{
				Name: "jobName",
				Alerts: []scheduler.Alert{
					{
						On:       scheduler.EventCategorySLAMiss,
						Channels: nil,
						Config: map[string]string{
							"duration": "2l",
						},
					},
				},
			}
			duration, err := jobWithDetails.SLADuration()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "failed to parse sla_miss duration 2l: time: unknown unit \"l\" in duration \"2l\"")
			assert.Equal(t, int64(0), duration)
		})
		t.Run("should return 0 if duration is not specified", func(t *testing.T) {
			jobWithDetails := scheduler.JobWithDetails{
				Name: "jobName",
				Alerts: []scheduler.Alert{
					{
						On:       scheduler.EventCategorySLAMiss,
						Channels: nil,
						Config:   map[string]string{},
					},
				},
			}
			duration, err := jobWithDetails.SLADuration()
			assert.Nil(t, err)
			assert.Equal(t, int64(0), duration)
		})
		t.Run("should get sla duration", func(t *testing.T) {
			jobWithDetails := scheduler.JobWithDetails{
				Name: "jobName",
				Alerts: []scheduler.Alert{
					{
						On:       scheduler.EventCategorySLAMiss,
						Channels: nil,
						Config: map[string]string{
							"duration": "2h",
						},
					},
				},
			}
			duration, err := jobWithDetails.SLADuration()
			assert.Nil(t, err)
			assert.Equal(t, int64(7200), duration)
		})
	})
	t.Run("GetLabelsAsString", func(t *testing.T) {
		jobWithDetails := scheduler.JobWithDetails{
			Name: "jobName",
			JobMetadata: &scheduler.JobMetadata{
				Labels: map[string]string{
					"label1": "someVale",
				},
			},
		}
		labels := jobWithDetails.GetLabelsAsString()
		assert.Equal(t, labels, "label1=someVale")
	})
	t.Run("GroupJobsByTenant", func(t *testing.T) {
		t1, _ := tenant.NewTenant("proj", "ns1")
		t2, _ := tenant.NewTenant("proj", "ns1")
		t3, _ := tenant.NewTenant("proj", "ns2")
		job1 := scheduler.JobWithDetails{
			Name: "job1",
			Job: &scheduler.Job{
				Tenant: t1,
			},
		}
		job2 := scheduler.JobWithDetails{
			Name: "job2",
			Job: &scheduler.Job{
				Tenant: t2,
			},
		}

		job3 := scheduler.JobWithDetails{
			Name: "job3",
			Job: &scheduler.Job{
				Tenant: t1,
			},
		}
		job4 := scheduler.JobWithDetails{
			Name: "job4",
			Job: &scheduler.Job{
				Tenant: t3,
			},
		}

		group := scheduler.GroupJobsByTenant(
			[]*scheduler.JobWithDetails{
				&job1, &job2, &job3, &job4,
			})

		assert.Equal(t, 2, len(group))
		assert.Equal(t, 3, len(group[t1]))
		assert.Equal(t, 1, len(group[t3]))
	})
}
