package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/scheduler/service"
	"github.com/odpf/optimus/core/tenant"
)

func TestNotificationService(t *testing.T) {
	ctx := context.Background()
	project, _ := tenant.NewProject("proj1", map[string]string{
		"STORAGE_PATH":   "somePath",
		"SCHEDULER_HOST": "localhost",
	})
	namespace, _ := tenant.NewNamespace("ns1", project.Name(), map[string]string{})
	tnnt, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	jobName := scheduler.JobName("job1")
	t.Run("Push", func(t *testing.T) {
		t.Run("should give error if getJobDetails fails", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(nil, fmt.Errorf("some error"))
			defer jobRepo.AssertExpectations(t)

			notifyService := service.NewNotifyService(nil,
				jobRepo, nil, nil)

			event := scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.JobStartEvent,
				Values:  map[string]any{},
			}
			err := notifyService.Push(ctx, event)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "some error")
		})
	})
}
