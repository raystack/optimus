package service_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/raystack/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/raystack/optimus/core/scheduler"
	"github.com/raystack/optimus/core/scheduler/service"
	"github.com/raystack/optimus/core/tenant"
)

func TestNotificationService(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNoop()
	project, _ := tenant.NewProject("proj1", map[string]string{
		"STORAGE_PATH":   "somePath",
		"SCHEDULER_HOST": "localhost",
	})
	namespace, _ := tenant.NewNamespace("ns1", project.Name(), map[string]string{})
	tnnt, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	startDate, _ := time.Parse(time.RFC3339, "2022-03-20T02:00:00+00:00")
	jobName := scheduler.JobName("job1")
	t.Run("Push", func(t *testing.T) {
		t.Run("should give error if getJobDetails fails", func(t *testing.T) {
			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(nil, fmt.Errorf("some error"))
			defer jobRepo.AssertExpectations(t)

			notifyService := service.NewNotifyService(logger, jobRepo, nil, nil)

			event := &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.TaskStartEvent,
				Values:  map[string]any{},
			}
			err := notifyService.Push(ctx, event)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "some error")
		})
		t.Run("should send notification to the appropriate channel for sla miss notify", func(t *testing.T) {
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
					Owner:   "jobOwnerName",
				},
				Alerts: []scheduler.Alert{
					{
						On:       scheduler.EventCategorySLAMiss,
						Channels: []string{"slack://#chanel-name"},
						Config:   nil,
					},
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			event := &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.SLAMissEvent,
				Values:  map[string]any{},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			plainSecret, _ := tenant.NewPlainTextSecret("NOTIFY_SLACK", "secretValue")
			plainSecrets := []*tenant.PlainTextSecret{plainSecret}
			tenantService := new(mockTenantService)
			tenantService.On("GetSecrets", ctx, tnnt).Return(plainSecrets, nil)
			defer tenantService.AssertExpectations(t)

			notifyChanelSlack := new(mockNotificationChanel)
			notifyChanelSlack.On("Notify", ctx, scheduler.NotifyAttrs{
				Owner:    "jobOwnerName",
				JobEvent: event,
				Route:    "#chanel-name",
				Secret:   "secretValue",
			}).Return(nil)
			defer notifyChanelSlack.AssertExpectations(t)
			notifyChanelPager := new(mockNotificationChanel)
			defer notifyChanelPager.AssertExpectations(t)

			notifierChannels := map[string]service.Notifier{
				"slack":     notifyChanelSlack,
				"pagerduty": notifyChanelPager,
			}

			notifyService := service.NewNotifyService(logger, jobRepo, tenantService, notifierChannels)

			err := notifyService.Push(ctx, event)
			assert.Nil(t, err)
		})
		t.Run("should send notification to the appropriate channel for job fail", func(t *testing.T) {
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
					Owner:   "jobOwnerName",
				},
				Alerts: []scheduler.Alert{
					{
						On:       scheduler.EventCategoryJobFailure,
						Channels: []string{"pagerduty://#chanel-name"},
						Config:   nil,
					},
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			event := &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.JobFailureEvent,
				Values:  map[string]any{},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			plainSecret, _ := tenant.NewPlainTextSecret("notify_chanel-name", "secretValue")
			plainSecrets := []*tenant.PlainTextSecret{plainSecret}
			tenantService := new(mockTenantService)
			tenantService.On("GetSecrets", ctx, tnnt).Return(plainSecrets, nil)
			defer tenantService.AssertExpectations(t)

			notifChanelSlack := new(mockNotificationChanel)
			defer notifChanelSlack.AssertExpectations(t)
			notifyChanelPager := new(mockNotificationChanel)
			notifyChanelPager.On("Notify", ctx, scheduler.NotifyAttrs{
				Owner:    "jobOwnerName",
				JobEvent: event,
				Route:    "#chanel-name",
				Secret:   "secretValue",
			}).Return(nil)
			defer notifyChanelPager.AssertExpectations(t)

			notifierChannels := map[string]service.Notifier{
				"slack":     notifChanelSlack,
				"pagerduty": notifyChanelPager,
			}

			notifyService := service.NewNotifyService(logger, jobRepo, tenantService, notifierChannels)

			err := notifyService.Push(ctx, event)
			assert.Nil(t, err)
		})
		t.Run("should return error if notification to the appropriate channel for job_failure fails", func(t *testing.T) {
			job := scheduler.Job{
				Name:   jobName,
				Tenant: tnnt,
			}
			jobWithDetails := scheduler.JobWithDetails{
				Job: &job,
				JobMetadata: &scheduler.JobMetadata{
					Version: 1,
					Owner:   "jobOwnerName",
				},
				Alerts: []scheduler.Alert{
					{
						On:       scheduler.EventCategoryJobFailure,
						Channels: []string{"pagerduty://#chanel-name"},
						Config:   nil,
					},
				},
				Schedule: &scheduler.Schedule{
					StartDate: startDate.Add(-time.Hour * 24),
					EndDate:   nil,
					Interval:  "0 12 * * *",
				},
			}
			event := &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.JobFailureEvent,
				Values:  map[string]any{},
			}

			jobRepo := new(JobRepository)
			jobRepo.On("GetJobDetails", ctx, project.Name(), jobName).Return(&jobWithDetails, nil)
			defer jobRepo.AssertExpectations(t)

			plainSecret, _ := tenant.NewPlainTextSecret("notify_chanel-name", "secretValue")
			plainSecrets := []*tenant.PlainTextSecret{plainSecret}
			tenantService := new(mockTenantService)
			tenantService.On("GetSecrets", ctx, tnnt).Return(plainSecrets, nil)
			defer tenantService.AssertExpectations(t)

			notifyChanelSlack := new(mockNotificationChanel)
			defer notifyChanelSlack.AssertExpectations(t)
			notifyChanelPager := new(mockNotificationChanel)
			notifyChanelPager.On("Notify", ctx, scheduler.NotifyAttrs{
				Owner:    "jobOwnerName",
				JobEvent: event,
				Route:    "#chanel-name",
				Secret:   "secretValue",
			}).Return(fmt.Errorf("error in pagerduty push"))
			defer notifyChanelPager.AssertExpectations(t)

			notifierChannels := map[string]service.Notifier{
				"slack":     notifyChanelSlack,
				"pagerduty": notifyChanelPager,
			}

			notifyService := service.NewNotifyService(logger, jobRepo, tenantService, notifierChannels)

			err := notifyService.Push(ctx, event)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "ErrorsInNotifypush:\n notifyChannel.Notify: pagerduty://#chanel-name: error in pagerduty push")
		})
	})
}

type mockNotificationChanel struct {
	io.Closer
	mock.Mock
}

func (m *mockNotificationChanel) Notify(ctx context.Context, attr scheduler.NotifyAttrs) error {
	args := m.Called(ctx, attr)
	return args.Error(0)
}

func (m *mockNotificationChanel) Close() error {
	args := m.Called()
	return args.Error(0)
}
