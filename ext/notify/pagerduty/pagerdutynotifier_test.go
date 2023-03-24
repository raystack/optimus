package pagerduty_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/notify/pagerduty"
)

type PagerDutyServiceImplMock struct {
	mock.Mock
}

func (s *PagerDutyServiceImplMock) SendAlert(ctx context.Context, evt pagerduty.Event) error {
	err := s.Called(ctx, evt).Error(0)
	return err
}

func TestPagerDuty(t *testing.T) {
	t.Run("should send alert to pagerduty service using service name successfully", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var sendErrors []error
		tnnt, _ := tenant.NewTenant("foo", "test")
		pagerDutyServiceMock := new(PagerDutyServiceImplMock)
		pgEvent := pagerduty.NewEvent("test-token", "",
			&scheduler.Event{
				JobName: "foo-job-spec",
				Tenant:  tnnt,
				Type:    scheduler.JobFailureEvent,
				Values:  map[string]any(nil),
			})

		pagerDutyServiceMock.On("SendAlert", ctx, pgEvent).Return(nil)
		defer pagerDutyServiceMock.AssertExpectations(t)

		client := pagerduty.NewNotifier(
			ctx,
			time.Millisecond*500,
			func(err error) {
				sendErrors = append(sendErrors, err)
			},
			pagerDutyServiceMock,
		)
		defer client.Close()

		err := client.Notify(context.Background(), scheduler.NotifyAttrs{
			Owner: "",
			JobEvent: &scheduler.Event{
				JobName: "foo-job-spec",
				Tenant:  tnnt,
				Type:    scheduler.JobFailureEvent,
				Values:  nil,
			},
			Route:  "optimus@test.com",
			Secret: "test-token",
		})

		assert.Nil(t, err)
		cancel()
		assert.Nil(t, client.Close())
		assert.Nil(t, sendErrors)
	})

	t.Run("should call error handler function for any error from pagerduty service ", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var sendErrors []error
		tnnt, _ := tenant.NewTenant("foo", "test")
		jobName := scheduler.JobName("foo-job-spec")
		pagerDutyServiceMock := new(PagerDutyServiceImplMock)
		pgEvent := pagerduty.NewEvent("test-invalid-token", "",
			&scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.JobFailureEvent,
				Values:  map[string]any(nil),
			})
		pagerDutyServiceMock.On("SendAlert", ctx, pgEvent).
			Return(fmt.Errorf("invalid routing key test-invalid-token"))
		defer pagerDutyServiceMock.AssertExpectations(t)

		client := pagerduty.NewNotifier(
			ctx,
			time.Millisecond*500,
			func(err error) {
				sendErrors = append(sendErrors, err)
			},
			pagerDutyServiceMock,
		)
		defer client.Close()

		client.Notify(context.Background(), scheduler.NotifyAttrs{
			Owner: "",
			JobEvent: &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.JobFailureEvent,
				Values:  map[string]any(nil),
			},

			Route:  "optimus@test.com",
			Secret: "test-invalid-token",
		})

		cancel()
		assert.Nil(t, client.Close())
		assert.NotNil(t, sendErrors)
	})
}
