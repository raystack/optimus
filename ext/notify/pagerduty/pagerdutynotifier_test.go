package pagerduty

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/odpf/optimus/models"
)

type PagerDutyServiceImplMock struct {
	mock.Mock
}

func (s *PagerDutyServiceImplMock) SendAlert(ctx context.Context, evt Event) error {
	err := s.Called(ctx, evt).Error(0)
	return err
}

func TestPagerDuty(t *testing.T) {
	t.Run("should send alert to pagerduty service using service name successfully", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var sendErrors []error

		pagerDutyServiceMock := new(PagerDutyServiceImplMock)
		pagerDutyServiceMock.On("SendAlert", ctx,
			Event{
				routingKey:    "test-token",
				projectName:   "foo",
				namespaceName: "test",
				jobName:       "foo-job-spec",
				owner:         "",
				meta: models.JobEvent{
					Type:  "failure",
					Value: map[string]*structpb.Value(nil),
				},
			},
		).Return(nil)
		defer pagerDutyServiceMock.AssertExpectations(t)

		client := NewNotifier(
			ctx,
			time.Millisecond*500,
			func(err error) {
				sendErrors = append(sendErrors, err)
			},
			pagerDutyServiceMock,
		)
		defer client.Close()

		err := client.Notify(context.Background(), models.NotifyAttrs{
			Namespace: models.NamespaceSpec{
				Name: "test",
				ProjectSpec: models.ProjectSpec{
					Name: "foo",
					Secret: []models.ProjectSecretItem{
						{
							Name:  "optimus@test.com",
							Value: "test-token",
						},
					},
				},
			},
			JobSpec: models.JobSpec{
				Name: "foo-job-spec",
			},
			JobEvent: models.JobEvent{
				Type: models.JobFailureEvent,
			},
			Route: "optimus@test.com",
		})

		assert.Nil(t, err)
		cancel()
		assert.Nil(t, client.Close())
		assert.Nil(t, sendErrors)
	})

	t.Run("should call error handler function for any error from pagerduty service ", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var sendErrors []error

		pagerDutyServiceMock := new(PagerDutyServiceImplMock)
		pagerDutyServiceMock.On("SendAlert", ctx,
			Event{
				routingKey:    "test-invalid-token",
				projectName:   "foo",
				namespaceName: "test",
				jobName:       "foo-job-spec",
				owner:         "",
				meta: models.JobEvent{
					Type:  "failure",
					Value: map[string]*structpb.Value(nil),
				},
			},
		).Return(fmt.Errorf("invalid routing key test-invalid-token"))
		defer pagerDutyServiceMock.AssertExpectations(t)

		client := NewNotifier(
			ctx,
			time.Millisecond*500,
			func(err error) {
				sendErrors = append(sendErrors, err)
			},
			pagerDutyServiceMock,
		)
		defer client.Close()

		client.Notify(context.Background(), models.NotifyAttrs{
			Namespace: models.NamespaceSpec{
				Name: "test",
				ProjectSpec: models.ProjectSpec{
					Name: "foo",
					Secret: []models.ProjectSecretItem{
						{
							Name:  "optimus@test.com",
							Value: "test-invalid-token",
						},
					},
				},
			},
			JobSpec: models.JobSpec{
				Name: "foo-job-spec",
			},
			JobEvent: models.JobEvent{
				Type: models.JobFailureEvent,
			},
			Route: "optimus@test.com",
		})

		cancel()
		assert.Nil(t, client.Close())
		assert.NotNil(t, sendErrors)
	})
}
