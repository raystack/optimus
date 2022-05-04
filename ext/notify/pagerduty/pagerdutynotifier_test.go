package pagerduty

import (
	"context"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/structpb"
	"testing"
	"time"
)

type PagerDutyServiceImplMock struct {
	mock.Mock
}

func (s *PagerDutyServiceImplMock) SendPagerDutyAlert(ctx context.Context, evt Event) error {
	s.Called(ctx, evt)
	return nil
}

func TestPagerDuty(t *testing.T) {
	t.Run("should send alert to pagerduty service using service name successfully", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		var sendErrors []error

		pagerDutyServiceMock := new(PagerDutyServiceImplMock)
		pagerDutyServiceMock.On("SendPagerDutyAlert", ctx,
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
				Type: models.JobEventTypeFailure,
			},
			Route: "optimus@test.com",
		})

		assert.Nil(t, err)
		cancel()
	})
}
