package v1beta1_test

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/odpf/salt/log"
	tMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/api/handler/v1beta1"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/datastore"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

type dummyEvent struct{}

func (dummyEvent) String() string {
	return "dummy message"
}

type ResourceObserverTestSuite struct {
	suite.Suite
}

func (r *ResourceObserverTestSuite) TestNotify() {
	spec := models.ResourceSpec{
		Name: "resource",
	}
	err := errors.New("random error")
	events := []progress.Event{
		&datastore.EventResourceCreated{
			Spec: spec,
		},
		&datastore.EventResourceCreated{
			Spec: spec,
			Err:  err,
		},
	}

	r.Run("should send response through stream if event is recognized", func() {
		stream := &mock.DeployResourceSpecificationServer{}
		logger := &log.Logrus{}
		mu := &sync.Mutex{}
		observer := v1beta1.NewResourceObserver(stream, logger, mu)

		stream.On("Send", tMock.Anything).Return(nil)
		defer stream.AssertExpectations(r.T())

		for _, e := range events {
			observer.Notify(e)
		}
	})

	r.Run("should write log when error sending response", func() {
		stream := &mock.DeployResourceSpecificationServer{}
		mu := &sync.Mutex{}

		stream.On("Send", tMock.Anything).Return(errors.New("random error"))
		defer stream.AssertExpectations(r.T())

		for _, e := range events {
			buff := &bytes.Buffer{}
			logger := log.NewLogrus(log.LogrusWithWriter(buff))
			observer := v1beta1.NewResourceObserver(stream, logger, mu)

			observer.Notify(e)

			actualMessage := buff.String()

			r.NotEmpty(actualMessage)
		}
	})

	r.Run("should not send response if event is not within the recognized", func() {
		stream := &mock.DeployResourceSpecificationServer{}
		logger := &log.Logrus{}
		mu := &sync.Mutex{}

		observer := v1beta1.NewResourceObserver(stream, logger, mu)

		event := &dummyEvent{}
		observer.Notify(event)
	})
}

func TestResourceObserver(t *testing.T) {
	suite.Run(t, &ResourceObserverTestSuite{})
}
