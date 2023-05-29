package bigquery_test

import (
	"context"
	"errors"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/ext/store/bigquery"
)

func TestJob(t *testing.T) {
	ctx := context.Background()

	t.Run("JobHandle", func(t *testing.T) {
		t.Run("return error when error in wait", func(t *testing.T) {
			bqJob := new(mockBQJob)
			bqJob.On("Wait", ctx).Return(nil, errors.New("error in wait"))

			copyJob := bigquery.NewJob(bqJob)

			err := copyJob.Wait(ctx)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "internal error for entity BigqueryStore: error while wait for bq job")
		})
		t.Run("return no error when successful", func(t *testing.T) {
			bqJob := new(mockBQJob)
			bqJob.On("Wait", ctx).Return(&bq.JobStatus{}, nil)
			defer bqJob.AssertExpectations(t)

			copyJob := bigquery.NewJob(bqJob)

			err := copyJob.Wait(ctx)
			assert.Nil(t, err)
		})
	})
}

type mockBQJob struct {
	mock.Mock
}

func (m *mockBQJob) Wait(ctx context.Context) (*bq.JobStatus, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*bq.JobStatus), args.Error(1)
}
