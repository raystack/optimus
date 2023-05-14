package bigquery_test

import (
	"context"
	"errors"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/ext/store/bigquery"
)

func TestTableCopier(t *testing.T) {
	ctx := context.Background()

	t.Run("Run", func(t *testing.T) {
		t.Run("return error when error in creating job", func(t *testing.T) {
			bqCopier := new(mockBQCopier)
			bqCopier.On("Run", ctx).Return(nil, errors.New("error in creating run"))
			defer bqCopier.AssertExpectations(t)

			copierHandle := bigquery.NewCopier(bqCopier)

			_, err := copierHandle.Run(ctx)
			assert.Error(t, err)
			assert.EqualError(t, err, "internal error for entity BigqueryStore: not able to create copy job")
		})
		t.Run("return copy job when successful", func(t *testing.T) {
			bqCopier := new(mockBQCopier)
			bqCopier.On("Run", ctx).Return(&bq.Job{}, nil)
			defer bqCopier.AssertExpectations(t)

			copierHandle := bigquery.NewCopier(bqCopier)

			copyJob, err := copierHandle.Run(ctx)
			assert.Nil(t, err)
			assert.NotNil(t, copyJob)
		})
	})
}

type mockBQCopier struct {
	mock.Mock
}

func (m *mockBQCopier) Run(ctx context.Context) (*bq.Job, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*bq.Job), args.Error(1)
}
