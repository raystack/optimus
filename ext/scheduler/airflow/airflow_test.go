package airflow_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/ext/scheduler/airflow"
	mocked "github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/resources"
)

func TestAirflow(t *testing.T) {
	t.Run("Bootstrap", func(t *testing.T) {
		ctx := context.Background()
		t.Run("should successfully bootstrap for gcs buckets", func(t *testing.T) {
			var out bytes.Buffer
			wc := new(mocked.WriteCloser)
			defer wc.AssertExpectations(t)
			wc.On("Write").Return(&out, nil)
			wc.On("Close").Return(nil)

			ow := new(mocked.ObjectWriter)
			defer ow.AssertExpectations(t)

			bucket := "mybucket"
			objectPath := fmt.Sprintf("/hello/%s/%s", "dags", "__lib.py")
			ow.On("NewWriter", ctx, bucket, objectPath).Return(wc, nil)

			air := airflow.NewScheduler(resources.FileSystem, ow)
			err := air.Bootstrap(context.Background(), models.ProjectSpec{
				Name: "proj-name",
				Config: map[string]string{
					models.ProjectStoragePathKey: "gs://mybucket/hello",
				},
			})
			assert.Nil(t, err)
		})
		t.Run("should fail if no storage config is set", func(t *testing.T) {
			air := airflow.NewScheduler(nil, nil)
			err := air.Bootstrap(context.Background(), models.ProjectSpec{
				Name:   "proj-name",
				Config: map[string]string{},
			})
			assert.NotNil(t, err)
		})
		t.Run("should fail for unsupported storage interfaces", func(t *testing.T) {
			air := airflow.NewScheduler(nil, nil)
			err := air.Bootstrap(context.Background(), models.ProjectSpec{
				Name: "proj-name",
				Config: map[string]string{
					models.ProjectStoragePathKey: "xxx://mybucket/dags",
				},
			})
			assert.NotNil(t, err)
		})
	})
}
