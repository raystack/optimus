//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	postgres "github.com/odpf/optimus/internal/store/postgres/scheduler"
)

func TestPostgresJobOperatorRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("test-proj", "test-ns")
	currentTime := time.Now().UTC()
	scheduledAt := currentTime.Add(-time.Hour)
	operatorStartTime := currentTime
	operatorEndTime := currentTime.Add(time.Hour)
	slaDefinitionInSec := int64(3600) //seconds

	t.Run("CreateOperatorRun", func(t *testing.T) {
		t.Run("creates a operator run", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, slaDefinitionInSec)
			assert.Nil(t, err)

			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			operatorRunRepo := postgres.NewOperatorRunRepository(db)
			err = operatorRunRepo.CreateOperatorRun(ctx, "some-operator-name", scheduler.OperatorSensor, jobRun.ID, operatorStartTime)
			assert.Nil(t, err)

			operatorRun, err := operatorRunRepo.GetOperatorRun(ctx, "some-operator-name", scheduler.OperatorSensor, jobRun.ID)
			assert.Nil(t, err)
			assert.Equal(t, operatorStartTime.UTC().Format(time.RFC1123), operatorRun.StartTime.UTC().Format(time.RFC1123))
		})
	})
	t.Run("GetOperatorRun", func(t *testing.T) {
		t.Run("should return not found error", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, slaDefinitionInSec)
			assert.Nil(t, err)

			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			operatorRunRepo := postgres.NewOperatorRunRepository(db)
			operatorRun, err := operatorRunRepo.GetOperatorRun(ctx, "some-operator-name", scheduler.OperatorHook, jobRun.ID)
			assert.True(t, errors.IsErrorType(err, errors.ErrNotFound))
			assert.Nil(t, operatorRun)
		})
		t.Run("should return InvalidArgument error when wrong operator name", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, slaDefinitionInSec)
			assert.Nil(t, err)

			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			operatorRunRepo := postgres.NewOperatorRunRepository(db)
			operatorRun, err := operatorRunRepo.GetOperatorRun(ctx, "some-operator-name", "some-other-operator", jobRun.ID)
			assert.True(t, errors.IsErrorType(err, errors.ErrInvalidArgument))
			assert.Nil(t, operatorRun)
		})
	})

	t.Run("UpdateOperatorRun", func(t *testing.T) {
		t.Run("updates a specific operator run by job id", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, slaDefinitionInSec)
			assert.Nil(t, err)

			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			operatorRunRepo := postgres.NewOperatorRunRepository(db)
			err = operatorRunRepo.CreateOperatorRun(ctx, "some-operator-name", scheduler.OperatorTask, jobRun.ID, operatorStartTime)
			assert.Nil(t, err)

			operatorRun, err := operatorRunRepo.GetOperatorRun(ctx, "some-operator-name", scheduler.OperatorTask, jobRun.ID)
			assert.Nil(t, err)
			assert.Equal(t, operatorStartTime.UTC().Format(time.RFC1123), operatorRun.StartTime.UTC().Format(time.RFC1123))

			err = operatorRunRepo.UpdateOperatorRun(ctx, scheduler.OperatorTask, operatorRun.ID, operatorEndTime, scheduler.StateFailed)
			assert.Nil(t, err)

			operatorRun, err = operatorRunRepo.GetOperatorRun(ctx, "some-operator-name", scheduler.OperatorTask, jobRun.ID)
			assert.Nil(t, err)
			assert.Equal(t, operatorEndTime.UTC().Format(time.RFC1123), operatorRun.EndTime.UTC().Format(time.RFC1123))
			assert.Equal(t, scheduler.StateFailed, operatorRun.Status)
		})
	})
}
