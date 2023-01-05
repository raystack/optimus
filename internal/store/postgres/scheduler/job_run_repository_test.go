//go:build !unit_test

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	postgres "github.com/odpf/optimus/internal/store/postgres/scheduler"
)

func TestPostgresJobRunRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("test-proj", "test-ns")
	currentTime := time.Now().UTC()
	scheduledAt := currentTime.Add(-time.Hour)
	slaDefinitionInSec := int64(3600) //seconds

	t.Run("Create", func(t *testing.T) {
		t.Run("creates a job run", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)
			assert.Equal(t, jobAName, jobRun.JobName.String())
		})
	})
	t.Run("GetByID", func(t *testing.T) {
		t.Run("gets a specific job run by ID", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			jobRunByID, err := jobRunRepo.GetByID(ctx, scheduler.JobRunID(jobRun.ID))
			assert.Nil(t, err)
			assert.EqualValues(t, jobRunByID, jobRun)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("updates a specific job run by id", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			jobEndTime := currentTime.Add(-time.Minute)
			err = jobRunRepo.Update(ctx, jobRun.ID, jobEndTime, scheduler.StateSuccess)
			assert.Nil(t, err)

			jobRunByID, err := jobRunRepo.GetByID(ctx, scheduler.JobRunID(jobRun.ID))
			assert.Nil(t, err)
			assert.EqualValues(t, scheduler.StateSuccess, jobRunByID.State)
			assert.Equal(t, jobEndTime.UTC().Format(time.RFC1123), jobRunByID.EndTime.UTC().Format(time.RFC1123))
		})
	})
	t.Run("UpdateSLA", func(t *testing.T) {
		t.Run("updates jobs sla alert firing status", func(t *testing.T) {
			db := dbSetup()
			_ = addJobs(ctx, t, db)
			jobRunRepo := postgres.NewJobRunRepository(db)
			err := jobRunRepo.Create(ctx, tnnt, jobAName, scheduledAt, slaDefinitionInSec)
			assert.Nil(t, err)
			jobRun, err := jobRunRepo.GetByScheduledAt(ctx, tnnt, jobAName, scheduledAt)
			assert.Nil(t, err)

			var slaObject = scheduler.SLAObject{
				JobName:        jobAName,
				JobScheduledAt: scheduledAt,
			}
			var slaObjects = []*scheduler.SLAObject{&slaObject}

			err = jobRunRepo.UpdateSLA(ctx, slaObjects)
			assert.Nil(t, err)

			jobRunByID, err := jobRunRepo.GetByID(ctx, scheduler.JobRunID(jobRun.ID))
			assert.Nil(t, err)
			assert.True(t, jobRunByID.SLAAlert)
		})
	})
}
