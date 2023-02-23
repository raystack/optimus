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

func TestPostgresSchedulerRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("test-proj", "test-ns")
	endTime := time.Now()
	startTime := endTime.Add(-48 * time.Hour)
	description := "sample backfill"

	t.Run("RegisterReplay", func(t *testing.T) {
		t.Run("store replay request and the runs", func(t *testing.T) {
			db := dbSetup()
			replayRepo := postgres.NewReplayRepository(db)

			replayConfig := scheduler.NewReplayConfig(startTime, endTime, true, description)
			jobRuns := []*scheduler.JobRunStatus{
				{
					ScheduledAt: startTime,
					State:       scheduler.StatePending,
				},
				{
					ScheduledAt: startTime.Add(24 * time.Hour),
					State:       scheduler.StatePending,
				},
			}
			replayReq := scheduler.NewReplay(jobAName, tnnt, replayConfig, jobRuns, scheduler.ReplayStateCreated)

			replayID, err := replayRepo.RegisterReplay(ctx, replayReq)
			assert.Nil(t, err)
			assert.NotNil(t, replayID)
		})
	})

	t.Run("GetReplaysToExecute", func(t *testing.T) {

	})
}
