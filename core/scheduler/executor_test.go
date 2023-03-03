package scheduler_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

func TestExecutor(t *testing.T) {
	t.Run("ExecutorType", func(t *testing.T) {
		t.Run("returns error when executor type is invalid", func(t *testing.T) {
			_, err := scheduler.ExecutorTypeFrom("invalid")
			assert.Error(t, err)
			assert.EqualError(t, err, "invalid argument for entity jobRun: failed to convert to executor type,"+
				" invalid value: invalid")
		})
		t.Run("returns error when executor type is invalid", func(t *testing.T) {
			validExecutorTypes := []string{"task", "hook"}

			for _, executorType := range validExecutorTypes {
				typ, err := scheduler.ExecutorTypeFrom(executorType)
				assert.NoError(t, err)
				assert.Equal(t, executorType, typ.String())
			}
		})
	})
	t.Run("Executor", func(t *testing.T) {
		t.Run("returns error when name is invalid", func(t *testing.T) {
			_, err := scheduler.ExecutorFrom("", scheduler.ExecutorTask)
			assert.Error(t, err)
			assert.EqualError(t, err, "invalid argument for entity jobRun: executor name is invalid")
		})
		t.Run("returns executor", func(t *testing.T) {
			executor, err := scheduler.ExecutorFrom("bigquery-runner", scheduler.ExecutorTask)
			assert.NoError(t, err)
			assert.Equal(t, scheduler.ExecutorTask, executor.Type)
			assert.Equal(t, "bigquery-runner", executor.Name)
		})
		t.Run("ExecutorFromEnum", func(t *testing.T) {
			t.Run("returns error when enum is empty", func(t *testing.T) {
				_, err := scheduler.ExecutorFromEnum("bq2bq", "")
				assert.Error(t, err)
				assert.EqualError(t, err, "invalid argument for entity jobRun: executor type is empty")
			})
			t.Run("returns error when enum is invalid", func(t *testing.T) {
				_, err := scheduler.ExecutorFromEnum("bq2bq", "task_type")
				assert.Error(t, err)
				assert.EqualError(t, err, "invalid argument for entity jobRun: failed to convert to executor "+
					"type, invalid value: task_type")
			})
			t.Run("returns the executor", func(t *testing.T) {
				exec, err := scheduler.ExecutorFromEnum("bq2bq", "type_task")
				assert.NoError(t, err)
				assert.Equal(t, scheduler.ExecutorTask, exec.Type)
				assert.Equal(t, "bq2bq", exec.Name)
			})
		})
	})
	t.Run("RunConfig", func(t *testing.T) {
		t.Run("returns error when run id is invalid", func(t *testing.T) {
			executor := scheduler.Executor{Name: "bq2bq", Type: scheduler.ExecutorTask}

			_, err := scheduler.RunConfigFrom(executor, time.Now(), "c06049ad-cb2e-43a5-9daf-c3c53d18")
			assert.Error(t, err)
			assert.EqualError(t, err, "invalid argument for entity jobRun: invalid job run ID"+
				" c06049ad-cb2e-43a5-9daf-c3c53d18")
		})
		t.Run("returns run config with empty runID", func(t *testing.T) {
			executor := scheduler.Executor{Name: "bq2bq", Type: scheduler.ExecutorTask}
			now := time.Now()

			runConfig, err := scheduler.RunConfigFrom(executor, now, "")
			assert.NoError(t, err)
			assert.Equal(t, uuid.Nil, runConfig.JobRunID.UUID())
			assert.Equal(t, "bq2bq", runConfig.Executor.Name)
			assert.Equal(t, now, runConfig.ScheduledAt)
		})
	})
}
