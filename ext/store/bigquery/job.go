package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"

	"github.com/goto/optimus/internal/errors"
)

type BQJob interface {
	Wait(context.Context) (*bigquery.JobStatus, error)
}

type JobHandle struct {
	bqJob BQJob
}

func NewJob(job BQJob) *JobHandle {
	return &JobHandle{bqJob: job}
}

func (j JobHandle) Wait(ctx context.Context) error {
	status, err := j.bqJob.Wait(ctx)
	if err != nil {
		return errors.InternalError(store, "error while wait for bq job", err)
	}

	if err = status.Err(); err != nil {
		return errors.InternalError(store, "error in job status", err)
	}
	return nil
}
