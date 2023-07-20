package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"

	"github.com/raystack/optimus/internal/errors"
)

type BQCopier interface {
	Run(context.Context) (*bigquery.Job, error)
}

type CopyJob interface {
	Wait(ctx context.Context) error
}

type Copier struct {
	bqCopier BQCopier
}

func NewCopier(bqCopier BQCopier) *Copier {
	return &Copier{bqCopier: bqCopier}
}

func (c Copier) Run(ctx context.Context) (CopyJob, error) {
	job, err := c.bqCopier.Run(ctx)
	if err != nil {
		return nil, errors.InternalError(store, "not able to create copy job", err)
	}

	return NewJob(job), nil
}
