package setup

import (
	"context"

	"gocloud.dev/blob/memblob"

	"github.com/odpf/optimus/ext/scheduler/airflow2"
	"github.com/odpf/optimus/models"
)

type BucketFactory struct{}

func (bf *BucketFactory) New(context.Context, models.ProjectSpec) (airflow2.Bucket, error) {
	return memblob.OpenBucket(nil), nil
}
