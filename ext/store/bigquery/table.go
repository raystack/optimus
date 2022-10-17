package bigquery

import (
	"context"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
)

type BqTable interface {
	Create(context.Context, *bigquery.TableMetadata) error
	Update(context.Context, bigquery.TableMetadataToUpdate, string) (*bigquery.TableMetadata, error)
}

type TableHandle struct {
	bqTable BqTable
}

func (t TableHandle) Create(ctx context.Context, res *resource.Resource) error {
	table, err := resource.ConvertSpecTo[resource.Table](res)
	if err != nil {
		return err
	}

	meta, err := toBQTableMetadata(table, res)
	if err != nil {
		return errors.AddErrContext(err, resource.EntityTable, "failed to get metadata to create for "+res.FullName())
	}

	err = t.bqTable.Create(ctx, meta)
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) &&
			metaErr.Code == 409 && strings.Contains(metaErr.Message, "Already Exists") {
			return errors.AlreadyExists(resource.EntityTable, "table already exists on bigquery: "+res.FullName())
		}
		return errors.InternalError(resource.EntityTable, "failed to create resource "+res.FullName(), err)
	}
	return nil
}

func (t TableHandle) Update(ctx context.Context, res *resource.Resource) error {
	table, err := resource.ConvertSpecTo[resource.Table](res)
	if err != nil {
		return err
	}

	metadataToUpdate, err := getMetadataToUpdate(table.Description, table.ExtraConfig, res.Metadata().Labels)
	if err != nil {
		return errors.AddErrContext(err, resource.EntityTable, "failed to get metadata to update for "+res.FullName())
	}

	metadataToUpdate.Schema = toBQSchema(table.Schema)

	if table.Partition != nil {
		// updating range based partition after creation is not supported
		if table.Partition.Range == nil {
			metadataToUpdate.TimePartitioning = toBQTimePartitioning(table.Partition)
		}
	}

	_, err = t.bqTable.Update(ctx, metadataToUpdate, "")
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) && metaErr.Code == http.StatusNotFound {
			return errors.NotFound(resource.EntityTable, "failed to update table in bigquery for "+res.FullName())
		}
		return errors.InternalError(resource.EntityTable, "failed to update table on bigquery for "+res.FullName(), err)
	}

	return nil
}

func NewTableHandle(bq BqTable) *TableHandle {
	return &TableHandle{bqTable: bq}
}

func toBQTableMetadata(t *resource.Table, res *resource.Resource) (*bigquery.TableMetadata, error) {
	meta, err := getMetadataToCreate(t.Description, t.ExtraConfig, res.Metadata().Labels)
	if err != nil {
		return nil, errors.AddErrContext(err, resource.EntityExternalTable, "failed to get metadata to update for "+t.FullName())
	}

	meta.Schema = toBQSchema(t.Schema)

	if t.Partition != nil {
		if t.Partition.Range == nil {
			meta.TimePartitioning = toBQTimePartitioning(t.Partition)
		} else {
			meta.RangePartitioning = toBQRangePartitioning(t.Partition)
		}
	}
	if t.Cluster != nil {
		meta.Clustering = toBQClustering(t.Cluster)
	}

	return meta, nil
}

func toBQRangePartitioning(t *resource.Partition) *bigquery.RangePartitioning {
	return &bigquery.RangePartitioning{
		Field: t.Field,
		Range: &bigquery.RangePartitioningRange{
			Start:    t.Range.Start,
			End:      t.Range.End,
			Interval: t.Range.Interval,
		},
	}
}

func toBQTimePartitioning(t *resource.Partition) *bigquery.TimePartitioning {
	info := &bigquery.TimePartitioning{
		Field:      t.Field,
		Expiration: time.Duration(t.Expiration) * time.Hour,
	}
	if strings.EqualFold(t.Type, string(bigquery.HourPartitioningType)) {
		info.Type = bigquery.HourPartitioningType
	} else {
		info.Type = bigquery.DayPartitioningType
	}
	return info
}

func toBQClustering(ct *resource.Cluster) *bigquery.Clustering {
	clustering := &bigquery.Clustering{
		Fields: ct.Using,
	}
	return clustering
}
