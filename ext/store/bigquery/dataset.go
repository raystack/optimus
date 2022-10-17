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

const (
	locationKey        = "location"
	tableExpirationKey = "table_expiration"
)

type BqDataset interface {
	Create(context.Context, *bigquery.DatasetMetadata) error
	Update(context.Context, bigquery.DatasetMetadataToUpdate, string) (*bigquery.DatasetMetadata, error)
}

type DatasetHandle struct {
	bqDataset BqDataset
}

func (d DatasetHandle) Create(ctx context.Context, res *resource.Resource) error {
	details, err := resource.ConvertSpecTo[resource.DatasetDetails](res)
	if err != nil {
		return err
	}

	err = d.bqDataset.Create(ctx, toBQDatasetMetadata(details, res))
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) &&
			metaErr.Code == 409 && strings.Contains(metaErr.Message, "Already Exists") {
			return errors.AlreadyExists(resource.EntityDataset, "dataset already exists on bigquery: "+res.FullName())
		}
		return errors.InternalError(resource.EntityDataset, "failed to create resource "+res.FullName(), err)
	}
	return nil
}

func (d DatasetHandle) Update(ctx context.Context, res *resource.Resource) error {
	details, err := resource.ConvertSpecTo[resource.DatasetDetails](res)
	if err != nil {
		return err
	}

	metadataToUpdate := bigquery.DatasetMetadataToUpdate{}
	if len(details.Description) > 0 {
		metadataToUpdate.Description = details.Description
	}

	expirationAsInt := ConfigAs[int](details.ExtraConfig, tableExpirationKey)
	if expirationAsInt > 0 {
		metadataToUpdate.DefaultTableExpiration = time.Hour * time.Duration(expirationAsInt)
	}

	for k, v := range res.Metadata().Labels {
		metadataToUpdate.SetLabel(k, v)
	}

	_, err = d.bqDataset.Update(ctx, metadataToUpdate, "")
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) && metaErr.Code == http.StatusNotFound {
			return errors.NotFound(resource.EntityDataset, "failed to update dataset in bigquery for "+res.FullName())
		}
		return errors.InternalError(resource.EntityDataset, "failed to update resource on bigquery for "+res.FullName(), err)
	}

	return nil
}

func NewDatasetHandle(ds BqDataset) *DatasetHandle {
	return &DatasetHandle{bqDataset: ds}
}

func toBQDatasetMetadata(details *resource.DatasetDetails, res *resource.Resource) *bigquery.DatasetMetadata {
	meta := &bigquery.DatasetMetadata{
		Description: details.Description,
		Labels:      res.Metadata().Labels,
	}

	location := ConfigAs[string](details.ExtraConfig, locationKey)
	if location != "" {
		meta.Location = location
	}

	expirationAsInt := ConfigAs[int](details.ExtraConfig, tableExpirationKey)
	if expirationAsInt > 0 {
		meta.DefaultTableExpiration = time.Hour * time.Duration(expirationAsInt)
	}

	return meta
}

func ConfigAs[T any](mapping map[string]any, key string) T {
	var zero T
	val, ok := mapping[key]
	if ok {
		s, ok := val.(T)
		if ok {
			return s
		}
	}
	return zero
}
