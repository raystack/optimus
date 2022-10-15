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
	expirationTimeKey = "expiration_time"
)

type ExternalTableHandle struct {
	bqExternalTable BqTable
}

func (et ExternalTableHandle) Create(ctx context.Context, res *resource.Resource) error {
	externalTable, err := resource.ConvertSpecTo[resource.ExternalTable](res)
	if err != nil {
		return err
	}

	meta := &bigquery.TableMetadata{
		Description: externalTable.Description,
		Labels:      res.Metadata().Labels,
	}

	expiration := ConfigAs[string](externalTable.ExtraConfig, expirationTimeKey)
	if expiration != "" {
		expiryTime, err := time.Parse(time.RFC3339, expiration)
		if err != nil {
			return errors.InvalidArgument(resource.EntityExternalTable, "unable to parse timestamp "+externalTable.FullName())
		}
		meta.ExpirationTime = expiryTime
	}

	err = et.bqExternalTable.Create(ctx, meta)
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) &&
			metaErr.Code == 409 && strings.Contains(metaErr.Message, "Already Exists") {
			return errors.AlreadyExists(resource.EntityExternalTable, "external table already exists on bigquery: "+res.FullName())
		}
		return errors.InternalError(resource.EntityExternalTable, "failed to create external table "+res.FullName(), err)
	}
	return nil
}

func (et ExternalTableHandle) Update(ctx context.Context, res *resource.Resource) error {
	externalTable, err := resource.ConvertSpecTo[resource.ExternalTable](res)
	if err != nil {
		return err
	}

	metadataToUpdate := bigquery.TableMetadataToUpdate{
		Description: externalTable.Description,
	}

	expiration := ConfigAs[string](externalTable.ExtraConfig, expirationTimeKey)
	if expiration != "" {
		expiryTime, err := time.Parse(time.RFC3339, expiration)
		if err != nil {
			return errors.InvalidArgument(resource.EntityExternalTable, "unable to parse timestamp "+externalTable.FullName())
		}
		metadataToUpdate.ExpirationTime = expiryTime
	}
	for k, v := range res.Metadata().Labels {
		metadataToUpdate.SetLabel(k, v)
	}

	_, err = et.bqExternalTable.Update(ctx, metadataToUpdate, "")
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) && metaErr.Code == http.StatusNotFound {
			return errors.NotFound(resource.EntityExternalTable, "failed to update external_table in bigquery for "+res.FullName())
		}
		return errors.InternalError(resource.EntityExternalTable, "failed to update external_table on bigquery for "+res.FullName(), err)
	}

	return nil
}

func NewExternalTableHandle(bq BqTable) *ExternalTableHandle {
	return &ExternalTableHandle{bqExternalTable: bq}
}
