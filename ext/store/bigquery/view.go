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

type ViewHandle struct {
	bqView BqTable
}

func (v ViewHandle) Create(ctx context.Context, res *resource.Resource) error {
	view, err := resource.ConvertSpecTo[resource.View](res)
	if err != nil {
		return err
	}

	meta := &bigquery.TableMetadata{
		ViewQuery:   view.ViewQuery,
		Description: view.Description,
		Labels:      res.Metadata().Labels,
	}
	expiration := ConfigAs[string](view.ExtraConfig, expirationTimeKey)
	if expiration != "" {
		expiryTime, err := time.Parse(time.RFC3339, expiration)
		if err != nil {
			return errors.InvalidArgument(resource.EntityView, "unable to parse timestamp "+view.FullName())
		}
		meta.ExpirationTime = expiryTime
	}

	err = v.bqView.Create(ctx, meta)
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) &&
			metaErr.Code == 409 && strings.Contains(metaErr.Message, "Already Exists") {
			return errors.AlreadyExists(resource.EntityView, "view already exists on bigquery: "+res.FullName())
		}
		return errors.InternalError(resource.EntityView, "failed to create resource "+res.FullName(), err)
	}
	return nil
}

func (v ViewHandle) Update(ctx context.Context, res *resource.Resource) error {
	view, err := resource.ConvertSpecTo[resource.View](res)
	if err != nil {
		return err
	}

	metadataToUpdate := bigquery.TableMetadataToUpdate{
		Description: view.Description,
		ViewQuery:   view.ViewQuery,
	}

	expiration := ConfigAs[string](view.ExtraConfig, expirationTimeKey)
	if expiration != "" {
		expiryTime, err := time.Parse(time.RFC3339, expiration)
		if err != nil {
			return errors.InvalidArgument(resource.EntityView, "unable to parse timestamp "+view.FullName())
		}
		metadataToUpdate.ExpirationTime = expiryTime
	}

	for k, val := range res.Metadata().Labels {
		metadataToUpdate.SetLabel(k, val)
	}

	_, err = v.bqView.Update(ctx, metadataToUpdate, "")
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) && metaErr.Code == http.StatusNotFound {
			return errors.NotFound(resource.EntityView, "failed to update dataset in bigquery for "+res.FullName())
		}
		return errors.InternalError(resource.EntityView, "failed to update resource on bigquery for "+res.FullName(), err)
	}

	return nil
}

func NewViewHandle(bq BqTable) *ViewHandle {
	return &ViewHandle{bqView: bq}
}
