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

	meta, err := getMetadataToCreate(view.Description, view.ExtraConfig, res.Metadata().Labels)
	if err != nil {
		return errors.AddErrContext(err, resource.EntityView, "failed to get metadata to update for "+res.FullName())
	}
	meta.ViewQuery = view.ViewQuery

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

	meta, err := getMetadataToUpdate(view.Description, view.ExtraConfig, res.Metadata().Labels)
	if err != nil {
		return errors.AddErrContext(err, resource.EntityView, "failed to get metadata to update for "+res.FullName())
	}
	meta.ViewQuery = view.ViewQuery

	_, err = v.bqView.Update(ctx, meta, "")
	if err != nil {
		var metaErr *googleapi.Error
		if errors.As(err, &metaErr) && metaErr.Code == http.StatusNotFound {
			return errors.NotFound(resource.EntityView, "failed to update dataset in bigquery for "+res.FullName())
		}
		return errors.InternalError(resource.EntityView, "failed to update resource on bigquery for "+res.FullName(), err)
	}

	return nil
}

func (v ViewHandle) Exists(ctx context.Context) bool {
	_, err := v.bqView.Metadata(ctx)
	// There can be connection issue, we return false for now
	return err == nil
}

func NewViewHandle(bq BqTable) *ViewHandle {
	return &ViewHandle{bqView: bq}
}

func getMetadataToCreate(desc string, extraConf map[string]any, labels map[string]string) (*bigquery.TableMetadata, error) {
	meta := &bigquery.TableMetadata{
		Description: desc,
		Labels:      labels,
	}
	expiration := ConfigAs[string](extraConf, expirationTimeKey)
	if expiration != "" {
		expiryTime, err := time.Parse(time.RFC3339, expiration)
		if err != nil {
			return nil, errors.InvalidArgument(resource.EntityResource, "unable to parse timestamp ")
		}
		meta.ExpirationTime = expiryTime
	}
	return meta, nil
}

func getMetadataToUpdate(description string, extraConf map[string]any, labels map[string]string) (bigquery.TableMetadataToUpdate, error) {
	metadataToUpdate := bigquery.TableMetadataToUpdate{}
	if len(description) > 0 {
		metadataToUpdate.Description = description
	}

	expiration := ConfigAs[string](extraConf, expirationTimeKey)
	if expiration != "" {
		expiryTime, err := time.Parse(time.RFC3339, expiration)
		if err != nil {
			return bigquery.TableMetadataToUpdate{}, errors.InvalidArgument(resource.EntityResource, "unable to parse timestamp")
		}
		metadataToUpdate.ExpirationTime = expiryTime
	}

	for k, val := range labels {
		metadataToUpdate.SetLabel(k, val)
	}

	return metadataToUpdate, nil
}
