package bigquery

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	bqapi "cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"google.golang.org/api/googleapi"

	"github.com/odpf/optimus/models"
)

var datasetMutex sync.Mutex

func createDataset(ctx context.Context, spec models.ResourceSpec, client bqiface.Client, upsert bool) error {
	spanCtx, span := startChildSpan(ctx, "CreateDataset")
	defer span.End()

	bqResource, ok := spec.Spec.(BQDataset)
	if !ok {
		return errors.New("failed to read dataset spec for bigquery")
	}

	// inherit from base
	bqResource.Metadata.Labels = spec.Labels

	dataset := client.DatasetInProject(bqResource.Project, bqResource.Dataset)
	return ensureDataset(spanCtx, dataset, bqResource, upsert)
}

func ensureDataset(ctx context.Context, datasetHandle bqiface.Dataset, bqResource BQDataset, upsert bool) error {
	spanCtx, span := startChildSpan(ctx, "EnsureDataset")
	defer span.End()

	// this is needed if dataset is getting updated & tables are created at the same time
	datasetMutex.Lock()
	defer datasetMutex.Unlock()

	meta, err := datasetHandle.Metadata(spanCtx)
	if err != nil {
		var metaErr *googleapi.Error
		if !errors.As(err, &metaErr) || metaErr.Code != http.StatusNotFound {
			return err
		}
		meta := bqapi.DatasetMetadata{
			Description: bqResource.Metadata.Description,
			Labels:      bqResource.Metadata.Labels,
		}
		if bqResource.Metadata.DefaultTableExpiration > 0 {
			meta.DefaultTableExpiration = time.Hour * time.Duration(bqResource.Metadata.DefaultTableExpiration)
		}
		return datasetHandle.Create(spanCtx, &bqiface.DatasetMetadata{
			DatasetMetadata: meta,
		})
	}
	if !upsert {
		return nil
	}

	// update if already exists
	m := bqapi.DatasetMetadataToUpdate{
		Description: bqResource.Metadata.Description,
		Name:        bqResource.Dataset,
	}
	if bqResource.Metadata.DefaultTableExpiration > 0 {
		m.DefaultTableExpiration = time.Hour * time.Duration(bqResource.Metadata.DefaultTableExpiration)
	}
	datasetMetadataToUpdate := bqiface.DatasetMetadataToUpdate{
		DatasetMetadataToUpdate: m,
	}
	if _, err := datasetHandle.Update(ctx, datasetMetadataToUpdate, meta.ETag); err != nil {
		return err
	}
	return nil
}

// getDataset retrieves bq dataset information
func getDataset(ctx context.Context, resourceSpec models.ResourceSpec, client bqiface.Client) (models.ResourceSpec, error) {
	spanCtx, span := startChildSpan(ctx, "GetDataset")
	defer span.End()

	var bqResource BQDataset
	bqResource, ok := resourceSpec.Spec.(BQDataset)
	if !ok {
		return models.ResourceSpec{}, errors.New("failed to read dataset spec for bigquery")
	}

	dataset := client.DatasetInProject(bqResource.Project, bqResource.Dataset)
	datasetMeta, err := dataset.Metadata(spanCtx)
	if err != nil {
		return models.ResourceSpec{}, err
	}

	bqResource.Metadata = BQDatasetMetadata{
		Description:            datasetMeta.Description,
		Labels:                 datasetMeta.Labels,
		DefaultTableExpiration: int64(datasetMeta.DefaultTableExpiration.Hours()),
		Location:               datasetMeta.Location,
	}
	resourceSpec.Spec = bqResource
	return resourceSpec, nil
}

func deleteDataset(ctx context.Context, resourceSpec models.ResourceSpec, client bqiface.Client) error {
	spanCtx, span := startChildSpan(ctx, "DeleteDataset")
	defer span.End()

	bqResource, ok := resourceSpec.Spec.(BQDataset)
	if !ok {
		return errors.New("failed to read dataset spec for bigquery")
	}
	dataset := client.DatasetInProject(bqResource.Project, bqResource.Dataset)
	return dataset.Delete(spanCtx)
}
