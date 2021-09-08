package bigquery

import (
	"context"
	"net/http"
	"sync"
	"time"

	bqapi "cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
)

var (
	datasetMutex sync.Mutex
)

func createDataset(ctx context.Context, spec models.ResourceSpec, client bqiface.Client, upsert bool) error {
	bqResource, ok := spec.Spec.(BQDataset)
	if !ok {
		return errors.New("failed to read dataset spec for bigquery")
	}

	// inherit from base
	bqResource.Metadata.Labels = spec.Labels

	dataset := client.DatasetInProject(bqResource.Project, bqResource.Dataset)
	if err := ensureDataset(ctx, dataset, bqResource, upsert); err != nil {
		return err
	}
	return nil
}

func ensureDataset(ctx context.Context, datasetHandle bqiface.Dataset, bqResource BQDataset, upsert bool) error {
	// this is needed if dataset is getting updated & tables are created at the same time
	datasetMutex.Lock()
	defer datasetMutex.Unlock()

	meta, err := datasetHandle.Metadata(ctx)
	if err != nil {
		if metaErr, ok := err.(*googleapi.Error); !ok || metaErr.Code != http.StatusNotFound {
			return err
		}
		meta := bqapi.DatasetMetadata{
			Description: bqResource.Metadata.Description,
			Labels:      bqResource.Metadata.Labels,
		}
		if bqResource.Metadata.DefaultTableExpiration > 0 {
			meta.DefaultTableExpiration = time.Hour * time.Duration(bqResource.Metadata.DefaultTableExpiration)
		}
		return datasetHandle.Create(ctx, &bqiface.DatasetMetadata{
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
	var bqResource BQDataset
	bqResource, ok := resourceSpec.Spec.(BQDataset)
	if !ok {
		return models.ResourceSpec{}, errors.New("failed to read dataset spec for bigquery")
	}

	dataset := client.DatasetInProject(bqResource.Project, bqResource.Dataset)
	datasetMeta, err := dataset.Metadata(ctx)
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
	bqResource, ok := resourceSpec.Spec.(BQDataset)
	if !ok {
		return errors.New("failed to read dataset spec for bigquery")
	}
	dataset := client.DatasetInProject(bqResource.Project, bqResource.Dataset)
	return dataset.Delete(ctx)
}
