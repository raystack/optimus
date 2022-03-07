package bigquery_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	bqapi "cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"

	"github.com/odpf/optimus/ext/datastore/bigquery"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestBigquery(t *testing.T) {
	testingContext := context.Background()
	testingProject := "project"
	testingDataset := "dataset"
	secret := "some_secret"
	projectSpec := models.ProjectSpec{
		Secret: models.ProjectSecrets{{
			Name:  bigquery.SecretName,
			Value: secret,
		}},
	}

	t.Run("CreateResource", func(t *testing.T) {
		t.Run("should return error when secret not found", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
				Type: models.ResourceTypeDataset,
			}
			invalidProjectSpec := models.ProjectSpec{}
			resourceRequest := models.CreateResourceRequest{
				Resource: resourceSpec,
				Project:  invalidProjectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bq := bigquery.BigQuery{}
			err := bq.CreateResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when creating BQ client is failed", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
				Type: models.ResourceTypeDataset,
			}
			resourceRequest := models.CreateResourceRequest{
				Resource: resourceSpec,
				Project:  projectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, errors.New("some error"))

			bq := bigquery.BigQuery{
				ClientFac: bQClientFactory,
			}
			err := bq.CreateResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when the resource type is unsupported", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
			}
			resourceRequest := models.CreateResourceRequest{
				Resource: resourceSpec,
				Project:  projectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, nil)

			bq := bigquery.BigQuery{
				ClientFac: bQClientFactory,
			}
			err := bq.CreateResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
	})
	t.Run("UpdateResource", func(t *testing.T) {
		t.Run("should return error when secret not found", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
				Type: models.ResourceTypeDataset,
			}
			invalidProjectSpec := models.ProjectSpec{}
			resourceRequest := models.UpdateResourceRequest{
				Resource: resourceSpec,
				Project:  invalidProjectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bq := bigquery.BigQuery{}
			err := bq.UpdateResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when creating BQ client is failed", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
				Type: models.ResourceTypeDataset,
			}
			resourceRequest := models.UpdateResourceRequest{
				Resource: resourceSpec,
				Project:  projectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, errors.New("some error"))

			bq := bigquery.BigQuery{
				ClientFac: bQClientFactory,
			}
			err := bq.UpdateResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when the resource type is unsupported", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
			}
			resourceRequest := models.UpdateResourceRequest{
				Resource: resourceSpec,
				Project:  projectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, nil)

			bq := bigquery.BigQuery{
				ClientFac: bQClientFactory,
			}
			err := bq.UpdateResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
	})
	t.Run("ReadResource", func(t *testing.T) {
		t.Run("should return error when secret not found", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
				Type: models.ResourceTypeDataset,
			}
			invalidProjectSpec := models.ProjectSpec{}
			resourceRequest := models.ReadResourceRequest{
				Resource: resourceSpec,
				Project:  invalidProjectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bq := bigquery.BigQuery{}
			resp, err := bq.ReadResource(testingContext, resourceRequest)

			assert.Equal(t, models.ResourceSpec{}, resp.Resource)
			assert.NotNil(t, err)
		})
		t.Run("should return error when creating BQ client is failed", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
				Type: models.ResourceTypeDataset,
			}
			resourceRequest := models.ReadResourceRequest{
				Resource: resourceSpec,
				Project:  projectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, errors.New("some error"))

			bq := bigquery.BigQuery{
				ClientFac: bQClientFactory,
			}
			resp, err := bq.ReadResource(testingContext, resourceRequest)

			assert.Equal(t, models.ResourceSpec{}, resp.Resource)
			assert.NotNil(t, err)
		})
		t.Run("should return error when the resource type is unsupported", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
			}
			resourceRequest := models.ReadResourceRequest{
				Resource: resourceSpec,
				Project:  projectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, nil)

			bq := bigquery.BigQuery{
				ClientFac: bQClientFactory,
			}
			resp, err := bq.ReadResource(testingContext, resourceRequest)

			assert.Equal(t, models.ResourceSpec{}, resp.Resource)
			assert.NotNil(t, err)
		})
	})
	t.Run("DeleteResource", func(t *testing.T) {
		t.Run("should return error when secret not found", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
				Type: models.ResourceTypeDataset,
			}
			invalidProjectSpec := models.ProjectSpec{}
			resourceRequest := models.DeleteResourceRequest{
				Resource: resourceSpec,
				Project:  invalidProjectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bq := bigquery.BigQuery{}
			err := bq.DeleteResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when creating BQ client is failed", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
				Type: models.ResourceTypeDataset,
			}
			resourceRequest := models.DeleteResourceRequest{
				Resource: resourceSpec,
				Project:  projectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, errors.New("some error"))

			bq := bigquery.BigQuery{
				ClientFac: bQClientFactory,
			}
			err := bq.DeleteResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when the resource type is unsupported", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := bigquery.BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := bigquery.BQDataset{
				Project:  testingProject,
				Dataset:  testingDataset,
				Metadata: bQDatasetMetadata,
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQDatasetResource,
			}
			resourceRequest := models.DeleteResourceRequest{
				Resource: resourceSpec,
				Project:  projectSpec,
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, nil)

			bq := bigquery.BigQuery{
				ClientFac: bQClientFactory,
			}
			err := bq.DeleteResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
	})
	t.Run("BackupResource", func(t *testing.T) {
		t.Run("should not return error when resource supported", func(t *testing.T) {
			spec := bigquery.BQTable{
				Project: "project",
				Dataset: "dataset",
				Table:   "table",
			}
			eTag := "unique ID"
			tableMetadata := &bqapi.TableMetadata{
				Name: spec.Table,
				Schema: bqapi.Schema{
					{
						Name: "message",
						Type: "STRING",
					},
					{
						Name: "message_type",
						Type: "STRING",
					},
					{
						Name:     "recipient",
						Type:     "STRING",
						Repeated: true,
					},
					{
						Name: "time",
						Type: "TIME",
					},
				},
				Clustering: &bqapi.Clustering{
					Fields: []string{"message_type"},
				},
				ETag: eTag,
			}
			resourceSpec := models.ResourceSpec{
				Name: "project:dataset.table",
				Spec: spec,
				Type: models.ResourceTypeTable,
			}
			backupTime := time.Now()
			resourceRequest := models.BackupResourceRequest{
				Resource: resourceSpec,
				BackupSpec: models.BackupRequest{
					Project: projectSpec,
					Config: map[string]string{
						models.ConfigTTL:             "720h",
						bigquery.BackupConfigDataset: "optimus_backup",
						bigquery.BackupConfigPrefix:  "backup",
					},
					ID: uuid.Must(uuid.NewRandom()),
				},
				BackupTime: backupTime,
			}

			destinationTable := bigquery.BQTable{
				Project: spec.Project,
				Dataset: resourceRequest.BackupSpec.Config[bigquery.BackupConfigDataset],
				Table:   fmt.Sprintf("backup_dataset_table_%s", backupTime.Format("2006_01_02_15_04_05")),
			}
			resultURN := fmt.Sprintf("%s://%s:%s.%s", bigquery.BigQuery{}.Name(), destinationTable.Project, destinationTable.Dataset, destinationTable.Table)

			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bqapi.DatasetMetadata{
					ETag: eTag,
				},
			}

			toUpdate := bqapi.TableMetadataToUpdate{
				ExpirationTime: resourceRequest.BackupTime.Add(time.Hour * 24 * 30),
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQDatasetHandle := new(bigquery.BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(bigquery.BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQCopier := new(bigquery.BqCopierMock)
			defer bQCopier.AssertExpectations(t)

			bQJob := new(bigquery.BqJobMock)
			defer bQJob.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, nil)

			//duplicate table
			bQClient.On("DatasetInProject", spec.Project, spec.Dataset).Return(bQDatasetHandle).Once()
			bQClient.On("DatasetInProject", destinationTable.Project, destinationTable.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)
			bQDatasetHandle.On("Table", spec.Table).Return(bQTable)
			bQDatasetHandle.On("Table", destinationTable.Table).Return(bQTable)
			bQTable.On("CopierFrom", []bqiface.Table{bQTable}).Return(bQCopier)
			bQCopier.On("Run", testingContext).Return(bQJob, nil)
			bQJob.On("Wait", testingContext).Return(&bqapi.JobStatus{}, nil)

			//update expiry
			bQTable.On("Metadata", testingContext).Return(tableMetadata, nil).Once()
			bQTable.On("Update", testingContext, toUpdate, eTag).Return(tableMetadata, nil)

			//verify
			bQTable.On("Metadata", testingContext).Return(tableMetadata, nil).Once()
			bq := bigquery.BigQuery{
				ClientFac: bQClientFactory,
			}

			resp, err := bq.BackupResource(testingContext, resourceRequest)

			assert.Nil(t, err)
			assert.Equal(t, resultURN, resp.ResultURN)
			assert.Equal(t, destinationTable, resp.ResultSpec)
		})
		t.Run("should return error when resource is not supported", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Name: "project:dataset.table",
				Spec: bigquery.BQTable{
					Project: "project",
					Dataset: "dataset",
					Table:   "table",
				},
				Type: models.ResourceTypeView,
			}
			resourceRequest := models.BackupResourceRequest{
				Resource: resourceSpec,
			}

			bq := bigquery.BigQuery{}
			resp, err := bq.BackupResource(testingContext, resourceRequest)

			assert.Equal(t, models.ErrUnsupportedResource, err)
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should return error when datastore secret is not available", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Name: "project:dataset.table",
				Spec: bigquery.BQTable{
					Project: "project",
					Dataset: "dataset",
					Table:   "table",
				},
				Type: models.ResourceTypeTable,
			}
			resourceRequest := models.BackupResourceRequest{
				Resource: resourceSpec,
				BackupSpec: models.BackupRequest{
					Project: models.ProjectSpec{
						Secret: models.ProjectSecrets{{
							Name:  "other_secret",
							Value: secret,
						}},
					},
				},
			}

			bq := bigquery.BigQuery{}
			resp, err := bq.BackupResource(testingContext, resourceRequest)

			assert.Equal(t, fmt.Sprintf("secret %s required to migrate datastore not found for %s", bigquery.SecretName, bq.Name()), err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should return error when unable to create bq client", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Name: "project:dataset.table",
				Spec: bigquery.BQTable{
					Project: "project",
					Dataset: "dataset",
					Table:   "table",
				},
				Type: models.ResourceTypeTable,
			}
			resourceRequest := models.BackupResourceRequest{
				Resource: resourceSpec,
				BackupSpec: models.BackupRequest{
					Project: models.ProjectSpec{
						Secret: models.ProjectSecrets{{
							Name:  bigquery.SecretName,
							Value: secret,
						}},
					},
				},
			}

			bQClient := new(bigquery.BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(bigquery.BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			errorMsg := "bq client failed"
			bQClientFactory.On("New", testingContext, secret).Return(bQClient, errors.New(errorMsg))

			bq := bigquery.BigQuery{
				ClientFac: bQClientFactory,
			}
			resp, err := bq.BackupResource(testingContext, resourceRequest)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
	})
}
