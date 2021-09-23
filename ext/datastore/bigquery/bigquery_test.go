package bigquery

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"

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
			Name:  SecretName,
			Value: secret,
		}},
	}

	t.Run("CreateResource", func(t *testing.T) {
		t.Run("should return error when secret not found", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bq := BigQuery{}
			err := bq.CreateResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when creating BQ client is failed", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, errors.New("some error"))

			bq := BigQuery{
				ClientFac: bQClientFactory,
			}
			err := bq.CreateResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when the resource type is unsupported", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, nil)

			bq := BigQuery{
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
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bq := BigQuery{}
			err := bq.UpdateResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when creating BQ client is failed", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, errors.New("some error"))

			bq := BigQuery{
				ClientFac: bQClientFactory,
			}
			err := bq.UpdateResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when the resource type is unsupported", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, nil)

			bq := BigQuery{
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
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bq := BigQuery{}
			resp, err := bq.ReadResource(testingContext, resourceRequest)

			assert.Equal(t, models.ResourceSpec{}, resp.Resource)
			assert.NotNil(t, err)
		})
		t.Run("should return error when creating BQ client is failed", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, errors.New("some error"))

			bq := BigQuery{
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
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, nil)

			bq := BigQuery{
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
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bq := BigQuery{}
			err := bq.DeleteResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when creating BQ client is failed", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, errors.New("some error"))

			bq := BigQuery{
				ClientFac: bQClientFactory,
			}
			err := bq.DeleteResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
		t.Run("should return error when the resource type is unsupported", func(t *testing.T) {
			datasetLabels := map[string]string{
				"application": "optimus",
			}
			bQDatasetMetadata := BQDatasetMetadata{
				Labels: datasetLabels,
			}
			bQDatasetResource := BQDataset{
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

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQClientFactory.On("New", testingContext, secret).Return(bQClient, nil)

			bq := BigQuery{
				ClientFac: bQClientFactory,
			}
			err := bq.DeleteResource(testingContext, resourceRequest)

			assert.NotNil(t, err)
		})
	})
	t.Run("BackupResource", func(t *testing.T) {
		t.Run("should not return error when resource supported", func(t *testing.T) {
			spec := BQTable{
				Project: "project",
				Dataset: "dataset",
				Table:   "table",
			}
			eTag := "unique ID"
			tableMetadata := &bigquery.TableMetadata{
				Name: spec.Table,
				Schema: bigquery.Schema{
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
				Clustering: &bigquery.Clustering{
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
					Config: models.DestinationConfig{
						TTLInDays:   30,
						Dataset:     "optimus_backup",
						TablePrefix: "backup",
					},
					ID:         uuid.Must(uuid.NewRandom()),
					BackupTime: backupTime,
				},
			}

			destinationTable := BQTable{
				Project: spec.Project,
				Dataset: resourceRequest.BackupSpec.Config.Dataset,
				Table:   fmt.Sprintf("backup_dataset_table_%s", resourceRequest.BackupSpec.ID),
			}
			resultURN := fmt.Sprintf(tableURNFormat, BigQuery{}.Name(), destinationTable.Project, destinationTable.Dataset, destinationTable.Table)

			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{
					ETag: eTag,
				},
			}

			toUpdate := bigquery.TableMetadataToUpdate{
				ExpirationTime: resourceRequest.BackupSpec.BackupTime.Add(time.Hour * 24 * 30),
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQCopier := new(BqCopierMock)
			defer bQCopier.AssertExpectations(t)

			bQJob := new(BqJobMock)
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
			bQJob.On("Wait", testingContext).Return(&bigquery.JobStatus{}, nil)

			//update expiry
			bQTable.On("Metadata", testingContext).Return(tableMetadata, nil).Once()
			bQTable.On("Update", testingContext, toUpdate, eTag).Return(tableMetadata, nil)

			//verify
			bQTable.On("Metadata", testingContext).Return(tableMetadata, nil).Once()
			bq := BigQuery{
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
				Spec: BQTable{
					Project: "project",
					Dataset: "dataset",
					Table:   "table",
				},
				Type: models.ResourceTypeView,
			}
			resourceRequest := models.BackupResourceRequest{
				Resource: resourceSpec,
			}

			bq := BigQuery{}
			resp, err := bq.BackupResource(testingContext, resourceRequest)

			assert.Equal(t, models.ErrUnsupportedResource, err)
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should return error when datastore secret is not available", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Name: "project:dataset.table",
				Spec: BQTable{
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

			bq := BigQuery{}
			resp, err := bq.BackupResource(testingContext, resourceRequest)

			assert.Equal(t, fmt.Sprintf(errSecretNotFoundStr, SecretName, bq.Name()), err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should return error when unable to create bq client", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Name: "project:dataset.table",
				Spec: BQTable{
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
							Name:  SecretName,
							Value: secret,
						}},
					},
				},
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClientFactory := new(BQClientFactoryMock)
			defer bQClientFactory.AssertExpectations(t)

			errorMsg := "bq client failed"
			bQClientFactory.On("New", testingContext, secret).Return(bQClient, errors.New(errorMsg))

			bq := BigQuery{
				ClientFac: bQClientFactory,
			}
			resp, err := bq.BackupResource(testingContext, resourceRequest)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
	})
}
