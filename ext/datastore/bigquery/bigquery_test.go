package bigquery

import (
	"context"
	"errors"
	"testing"

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
				Project:  projectSpec,
			}

			bq := BigQuery{}
			err := bq.BackupResource(testingContext, resourceRequest)

			assert.Nil(t, err)
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
				Project:  projectSpec,
			}

			bq := BigQuery{}
			err := bq.BackupResource(testingContext, resourceRequest)

			assert.Equal(t, models.ErrUnsupportedResource, err)
		})
	})
}
