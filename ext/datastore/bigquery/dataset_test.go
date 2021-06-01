package bigquery

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
)

func TestDataset(t *testing.T) {
	testingContext := context.Background()
	testingProject := "project"
	testingDataset := "dataset"
	datasetLabels := map[string]string{
		"application": "optimus",
	}
	bQDatasetMetadata := BQDatasetMetadata{
		Labels: datasetLabels,
	}
	bQResource := BQDataset{
		Project:  testingProject,
		Dataset:  testingDataset,
		Metadata: bQDatasetMetadata,
	}
	errNotFound := &googleapi.Error{
		Code: 404,
	}
	t.Run("ensureDataset", func(t *testing.T) {
		t.Run("should create dataset if it does not exist", func(t *testing.T) {
			upsert := false

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQDatasetHandle.On("Metadata", testingContext).Return((*bqiface.DatasetMetadata)(nil), errNotFound)
			bQDatasetHandle.On("Create", testingContext, &bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{
					Labels: datasetLabels,
				},
			}).Return(nil)

			err := ensureDataset(testingContext, bQDatasetHandle, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should not do insert nor update if dataset is exist and it is not an upsert", func(t *testing.T) {
			upsert := false
			eTag := "uniqueID"
			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{
					Labels: datasetLabels,
					ETag:   eTag,
				},
			}

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)

			err := ensureDataset(testingContext, bQDatasetHandle, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return any error encountered when getting Metadata, except for an *googleapi.Error{Code: 404}", func(t *testing.T) {
			upsert := false
			otherErrors := []error{
				&googleapi.Error{Code: 401},
				errors.New("unexpected"),
			}
			for _, e := range otherErrors {
				bQDatasetHandle := new(BqDatasetMock)
				defer bQDatasetHandle.AssertExpectations(t)

				bQDatasetHandle.On("Metadata", testingContext).Return((*bqiface.DatasetMetadata)(nil), e)

				err := ensureDataset(testingContext, bQDatasetHandle, bQResource, upsert)
				assert.Equal(t, e, err)
			}
		})
		t.Run("should update dataset if it is an upsert", func(t *testing.T) {
			upsert := true
			eTag := "uniqueID"
			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{
					Labels: datasetLabels,
					ETag:   eTag,
				},
			}

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)

			datasetMetadataToUpdate := bqiface.DatasetMetadataToUpdate{}
			datasetMetadataToUpdate.Description = bQResource.Metadata.Description
			datasetMetadataToUpdate.Name = bQResource.Dataset
			bQDatasetHandle.On("Update", testingContext, datasetMetadataToUpdate, eTag).Return((*bqiface.DatasetMetadata)(nil), nil)

			err := ensureDataset(testingContext, bQDatasetHandle, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should fail if updating dataset fails", func(t *testing.T) {
			upsert := true
			eTag := "uniqueID"
			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{
					Labels: datasetLabels,
					ETag:   eTag,
				},
			}

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)

			datasetMetadataToUpdate := bqiface.DatasetMetadataToUpdate{}
			datasetMetadataToUpdate.Description = bQResource.Metadata.Description
			datasetMetadataToUpdate.Name = bQResource.Dataset
			bQDatasetHandle.On("Update", testingContext, datasetMetadataToUpdate, eTag).Return((*bqiface.DatasetMetadata)(nil), errors.New("some error"))

			err := ensureDataset(testingContext, bQDatasetHandle, bQResource, upsert)
			assert.NotNil(t, err)
		})
	})
	t.Run("createDataset", func(t *testing.T) {
		t.Run("should create dataset if given valid input", func(t *testing.T) {
			upsert := false
			eTag := "uniqueID"
			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{
					Labels: datasetLabels,
					ETag:   eTag,
				},
			}
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)

			err := createDataset(testingContext, resourceSpec, bQClient, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return error when created dataset is failed to be fetched", func(t *testing.T) {
			upsert := false
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return((*bqiface.DatasetMetadata)(nil), errors.New("some error"))

			err := createDataset(testingContext, resourceSpec, bQClient, upsert)
			assert.NotNil(t, err)
		})
		t.Run("should return error if read BQ dataset spec is failed", func(t *testing.T) {
			upsert := false
			resourceSpec := models.ResourceSpec{
				Spec: "non BQ Dataset",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			err := createDataset(testingContext, resourceSpec, bQClient, upsert)
			assert.NotNil(t, err)
		})
	})
	t.Run("getDataset", func(t *testing.T) {
		t.Run("should retrieve BQ dataset information", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}
			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{
					Labels: datasetLabels,
				},
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)

			bQResource.Metadata.Labels = datasetLabels
			expectedResourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			actualResourceSpec, err := getDataset(testingContext, resourceSpec, bQClient)
			assert.Equal(t, expectedResourceSpec, actualResourceSpec)
			assert.Nil(t, err)
		})
		t.Run("should return error if read BQ dataset spec is failed", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: "non BQ dataset",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			actualResourceSpec, err := getDataset(testingContext, resourceSpec, bQClient)
			assert.Equal(t, models.ResourceSpec{}, actualResourceSpec)
			assert.NotNil(t, err)
		})
		t.Run("should return error if read dataset metadata is failed", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return((*bqiface.DatasetMetadata)(nil), errors.New("some error"))

			actualResourceSpec, err := getDataset(testingContext, resourceSpec, bQClient)
			assert.Equal(t, models.ResourceSpec{}, actualResourceSpec)
			assert.NotNil(t, err)
		})
	})
	t.Run("deleteDataset", func(t *testing.T) {
		t.Run("should able to delete dataset if given bq dataset", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Delete", testingContext).Return(nil)

			err := deleteDataset(testingContext, resourceSpec, bQClient)
			assert.Nil(t, err)
		})
		t.Run("should return error if read BQ dataset spec is failed", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: "non BQ dataset",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			err := deleteDataset(testingContext, resourceSpec, bQClient)
			assert.NotNil(t, err)
		})
	})
}
