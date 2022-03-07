package bigquery

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
)

func TestStandardView(t *testing.T) {
	testingContext := context.Background()
	testingProject := "project"
	testingDataset := "dataset"
	testingTable := "view"
	eTag := "etag-0000"
	errNotFound := &googleapi.Error{
		Code: 404,
	}
	viewQuery := "select * from project.dataset.table"
	bQTableMetadata := BQTableMetadata{
		ViewQuery: viewQuery,
	}
	bQResource := BQTable{
		Project:  testingProject,
		Dataset:  testingDataset,
		Table:    testingTable,
		Metadata: bQTableMetadata,
	}
	createTableMeta := &bigquery.TableMetadata{
		ViewQuery: bQResource.Metadata.ViewQuery,
	}
	t.Run("ensureStandardView", func(t *testing.T) {
		t.Run("should create view if it does not exist", func(t *testing.T) {
			upsert := false

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), errNotFound)
			bQTable.On("Create", testingContext, createTableMeta).Return(nil)

			err := ensureStandardView(testingContext, bQTable, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should not do insert nor update if view is exist and not an upsert call", func(t *testing.T) {
			upsert := false

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), nil)

			err := ensureStandardView(testingContext, bQTable, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return any error encountered, except for an *googleapi.Error{Code: 404}", func(t *testing.T) {
			upsert := false
			otherErrors := []error{
				&googleapi.Error{Code: 401},
				errors.New("unexpected"),
			}
			for _, e := range otherErrors {
				bQTable := new(BqTableMock)
				defer bQTable.AssertExpectations(t)

				bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), e)

				err := ensureStandardView(testingContext, bQTable, bQResource, upsert)
				assert.Equal(t, e, err)
			}
		})
		t.Run("should update view if it is already exist and an upsert call", func(t *testing.T) {
			upsert := true
			updatedViewQuery := "select * from project.dataset.table when current_date() >= event_timestamp"
			description := "view description"
			updatedBQTableMetadata := BQTableMetadata{
				ViewQuery:   updatedViewQuery,
				Description: description,
			}
			updateBQResource := BQTable{
				Project:  bQResource.Project,
				Dataset:  bQResource.Dataset,
				Table:    bQResource.Dataset,
				Metadata: updatedBQTableMetadata,
			}
			updateTableMeta := bigquery.TableMetadataToUpdate{
				Description: description,
				ViewQuery:   updatedViewQuery,
			}
			tableMeta := &bigquery.TableMetadata{
				ViewQuery: bQResource.Metadata.ViewQuery,
				ETag:      eTag,
			}

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return(tableMeta, nil)
			bQTable.On("Update", testingContext, updateTableMeta, eTag).Return((*bigquery.TableMetadata)(nil), nil)

			err := ensureStandardView(testingContext, bQTable, updateBQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return error when update view failed", func(t *testing.T) {
			upsert := true
			updatedViewQuery := "select * from project.dataset.table when current_date() >= event_timestamp"
			description := "view description"
			updatedBQTableMetadata := BQTableMetadata{
				ViewQuery:   updatedViewQuery,
				Description: description,
			}
			updateBQResource := BQTable{
				Project:  bQResource.Project,
				Dataset:  bQResource.Dataset,
				Table:    bQResource.Dataset,
				Metadata: updatedBQTableMetadata,
			}
			updateTableMeta := bigquery.TableMetadataToUpdate{
				Description: description,
				ViewQuery:   updatedViewQuery,
			}
			tableMeta := &bigquery.TableMetadata{
				ViewQuery: bQResource.Metadata.ViewQuery,
				ETag:      eTag,
			}

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return(tableMeta, nil)
			bQTable.On("Update", testingContext, updateTableMeta, eTag).Return((*bigquery.TableMetadata)(nil), errors.New("some error"))

			err := ensureStandardView(testingContext, bQTable, updateBQResource, upsert)
			assert.NotNil(t, err)
		})
	})
	t.Run("createStandardView", func(t *testing.T) {
		t.Run("should create view if given valid input", func(t *testing.T) {
			upsert := false
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{},
			}
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)
			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), errNotFound)
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable, nil)
			bQTable.On("Create", testingContext, createTableMeta).Return(nil)

			err := createStandardView(testingContext, resourceSpec, bQClient, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return error if read BQ table spec is failed", func(t *testing.T) {
			upsert := false
			resourceSpec := models.ResourceSpec{
				Spec: "non bq view",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			err := createStandardView(testingContext, resourceSpec, bQClient, upsert)
			assert.NotNil(t, err)
		})
		t.Run("should return error if ensuring dataset is failed", func(t *testing.T) {
			upsert := false
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return((*bqiface.DatasetMetadata)(nil), errors.New("some error"))

			err := createStandardView(testingContext, resourceSpec, bQClient, upsert)
			assert.NotNil(t, err)
		})
	})
}
