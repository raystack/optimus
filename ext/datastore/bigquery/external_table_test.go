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

func TestExternalTable(t *testing.T) {
	testingContext := context.Background()
	testingProject := "project"
	testingDataset := "dataset"
	testingTable := "external_table"
	errNotFound := &googleapi.Error{
		Code: 404,
	}
	testingSource := &BQExternalSource{
		SourceType: string(ExternalTableTypeGoogleSheets),
		SourceURIs: []string{"http://googlesheets.com/1234"},
		Config:     map[string]interface{}{"skip_leading_rows": 1.0, "range": "A!:A1:B1"},
	}
	bQTableMetadata := BQTableMetadata{
		Source: testingSource,
	}
	bQResource := BQTable{
		Project:  testingProject,
		Dataset:  testingDataset,
		Table:    testingTable,
		Metadata: bQTableMetadata,
	}
	createTableMeta := &bigquery.TableMetadata{
		Name: testingTable,
		ExternalDataConfig: &bigquery.ExternalDataConfig{
			SourceFormat: bigquery.GoogleSheets,
			SourceURIs:   []string{"http://googlesheets.com/1234"},
			Options: &bigquery.GoogleSheetsOptions{
				SkipLeadingRows: 1,
				Range:           "A!:A1:B1",
			},
		},
	}
	t.Run("ensureExternalTable", func(t *testing.T) {
		t.Run("should create external table if it does not exist", func(t *testing.T) {
			upsert := false

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), errNotFound)
			bQTable.On("Create", testingContext, createTableMeta).Return(nil)

			err := ensureExternalTable(testingContext, bQTable, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should not do insert nor update if external table is exist and not an upsert call", func(t *testing.T) {
			upsert := false

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), nil)

			err := ensureExternalTable(testingContext, bQTable, bQResource, upsert)
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

				err := ensureExternalTable(testingContext, bQTable, bQResource, upsert)
				assert.Equal(t, e, err)
			}
		})
		t.Run("should update external table if it is already exist and an upsert call", func(t *testing.T) {
			upsert := true
			description := "table description"
			updatedBQTableMetadata := BQTableMetadata{
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
			}
			tableMeta := &bigquery.TableMetadata{
				ETag: "etag-0000",
			}

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return(tableMeta, nil)
			bQTable.On("Update", testingContext, updateTableMeta, tableMeta.ETag).Return((*bigquery.TableMetadata)(nil), nil)

			err := ensureExternalTable(testingContext, bQTable, updateBQResource, upsert)
			assert.Nil(t, err)
		})
	})
	t.Run("createExternalTable", func(t *testing.T) {
		t.Run("should create external table if given valid input", func(t *testing.T) {
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

			err := createExternalTable(testingContext, resourceSpec, bQClient, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return error if read BQ table spec is failed", func(t *testing.T) {
			upsert := false
			resourceSpec := models.ResourceSpec{
				Spec: "non bq table",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			err := createExternalTable(testingContext, resourceSpec, bQClient, upsert)
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

			err := createExternalTable(testingContext, resourceSpec, bQClient, upsert)
			assert.NotNil(t, err)
		})
	})
}
