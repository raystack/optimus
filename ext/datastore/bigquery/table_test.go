package bigquery //nolint: testpackage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
)

func TestTable(t *testing.T) {
	testingContext := context.Background()
	testingProject := "project"
	testingDataset := "dataset"
	testingTable := "table"
	errNotFound := &googleapi.Error{
		Code: 404,
	}
	bQTableMetadata := BQTableMetadata{
		Schema: BQSchema{
			{
				Name:   "message",
				Type:   "STRING",
				Mode:   "nullable",
				Schema: BQSchema{},
			},
			{
				Name:   "message_type",
				Type:   "STRING",
				Mode:   "nullable",
				Schema: BQSchema{},
			},
			{
				Name:   "recipient",
				Type:   "STRING",
				Mode:   "repeated",
				Schema: BQSchema{},
			},
			{
				Name:   "time",
				Type:   "TIME",
				Mode:   "nullable",
				Schema: BQSchema{},
			},
		},
		Cluster: &BQClusteringInfo{
			Using: []string{"message_type"},
		},
	}
	bQResource := BQTable{
		Project:  testingProject,
		Dataset:  testingDataset,
		Table:    testingTable,
		Metadata: bQTableMetadata,
	}
	createTableMeta := &bigquery.TableMetadata{
		Name: bQResource.Table,
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
	}

	t.Run("ensureTable", func(t *testing.T) {
		t.Run("should create table if it does not exist", func(t *testing.T) {
			upsert := false

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), errNotFound)
			bQTable.On("Create", testingContext, createTableMeta).Return(nil)

			err := ensureTable(testingContext, bQTable, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should not do insert nor update if table is exist and not an upsert call", func(t *testing.T) {
			upsert := false

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return(createTableMeta, nil)

			err := ensureTable(testingContext, bQTable, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return any error encountered, except for an *googleapi.Error{Code: 404}", func(t *testing.T) {
			otherErrors := []error{
				&googleapi.Error{Code: 401},
				errors.New("unexpected"),
			}
			upsert := false
			for _, e := range otherErrors {
				bQTable := new(BqTableMock)

				bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), e)
				err := ensureTable(testingContext, bQTable, bQResource, upsert)
				assert.Equal(t, e, err)
				bQTable.AssertExpectations(t)
			}
		})
		t.Run("should update table if is already exist and an upsert call", func(t *testing.T) {
			upsert := true

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			tableMeta := &bigquery.TableMetadata{
				ETag: "etag-0000",
			}
			updateTableMeta := bigquery.TableMetadataToUpdate{
				Name:   bQResource.Table,
				Schema: createTableMeta.Schema,
			}

			bQTable.On("Metadata", testingContext).Return(tableMeta, nil)
			bQTable.On("Update", testingContext, updateTableMeta, tableMeta.ETag).Return(tableMeta, nil)

			err := ensureTable(testingContext, bQTable, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return an error if bigquery field specification is invalid (on create)", func(t *testing.T) {
			upsert := false
			invalidTable := BQTable{
				Project: testingProject,
				Dataset: testingDataset,
				Table:   testingTable,
				Metadata: BQTableMetadata{
					Schema: BQSchema{
						{
							Name: "recipient",
							Type: "STRING",
							Mode: "abcd",
						},
					},
				},
			}

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), errNotFound)

			err := ensureTable(testingContext, bQTable, invalidTable, upsert)
			assert.NotNil(t, err)
		})
		t.Run("should return an error if bigquery field specification is invalid (on update)", func(t *testing.T) {
			upsert := true
			invalidTable := BQTable{
				Project: testingProject,
				Dataset: testingDataset,
				Table:   testingTable,
				Metadata: BQTableMetadata{
					Schema: BQSchema{
						{
							Name: "recipient",
							Type: "STRING",
							Mode: "abcd",
						},
					},
				},
			}

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), nil)

			err := ensureTable(testingContext, bQTable, invalidTable, upsert)
			assert.NotNil(t, err)
		})
	})

	t.Run("createTable", func(t *testing.T) {
		t.Run("should create table if given valid input", func(t *testing.T) {
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
			eTag := "uniqueID"
			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{
					ETag: eTag,
				},
			}
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)
			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), errNotFound)
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable, nil)
			bQTable.On("Create", testingContext, createTableMeta).Return(nil)

			err := createTable(testingContext, resourceSpec, bQClient, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return error if read BQ table spec is failed", func(t *testing.T) {
			upsert := false
			resourceSpec := models.ResourceSpec{
				Spec: "non bq table",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			err := createTable(testingContext, resourceSpec, bQClient, upsert)
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

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return((*bqiface.DatasetMetadata)(nil), errors.New("some error"))

			err := createTable(testingContext, resourceSpec, bQClient, upsert)
			assert.NotNil(t, err)
		})
	})

	t.Run("getTable", func(t *testing.T) {
		t.Run("should retrieve BQ table information", func(t *testing.T) {
			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}
			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{},
			}

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)
			bQTable.On("Metadata", testingContext).Return(createTableMeta, nil)
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable, nil)

			actualResourceSpec, err := getTable(testingContext, resourceSpec, bQClient)
			assert.Equal(t, resourceSpec, actualResourceSpec)
			assert.Nil(t, err)
		})
		t.Run("should return error if read BQ table spec is failed", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: "non bq table",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			actualResourceSpec, err := getTable(testingContext, resourceSpec, bQClient)
			assert.Equal(t, models.ResourceSpec{}, actualResourceSpec)
			assert.NotNil(t, err)
		})
	})

	t.Run("deleteTable", func(t *testing.T) {
		t.Run("should able to delete table if given bq table", func(t *testing.T) {
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
			bQDatasetHandle.On("Metadata", testingContext).Return(&bqiface.DatasetMetadata{}, nil)
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable, nil)
			bQTable.On("Delete", testingContext).Return(nil)

			err := deleteTable(testingContext, resourceSpec, bQClient)
			assert.Nil(t, err)
		})
		t.Run("should return error if read BQ table spec is failed", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: "non bq table",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			err := deleteTable(testingContext, resourceSpec, bQClient)
			assert.NotNil(t, err)
		})
		t.Run("should return error if checking dataset metadata is failed", func(t *testing.T) {
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
			bQDatasetHandle.On("Metadata", testingContext).Return(&bqiface.DatasetMetadata{}, errors.New("some error"))

			err := deleteTable(testingContext, resourceSpec, bQClient)
			assert.NotNil(t, err)
		})
	})

	t.Run("backupTable", func(t *testing.T) {
		eTag := "uniqueID"
		backupTime := time.Now()
		tableMetadata := &bigquery.TableMetadata{
			Name: bQResource.Table,
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
			Spec: bQResource,
			Type: models.ResourceTypeTable,
		}
		destinationConfig := map[string]string{
			models.ConfigTTL:    "720h",
			BackupConfigDataset: "optimus_backup",
			BackupConfigPrefix:  "backup",
		}
		request := models.BackupResourceRequest{
			Resource: resourceSpec,
			BackupSpec: models.BackupRequest{
				ID:     uuid.Must(uuid.NewRandom()),
				Config: destinationConfig,
			},
			BackupTime: backupTime,
		}
		destinationTable := BQTable{
			Project: bQResource.Project,
			Dataset: request.BackupSpec.Config[BackupConfigDataset],
			Table:   fmt.Sprintf("backup_dataset_table_%s", backupTime.Format(backupTimePostfixFormat)),
		}

		datasetMetadata := bqiface.DatasetMetadata{
			DatasetMetadata: bigquery.DatasetMetadata{
				ETag: eTag,
			},
		}
		toUpdate := bigquery.TableMetadataToUpdate{
			ExpirationTime: request.BackupTime.Add(time.Hour * 24 * 30),
		}
		resultURN := fmt.Sprintf(tableURNFormat, BigQuery{}.Name(), destinationTable.Project, destinationTable.Dataset, destinationTable.Table)
		t.Run("should able to backup table if given valid input", func(t *testing.T) {
			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQCopier := new(BqCopierMock)
			defer bQCopier.AssertExpectations(t)

			bQJob := new(BqJobMock)
			defer bQJob.AssertExpectations(t)

			// duplicate table
			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle).Once()
			bQClient.On("DatasetInProject", destinationTable.Project, destinationTable.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable)
			bQDatasetHandle.On("Table", destinationTable.Table).Return(bQTable)
			bQTable.On("CopierFrom", []bqiface.Table{bQTable}).Return(bQCopier)
			bQCopier.On("Run", testingContext).Return(bQJob, nil)
			bQJob.On("Wait", testingContext).Return(&bigquery.JobStatus{}, nil)

			// update expiry
			bQTable.On("Metadata", testingContext).Return(tableMetadata, nil).Once()
			bQTable.On("Update", testingContext, toUpdate, eTag).Return(tableMetadata, nil)

			// verify
			bQTable.On("Metadata", testingContext).Return(tableMetadata, nil).Once()

			resp, err := backupTable(testingContext, request, bQClient)

			assert.Nil(t, err)
			assert.Equal(t, resultURN, resp.ResultURN)
			assert.Equal(t, destinationTable, resp.ResultSpec)
		})
		t.Run("should fail when unable to read resource spec", func(t *testing.T) {
			invalidResourceSpec := models.ResourceSpec{
				Spec: "invalid spec",
				Type: models.ResourceTypeTable,
			}
			invalidRequest := models.BackupResourceRequest{
				Resource: invalidResourceSpec,
				BackupSpec: models.BackupRequest{
					ID:     uuid.Must(uuid.NewRandom()),
					Config: destinationConfig,
				},
				BackupTime: time.Now(),
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			resp, err := backupTable(testingContext, invalidRequest, bQClient)

			assert.Equal(t, errorReadTableSpec, err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should fail when destination dataset is not available and cannot be created", func(t *testing.T) {
			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			errorMsg := "unable to get dataset metadata"

			// duplicate table
			bQClient.On("DatasetInProject", destinationTable.Project, destinationTable.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&bqiface.DatasetMetadata{}, errors.New(errorMsg))
			resp, err := backupTable(testingContext, request, bQClient)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should fail when unable to get source dataset metadata", func(t *testing.T) {
			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			errorMsg := "unable to get dataset metadata"

			// duplicate table
			bQClient.On("DatasetInProject", destinationTable.Project, destinationTable.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, errors.New(errorMsg)).Once()

			resp, err := backupTable(testingContext, request, bQClient)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should fail when unable to copy source table", func(t *testing.T) {
			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQCopier := new(BqCopierMock)
			defer bQCopier.AssertExpectations(t)

			bQJob := new(BqJobMock)
			defer bQJob.AssertExpectations(t)

			errorMsg := "unable to copy table"

			// duplicate table
			bQClient.On("DatasetInProject", destinationTable.Project, destinationTable.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable)
			bQDatasetHandle.On("Table", destinationTable.Table).Return(bQTable)
			bQTable.On("CopierFrom", []bqiface.Table{bQTable}).Return(bQCopier)
			bQCopier.On("Run", testingContext).Return(bQJob, errors.New(errorMsg))

			resp, err := backupTable(testingContext, request, bQClient)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should fail when unable to get status of copy table process", func(t *testing.T) {
			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQCopier := new(BqCopierMock)
			defer bQCopier.AssertExpectations(t)

			bQJob := new(BqJobMock)
			defer bQJob.AssertExpectations(t)

			errorMsg := "unable to get status of copy table"

			// duplicate table
			bQClient.On("DatasetInProject", destinationTable.Project, destinationTable.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable)
			bQDatasetHandle.On("Table", destinationTable.Table).Return(bQTable)
			bQTable.On("CopierFrom", []bqiface.Table{bQTable}).Return(bQCopier)
			bQCopier.On("Run", testingContext).Return(bQJob, nil)
			bQJob.On("Wait", testingContext).Return(&bigquery.JobStatus{}, errors.New(errorMsg))

			resp, err := backupTable(testingContext, request, bQClient)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should fail when unable to get metadata of the backup table", func(t *testing.T) {
			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQCopier := new(BqCopierMock)
			defer bQCopier.AssertExpectations(t)

			bQJob := new(BqJobMock)
			defer bQJob.AssertExpectations(t)

			errorMsg := "unable to get metadata of backup table"

			// duplicate table
			bQClient.On("DatasetInProject", destinationTable.Project, destinationTable.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable)
			bQDatasetHandle.On("Table", destinationTable.Table).Return(bQTable)
			bQTable.On("CopierFrom", []bqiface.Table{bQTable}).Return(bQCopier)
			bQCopier.On("Run", testingContext).Return(bQJob, nil)
			bQJob.On("Wait", testingContext).Return(&bigquery.JobStatus{}, nil)

			// update expiry
			bQTable.On("Metadata", testingContext).Return(tableMetadata, errors.New(errorMsg)).Once()

			resp, err := backupTable(testingContext, request, bQClient)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should fail when unable to update expiration of the backup table", func(t *testing.T) {
			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQCopier := new(BqCopierMock)
			defer bQCopier.AssertExpectations(t)

			bQJob := new(BqJobMock)
			defer bQJob.AssertExpectations(t)

			errorMsg := "unable to update expiration of backup table"

			// duplicate table
			bQClient.On("DatasetInProject", destinationTable.Project, destinationTable.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable)
			bQDatasetHandle.On("Table", destinationTable.Table).Return(bQTable)
			bQTable.On("CopierFrom", []bqiface.Table{bQTable}).Return(bQCopier)
			bQCopier.On("Run", testingContext).Return(bQJob, nil)
			bQJob.On("Wait", testingContext).Return(&bigquery.JobStatus{}, nil)

			// update expiry
			bQTable.On("Metadata", testingContext).Return(tableMetadata, nil).Once()
			bQTable.On("Update", testingContext, toUpdate, eTag).Return(tableMetadata, errors.New(errorMsg))

			resp, err := backupTable(testingContext, request, bQClient)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
		t.Run("should fail when unable to ensure the backup table", func(t *testing.T) {
			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQCopier := new(BqCopierMock)
			defer bQCopier.AssertExpectations(t)

			bQJob := new(BqJobMock)
			defer bQJob.AssertExpectations(t)

			errorMsg := "unable to ensure the backup table"

			// duplicate table
			bQClient.On("DatasetInProject", destinationTable.Project, destinationTable.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle).Once()
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil).Once()
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable)
			bQDatasetHandle.On("Table", destinationTable.Table).Return(bQTable)
			bQTable.On("CopierFrom", []bqiface.Table{bQTable}).Return(bQCopier)
			bQCopier.On("Run", testingContext).Return(bQJob, nil)
			bQJob.On("Wait", testingContext).Return(&bigquery.JobStatus{}, nil)

			// update expiry
			bQTable.On("Metadata", testingContext).Return(tableMetadata, nil).Once()
			bQTable.On("Update", testingContext, toUpdate, eTag).Return(tableMetadata, nil)

			// verify
			bQTable.On("Metadata", testingContext).Return(tableMetadata, errors.New(errorMsg)).Once()

			resp, err := backupTable(testingContext, request, bQClient)

			assert.Equal(t, errorMsg, err.Error())
			assert.Equal(t, models.BackupResourceResponse{}, resp)
		})
	})
}
