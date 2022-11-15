package bigquery_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/store/bigquery"
)

func TestBigqueryStore(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("proj", "ns")
	bq := resource.Bigquery
	metadata := resource.Metadata{Description: "meta"}
	spec := map[string]any{"description": "resource"}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Create(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when not able to get client", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Create(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in client")
		})
		t.Run("returns error when kind is invalid", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset.name", "unknown", bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Create(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity BigqueryStore: invalid kind for bigquery resource unknown")
		})
		t.Run("calls appropriate handler for each dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockResourceHandle)
			datasetHandle.On("Create", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("DatasetHandleFrom", dataset).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, dataset)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockResourceHandle)
			datasetHandle.On("Create", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("DatasetHandleFrom", dataset).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, dataset)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for table", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			table, err := resource.NewResource("proj.dataset.table", resource.KindTable, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockResourceHandle)
			tableHandle.On("Create", mock.Anything, table).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("TableHandleFrom", table).Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, table)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for view", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			view, err := resource.NewResource("proj.dataset.view", resource.KindView, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			viewHandle := new(mockResourceHandle)
			viewHandle.On("Create", mock.Anything, view).Return(nil)
			defer viewHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("ViewHandleFrom", view).Return(viewHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, view)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for each dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			extTable, err := resource.NewResource("proj.dataset.extTable", resource.KindExternalTable, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			extTableHandle := new(mockResourceHandle)
			extTableHandle.On("Create", mock.Anything, extTable).Return(nil)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("ExternalTableHandleFrom", extTable).Return(extTableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Create(ctx, extTable)
			assert.Nil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Update(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when not able to get client", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Update(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in client")
		})
		t.Run("returns error when kind is invalid", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)
			client := new(mockClient)
			client.On("Close")
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset.name1", "unknown", bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.Update(ctx, dataset)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity BigqueryStore: invalid kind for bigquery resource unknown")
		})
		t.Run("calls appropriate handler for each dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockResourceHandle)
			datasetHandle.On("Update", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("DatasetHandleFrom", dataset).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, dataset)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockResourceHandle)
			datasetHandle.On("Update", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("DatasetHandleFrom", dataset).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, dataset)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for table", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			table, err := resource.NewResource("proj.dataset.table", resource.KindTable, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockResourceHandle)
			tableHandle.On("Update", mock.Anything, table).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("TableHandleFrom", table).Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, table)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for view", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			view, err := resource.NewResource("proj.dataset.view", resource.KindView, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			viewHandle := new(mockResourceHandle)
			viewHandle.On("Update", mock.Anything, view).Return(nil)
			defer viewHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("ViewHandleFrom", view).Return(viewHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, view)
			assert.Nil(t, err)
		})
		t.Run("calls appropriate handler for each dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			extTable, err := resource.NewResource("proj.dataset.extTable", resource.KindExternalTable, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			extTableHandle := new(mockResourceHandle)
			extTableHandle.On("Update", mock.Anything, extTable).Return(nil)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("ExternalTableHandleFrom", extTable).Return(extTableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, extTable)
			assert.Nil(t, err)
		})
	})
	t.Run("BatchUpdate", func(t *testing.T) {
		t.Run("returns error when cannot get secret", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = bqStore.BatchUpdate(ctx, []*resource.Resource{dataset})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when cannot create client", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)
			updateDS := resource.FromExisting(dataset, resource.ReplaceStatus(resource.StatusToUpdate))

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").
				Return(nil, errors.New("some error"))

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.BatchUpdate(ctx, []*resource.Resource{updateDS})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "some error")
		})
		t.Run("returns error when one or more job fails", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)
			updateDS := resource.FromExisting(dataset, resource.ReplaceStatus(resource.StatusToUpdate))

			datasetHandle := new(mockResourceHandle)
			datasetHandle.On("Update", mock.Anything, updateDS).Return(errors.New("failed to update"))
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", updateDS).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.BatchUpdate(ctx, []*resource.Resource{updateDS})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error while resource batch update:\n failed to update")
		})
		t.Run("returns no error when successfully updates", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)
			updateDS := resource.FromExisting(dataset, resource.ReplaceStatus(resource.StatusToUpdate))

			datasetHandle := new(mockResourceHandle)
			datasetHandle.On("Update", mock.Anything, updateDS).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", updateDS).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.BatchUpdate(ctx, []*resource.Resource{updateDS})
			assert.Nil(t, err)
		})
	})
	t.Run("Exist", func(t *testing.T) {
		t.Run("returns error when secret is not provided", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			exist, err := bqStore.Exist(ctx, dataset)
			assert.False(t, exist)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when not able to get client", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			exist, err := bqStore.Exist(ctx, dataset)
			assert.False(t, exist)
			assert.EqualError(t, err, "error in client")
		})
		t.Run("returns error when kind is invalid", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset.name", "unknown", bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			exist, err := bqStore.Exist(ctx, dataset)
			assert.False(t, exist)
			assert.EqualError(t, err, "invalid argument for entity BigqueryStore: invalid kind for bigquery resource unknown")
		})
		t.Run("calls appropriate handler for each dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockResourceHandle)
			datasetHandle.On("Exists", mock.Anything).Return(true)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("DatasetHandleFrom", dataset).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			exist, err := bqStore.Exist(ctx, dataset)
			assert.True(t, exist)
			assert.NoError(t, err)
		})
		t.Run("calls appropriate handler for dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockResourceHandle)
			datasetHandle.On("Exists", mock.Anything).Return(true)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("DatasetHandleFrom", dataset).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			exist, err := bqStore.Exist(ctx, dataset)
			assert.True(t, exist)
			assert.NoError(t, err)
		})
		t.Run("calls appropriate handler for table", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			table, err := resource.NewResource("proj.dataset.table", resource.KindTable, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockResourceHandle)
			tableHandle.On("Exists", mock.Anything).Return(true)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("TableHandleFrom", table).Return(tableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			exist, err := bqStore.Exist(ctx, table)
			assert.True(t, exist)
			assert.NoError(t, err)
		})
		t.Run("calls appropriate handler for view", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			view, err := resource.NewResource("proj.dataset.view", resource.KindView, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			viewHandle := new(mockResourceHandle)
			viewHandle.On("Exists", mock.Anything).Return(true)
			defer viewHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("ViewHandleFrom", view).Return(viewHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			exist, err := bqStore.Exist(ctx, view)
			assert.True(t, exist)
			assert.NoError(t, err)
		})
		t.Run("calls appropriate handler for each dataset", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			extTable, err := resource.NewResource("proj.dataset.extTable", resource.KindExternalTable, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			extTableHandle := new(mockResourceHandle)
			extTableHandle.On("Exists", mock.Anything).Return(true)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("ExternalTableHandleFrom", extTable).Return(extTableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			exist, err := bqStore.Exist(ctx, extTable)
			assert.True(t, exist)
			assert.NoError(t, err)
		})
	})
}

type mockClientProvider struct {
	mock.Mock
}

func (m *mockClientProvider) Get(ctx context.Context, account string) (bigquery.Client, error) {
	args := m.Called(ctx, account)
	if args.Get(0) != nil {
		return args.Get(0).(bigquery.Client), args.Error(1)
	}
	return nil, args.Error(1)
}

type mockSecretProvider struct {
	mock.Mock
}

func (s *mockSecretProvider) GetSecret(ctx context.Context, ten tenant.Tenant, name string) (*tenant.PlainTextSecret, error) {
	args := s.Called(ctx, ten, name)
	var pts *tenant.PlainTextSecret
	if args.Get(0) != nil {
		pts = args.Get(0).(*tenant.PlainTextSecret)
	}
	return pts, args.Error(1)
}

type mockClient struct {
	mock.Mock
}

func (m *mockClient) DatasetHandleFrom(res *resource.Resource) bigquery.ResourceHandle {
	args := m.Called(res)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) ExternalTableHandleFrom(res *resource.Resource) bigquery.ResourceHandle {
	args := m.Called(res)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) TableHandleFrom(res *resource.Resource) bigquery.ResourceHandle {
	args := m.Called(res)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) ViewHandleFrom(res *resource.Resource) bigquery.ResourceHandle {
	args := m.Called(res)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) Close() {
	m.Called()
}

type mockResourceHandle struct {
	mock.Mock
}

func (m *mockResourceHandle) Create(ctx context.Context, res *resource.Resource) error {
	args := m.Called(ctx, res)
	return args.Error(0)
}

func (m *mockResourceHandle) Update(ctx context.Context, res *resource.Resource) error {
	args := m.Called(ctx, res)
	return args.Error(0)
}

func (m *mockResourceHandle) Exists(ctx context.Context) bool {
	args := m.Called(ctx)
	return args.Get(0).(bool)
}
