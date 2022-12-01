package bigquery_test

import (
	"context"
	"errors"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/store/bigquery"
)

func TestBigqueryStore(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("proj", "ns")
	store := resource.Bigquery
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
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

			dataset, err := resource.NewResource("proj.dataset.name", "unknown", store, tnnt, &metadata, spec)
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Create", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("DatasetHandleFrom", dataset.Dataset()).Return(datasetHandle)
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Create", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("DatasetHandleFrom", dataset.Dataset()).Return(datasetHandle)
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

			table, err := resource.NewResource("proj.dataset.table", resource.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Create", mock.Anything, table).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("TableHandleFrom", table.Dataset(), table.Name()).Return(tableHandle)
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

			view, err := resource.NewResource("proj.dataset.view", resource.KindView, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			viewHandle := new(mockTableResourceHandle)
			viewHandle.On("Create", mock.Anything, view).Return(nil)
			defer viewHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("ViewHandleFrom", view.Dataset(), view.Name()).Return(viewHandle)
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

			extTable, err := resource.NewResource("proj.dataset.extTable", resource.KindExternalTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			extTableHandle := new(mockTableResourceHandle)
			extTableHandle.On("Create", mock.Anything, extTable).Return(nil)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("ExternalTableHandleFrom", extTable.Dataset(), extTable.Name()).Return(extTableHandle)
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
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

			dataset, err := resource.NewResource("proj.dataset.name1", "unknown", store, tnnt, &metadata, spec)
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Update", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("DatasetHandleFrom", dataset.Dataset()).Return(datasetHandle)
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Update", mock.Anything, dataset).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("DatasetHandleFrom", dataset.Dataset()).Return(datasetHandle)
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

			table, err := resource.NewResource("proj.dataset.table", resource.KindTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("Update", mock.Anything, table).Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("TableHandleFrom", table.Dataset(), table.Name()).Return(tableHandle)
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

			view, err := resource.NewResource("proj.dataset.view", resource.KindView, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			viewHandle := new(mockTableResourceHandle)
			viewHandle.On("Update", mock.Anything, view).Return(nil)
			defer viewHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("ViewHandleFrom", view.Dataset(), view.Name()).Return(viewHandle)
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

			extTable, err := resource.NewResource("proj.dataset.extTable", resource.KindExternalTable, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			extTableHandle := new(mockTableResourceHandle)
			extTableHandle.On("Update", mock.Anything, extTable).Return(nil)
			defer extTableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("Close")
			client.On("ExternalTableHandleFrom", extTable.Dataset(), extTable.Name()).Return(extTableHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.Update(ctx, extTable)
			assert.Nil(t, err)
		})
	})
	t.Run("BatchUpdate", func(t *testing.T) {
		t.Run("returns no error when empty list", func(t *testing.T) {
			bqStore := bigquery.NewBigqueryDataStore(nil, nil)

			err := bqStore.BatchUpdate(ctx, []*resource.Resource{})
			assert.NoError(t, err)
		})
		t.Run("returns error when cannot get secret", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)
			updateDS := resource.FromExisting(dataset, resource.ReplaceStatus(resource.StatusToUpdate))

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Update", mock.Anything, updateDS).Return(errors.New("failed to update"))
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", updateDS.Dataset()).Return(datasetHandle)
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)
			updateDS := resource.FromExisting(dataset, resource.ReplaceStatus(resource.StatusToUpdate))

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Update", mock.Anything, updateDS).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", updateDS.Dataset()).Return(datasetHandle)
			defer client.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(client, nil)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			err = bqStore.BatchUpdate(ctx, []*resource.Resource{updateDS})
			assert.Nil(t, err)
		})
	})
	t.Run("Backup", func(t *testing.T) {
		createdAt := time.Date(2022, 11, 18, 1, 0, 0, 0, time.UTC)
		backup, backupErr := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
		assert.NoError(t, backupErr)

		t.Run("returns error when cannot get secret", func(t *testing.T) {
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(nil, errors.New("not found secret"))
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			_, err = bqStore.Backup(ctx, backup, []*resource.Resource{dataset})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found secret")
		})
		t.Run("returns error when cannot create client", func(t *testing.T) {
			pts, _ := tenant.NewPlainTextSecret("secret_name", "secret_value")
			secretProvider := new(mockSecretProvider)
			secretProvider.On("GetSecret", mock.Anything, tnnt, "DATASTORE_BIGQUERY").
				Return(pts, nil)
			defer secretProvider.AssertExpectations(t)

			clientProvider := new(mockClientProvider)
			clientProvider.On("Get", mock.Anything, "secret_value").Return(nil, errors.New("error in client"))
			defer clientProvider.AssertExpectations(t)

			bqStore := bigquery.NewBigqueryDataStore(secretProvider, clientProvider)

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			_, err = bqStore.Backup(ctx, backup, []*resource.Resource{dataset})
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in client")
		})
		t.Run("calls backup resources to backup the resources", func(t *testing.T) {
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

			dataset, err := resource.NewResource("proj.dataset", resource.KindDataset, store, tnnt, &metadata, spec)
			assert.Nil(t, err)

			result, err := bqStore.Backup(ctx, backup, []*resource.Resource{dataset})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(result.IgnoredResources))
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

func (m *mockClient) DatasetHandleFrom(dataset resource.Dataset) bigquery.ResourceHandle {
	args := m.Called(dataset)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) ExternalTableHandleFrom(ds resource.Dataset, name resource.Name) bigquery.ResourceHandle {
	args := m.Called(ds, name)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) TableHandleFrom(ds resource.Dataset, name resource.Name) bigquery.TableResourceHandle {
	args := m.Called(ds, name)
	return args.Get(0).(bigquery.TableResourceHandle)
}

func (m *mockClient) ViewHandleFrom(ds resource.Dataset, name resource.Name) bigquery.ResourceHandle {
	args := m.Called(ds, name)
	return args.Get(0).(bigquery.ResourceHandle)
}

func (m *mockClient) Close() {
	m.Called()
}

type mockTableResourceHandle struct {
	mock.Mock
}

func (m *mockTableResourceHandle) Create(ctx context.Context, res *resource.Resource) error {
	args := m.Called(ctx, res)
	return args.Error(0)
}

func (m *mockTableResourceHandle) Update(ctx context.Context, res *resource.Resource) error {
	args := m.Called(ctx, res)
	return args.Error(0)
}

func (m *mockTableResourceHandle) Exists(ctx context.Context) bool {
	args := m.Called(ctx)
	return args.Get(0).(bool)
}

func (m *mockTableResourceHandle) GetBQTable() (*bq.Table, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*bq.Table), args.Error(1)
}

func (m *mockTableResourceHandle) GetCopier(destination bigquery.TableResourceHandle) (bigquery.TableCopier, error) {
	args := m.Called(destination)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(bigquery.TableCopier), args.Error(1)
}

func (m *mockTableResourceHandle) UpdateExpiry(ctx context.Context, name string, expiry time.Time) error {
	args := m.Called(ctx, name, expiry)
	return args.Error(0)
}
