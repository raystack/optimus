package bigquery_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/ext/store/bigquery"
	"github.com/raystack/optimus/internal/errors"
)

func TestBigqueryBackup(t *testing.T) {
	ctx := context.Background()
	store := resource.Bigquery
	tnnt, _ := tenant.NewTenant("project", "namespace")
	createdAt := time.Date(2022, 11, 18, 1, 0, 0, 0, time.UTC)
	meta := &resource.Metadata{}
	kindTable := bigquery.KindTable
	spec := map[string]any{
		"description": "test resource",
	}
	fullName := "t-optimus.playground.product"
	source, resErr := resource.NewResource(fullName, kindTable, store, tnnt, meta, spec)
	assert.NoError(t, resErr)

	t.Run("BackupResources", func(t *testing.T) {
		t.Run("skips backup when not table", func(t *testing.T) {
			client := new(mockClient)

			viewName := "t-optimus.playground.product-view"
			view, err := resource.NewResource(viewName, bigquery.KindView, store, tnnt, meta, spec)
			assert.NoError(t, err)

			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			result, err := bigquery.BackupResources(ctx, backup, []*resource.Resource{view}, client)
			assert.NoError(t, err)
			assert.Equal(t, 0, len(result.ResourceNames))
			ignoredResource := result.IgnoredResources[0]
			assert.Equal(t, "t-optimus.playground.product-view", ignoredResource.Name)
			assert.Equal(t, "backup not supported for view", ignoredResource.Reason)
		})
		t.Run("returns error when cannot get destination dataset", func(t *testing.T) {
			client := new(mockClient)
			config := map[string]string{"dataset": ""}
			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, config)
			assert.NoError(t, err)

			_, err = bigquery.BackupResources(ctx, backup, []*resource.Resource{source}, client)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "bigquery dataset name is empty")
		})
		t.Run("returns error when cannot create dataset", func(t *testing.T) {
			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Exists", ctx).Return(false)
			datasetHandle.On("Create", ctx, mock.Anything).Return(errors.InternalError("bq", "some error in create", nil))
			defer datasetHandle.AssertExpectations(t)

			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			client := new(mockClient)
			client.On("DatasetHandleFrom", mock.Anything).Return(datasetHandle)
			defer client.AssertExpectations(t)

			_, err = bigquery.BackupResources(ctx, backup, []*resource.Resource{source}, client)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "some error in create")
		})
		t.Run("returns error when error in backup table", func(t *testing.T) {
			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Exists", ctx).Return(true)
			defer datasetHandle.AssertExpectations(t)

			config := map[string]string{
				"ttl": "32P",
			}
			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, config)
			assert.NoError(t, err)

			client := new(mockClient)
			client.On("DatasetHandleFrom", mock.Anything).Return(datasetHandle)
			defer client.AssertExpectations(t)

			_, err = bigquery.BackupResources(ctx, backup, []*resource.Resource{source}, client)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "failed to parse bigquery backup TTL 32P")
		})
		t.Run("backs up the resources successfully", func(t *testing.T) {
			mockJob := new(mockCopyJob)
			mockJob.On("Wait", ctx).Return(nil)
			defer mockJob.AssertExpectations(t)

			mockCopier := new(mockTableCopier)
			mockCopier.On("Run", ctx).Return(mockJob, nil)
			defer mockCopier.AssertExpectations(t)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("CopierFrom", mock.Anything).Return(mockCopier, nil)
			tableHandle.On("UpdateExpiry", ctx, mock.Anything, mock.Anything).
				Return(nil)
			defer tableHandle.AssertExpectations(t)

			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Exists", ctx).Return(true)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", mock.Anything).Return(datasetHandle)
			client.On("TableHandleFrom", mock.Anything, mock.Anything).Return(tableHandle)
			defer client.AssertExpectations(t)

			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			result, err := bigquery.BackupResources(ctx, backup, []*resource.Resource{source}, client)
			assert.NoError(t, err)
			assert.Equal(t, 0, len(result.IgnoredResources))
			assert.Equal(t, "t-optimus.optimus_backup.backup_playground_product_2022_11_18_01_00_00", result.ResourceNames[0])
		})
	})
	t.Run("BackupTable", func(t *testing.T) {
		t.Run("returns error when cannot generate destination dataset", func(t *testing.T) {
			client := new(mockClient)
			config := map[string]string{"dataset": ""}
			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, config)
			assert.NoError(t, err)

			_, err = bigquery.BackupTable(ctx, backup, source, client)
			assert.Error(t, err)
		})
		t.Run("returns error when expiry time is invalid", func(t *testing.T) {
			client := new(mockClient)
			config := map[string]string{
				"ttl": "32P",
			}
			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, config)
			assert.NoError(t, err)

			_, err = bigquery.BackupTable(ctx, backup, source, client)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "failed to parse bigquery backup TTL 32P")
		})
		t.Run("returns error when error in copy table", func(t *testing.T) {
			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("CopierFrom", mock.Anything).Return(nil, errors.InternalError("bq", "cannot get copier", nil))
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("TableHandleFrom", mock.Anything, mock.Anything).Return(tableHandle)
			defer client.AssertExpectations(t)

			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			_, err = bigquery.BackupTable(ctx, backup, source, client)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "cannot get copier")
		})
		t.Run("returns error when not able to update expiry time", func(t *testing.T) {
			mockJob := new(mockCopyJob)
			mockJob.On("Wait", ctx).Return(nil)
			defer mockJob.AssertExpectations(t)

			mockCopier := new(mockTableCopier)
			mockCopier.On("Run", ctx).Return(mockJob, nil)
			defer mockCopier.AssertExpectations(t)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("CopierFrom", mock.Anything).Return(mockCopier, nil)
			tableHandle.On("UpdateExpiry", ctx, mock.Anything, mock.Anything).
				Return(errors.InternalError("bq", "failed to update", nil))
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("TableHandleFrom", mock.Anything, mock.Anything).Return(tableHandle)
			defer client.AssertExpectations(t)

			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			_, err = bigquery.BackupTable(ctx, backup, source, client)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "failed to update")
		})
		t.Run("returns name of resource backup", func(t *testing.T) {
			mockJob := new(mockCopyJob)
			mockJob.On("Wait", ctx).Return(nil)
			defer mockJob.AssertExpectations(t)

			mockCopier := new(mockTableCopier)
			mockCopier.On("Run", ctx).Return(mockJob, nil)
			defer mockCopier.AssertExpectations(t)

			tableHandle := new(mockTableResourceHandle)
			tableHandle.On("CopierFrom", mock.Anything).Return(mockCopier, nil)
			tableHandle.On("UpdateExpiry", ctx, mock.Anything, mock.Anything).
				Return(nil)
			defer tableHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("TableHandleFrom", mock.Anything, mock.Anything).Return(tableHandle)
			defer client.AssertExpectations(t)

			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			name, err := bigquery.BackupTable(ctx, backup, source, client)
			assert.NoError(t, err)
			assert.Equal(t, "t-optimus.optimus_backup.backup_playground_product_2022_11_18_01_00_00", name)
		})
	})
	t.Run("CreateIfDatasetDoesNotExist", func(t *testing.T) {
		destDataset := bigquery.Dataset{
			Project:     "t-optimus",
			DatasetName: "backup_optimus",
		}

		t.Run("return no error if exists", func(t *testing.T) {
			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Exists", ctx).Return(true)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", destDataset).Return(datasetHandle)

			err := bigquery.CreateIfDatasetDoesNotExist(ctx, client, destDataset)
			assert.NoError(t, err)
		})
		t.Run("returns no error when create returns already exists", func(t *testing.T) {
			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Exists", ctx).Return(false)
			datasetHandle.On("Create", ctx, mock.Anything).Return(errors.AlreadyExists("backup_optimus", "already"))
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", destDataset).Return(datasetHandle)

			err := bigquery.CreateIfDatasetDoesNotExist(ctx, client, destDataset)
			assert.NoError(t, err)
		})
		t.Run("return error if create returns error", func(t *testing.T) {
			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Exists", ctx).Return(false)
			datasetHandle.On("Create", ctx, mock.Anything).Return(errors.InternalError("bq", "some error", nil))
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", destDataset).Return(datasetHandle)

			err := bigquery.CreateIfDatasetDoesNotExist(ctx, client, destDataset)
			assert.Error(t, err)
		})
		t.Run("return no error when created", func(t *testing.T) {
			datasetHandle := new(mockTableResourceHandle)
			datasetHandle.On("Exists", ctx).Return(false)
			datasetHandle.On("Create", ctx, mock.Anything).Return(nil)
			defer datasetHandle.AssertExpectations(t)

			client := new(mockClient)
			client.On("DatasetHandleFrom", destDataset).Return(datasetHandle)

			err := bigquery.CreateIfDatasetDoesNotExist(ctx, client, destDataset)
			assert.NoError(t, err)
		})
	})
	t.Run("CopyTable", func(t *testing.T) {
		t.Run("returns error when cannot create copier", func(t *testing.T) {
			mockDest := new(mockTableResourceHandle)
			mockSource := new(mockTableResourceHandle)
			mockDest.On("CopierFrom", mockSource).Return(nil, errors.InternalError("bq", "some error", nil))
			defer mockDest.AssertExpectations(t)

			err := bigquery.CopyTable(ctx, mockSource, mockDest)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "some error")
		})
		t.Run("returns error when copier returns error", func(t *testing.T) {
			mockCopier := new(mockTableCopier)
			mockCopier.On("Run", ctx).Return(nil, errors.InternalError("bq", "error in job", nil))
			defer mockCopier.AssertExpectations(t)

			mockDest := new(mockTableResourceHandle)
			mockSource := new(mockTableResourceHandle)
			mockDest.On("CopierFrom", mockSource).Return(mockCopier, nil)
			defer mockDest.AssertExpectations(t)

			err := bigquery.CopyTable(ctx, mockSource, mockDest)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "error in job")
		})
		t.Run("returns when successful", func(t *testing.T) {
			mockJob := new(mockCopyJob)
			mockJob.On("Wait", ctx).Return(nil)
			defer mockJob.AssertExpectations(t)

			mockCopier := new(mockTableCopier)
			mockCopier.On("Run", ctx).Return(mockJob, nil)
			defer mockCopier.AssertExpectations(t)

			mockDest := new(mockTableResourceHandle)
			mockSource := new(mockTableResourceHandle)
			mockDest.On("CopierFrom", mockSource).Return(mockCopier, nil)
			defer mockDest.AssertExpectations(t)

			err := bigquery.CopyTable(ctx, mockSource, mockDest)
			assert.NoError(t, err)
		})
	})
	t.Run("DestinationDataset", func(t *testing.T) {
		t.Run("returns the dataset for destination", func(t *testing.T) {
			backup, err := resource.NewBackup(store, tnnt, []string{fullName}, "", createdAt, nil)
			assert.NoError(t, err)

			dataset, err := bigquery.DataSetFor(source)
			assert.NoError(t, err)

			destinationDS, err := bigquery.DestinationDataset(dataset.Project, backup)
			assert.NoError(t, err)
			assert.Equal(t, "t-optimus.optimus_backup", destinationDS.FullName())
		})
	})
	t.Run("DestinationName", func(t *testing.T) {
		t.Run("creates name for destination table", func(t *testing.T) {
			backup, err := resource.NewBackup(store, tnnt, []string{fullName}, "", createdAt, nil)
			assert.NoError(t, err)

			dataset, err := bigquery.DataSetFor(source)
			assert.NoError(t, err)

			name, err := bigquery.ResourceNameFor(source)
			assert.NoError(t, err)

			destination := bigquery.DestinationName(dataset.DatasetName, name, backup)
			assert.Equal(t, "backup_playground_product_2022_11_18_01_00_00", destination)
		})
	})
	t.Run("DestinationExpiry", func(t *testing.T) {
		t.Run("returns error when cannot parse ttl", func(t *testing.T) {
			config := map[string]string{
				"ttl": "32P",
			}
			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, config)
			assert.NoError(t, err)

			_, err = bigquery.DestinationExpiry(backup)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid argument for entity BigqueryStore: failed to parse bigquery backup TTL 32P")
		})
		t.Run("returns the time for backup expiration", func(t *testing.T) {
			backup, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
			assert.NoError(t, err)

			expiry, err := bigquery.DestinationExpiry(backup)
			assert.NoError(t, err)
			assert.Equal(t, "2022-12-18 01:00:00 +0000 UTC", expiry.String())
		})
	})
}

type mockTableCopier struct {
	mock.Mock
}

func (m *mockTableCopier) Run(ctx context.Context) (bigquery.CopyJob, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(bigquery.CopyJob), args.Error(1)
}

type mockCopyJob struct {
	mock.Mock
}

func (m *mockCopyJob) Wait(ctx context.Context) error {
	return m.Called(ctx).Error(0)
}
