package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/resource/service"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

func TestBackupService(t *testing.T) {
	ctx := context.Background()
	store := resource.Bigquery
	tnnt, _ := tenant.NewTenant("project", "namespace")
	createdAt := time.Date(2022, 11, 18, 1, 0, 0, 0, time.UTC)
	backup, _ := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "", createdAt, nil)
	meta := &resource.Metadata{}
	spec := map[string]any{"description": "resource table"}
	source, resErr := resource.NewResource("p.d.t", "table", store, tnnt, meta, spec)
	assert.NoError(t, resErr)

	validID := "dda7b864-4268-4107-a096-dcf5343a0959"
	id, _ := uuid.Parse(validID)

	logger := log.NewLogrus()

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when cannot get resources", func(t *testing.T) {
			resourceProvider := new(mockResourceProvider)
			resourceProvider.On("GetResources", ctx, tnnt, store, []string{"p.d.t"}).
				Return(nil, errors.InternalError("repo", "cannot get", nil))
			defer resourceProvider.AssertExpectations(t)

			backupService := service.NewBackupService(nil, resourceProvider, nil, logger)
			_, err := backupService.Create(ctx, backup)
			assert.Error(t, err)
			assert.EqualError(t, err, "internal error for entity repo: cannot get")
		})
		t.Run("returns error when backup manager returns error", func(t *testing.T) {
			resources := []*resource.Resource{source}
			resourceProvider := new(mockResourceProvider)
			resourceProvider.On("GetResources", ctx, tnnt, store, []string{"p.d.t"}).
				Return(resources, nil)
			defer resourceProvider.AssertExpectations(t)

			backupManager := new(mockBackupManager)
			backupManager.On("Backup", ctx, backup, resources).
				Return(nil, errors.InternalError("bq", "something wrong", nil))
			defer backupManager.AssertExpectations(t)

			backupService := service.NewBackupService(nil, resourceProvider, backupManager, logger)
			_, err := backupService.Create(ctx, backup)
			assert.Error(t, err)
			assert.EqualError(t, err, "internal error for entity bq: something wrong")
		})
		t.Run("returns error when cannot create in db", func(t *testing.T) {
			resources := []*resource.Resource{source}
			resourceProvider := new(mockResourceProvider)
			resourceProvider.On("GetResources", ctx, tnnt, store, []string{"p.d.t"}).
				Return(resources, nil)
			defer resourceProvider.AssertExpectations(t)

			backupManager := new(mockBackupManager)
			backupManager.On("Backup", ctx, backup, resources).
				Return(&resource.BackupResult{ResourceNames: []string{"p.d.t"}}, nil)
			defer backupManager.AssertExpectations(t)

			repo := new(mockBackupRepo)
			repo.On("Create", ctx, backup).Return(errors.InternalError("repo", "cannot save", nil))
			defer repo.AssertExpectations(t)

			backupService := service.NewBackupService(repo, resourceProvider, backupManager, logger)
			_, err := backupService.Create(ctx, backup)
			assert.Error(t, err)
			assert.EqualError(t, err, "internal error for entity repo: cannot save")
		})
		t.Run("returns success when backup is done", func(t *testing.T) {
			resources := []*resource.Resource{source}
			resourceProvider := new(mockResourceProvider)
			resourceProvider.On("GetResources", ctx, tnnt, store, []string{"p.d.t"}).
				Return(resources, nil)
			defer resourceProvider.AssertExpectations(t)

			backupManager := new(mockBackupManager)
			backupManager.On("Backup", ctx, backup, resources).
				Return(&resource.BackupResult{ResourceNames: []string{"p.d.t"}}, nil)
			defer backupManager.AssertExpectations(t)

			repo := new(mockBackupRepo)
			repo.On("Create", ctx, backup).Return(nil)
			defer repo.AssertExpectations(t)

			backupService := service.NewBackupService(repo, resourceProvider, backupManager, logger)
			result, err := backupService.Create(ctx, backup)
			assert.NoError(t, err)
			assert.Equal(t, "p.d.t", result.ResourceNames[0])
		})
		t.Run("returns list of ignored resources", func(t *testing.T) {
			resources := []*resource.Resource{source}
			resourceProvider := new(mockResourceProvider)
			resourceNames := []string{"p.d.t", "p.d.t1", "p.d.t2"}
			resourceProvider.On("GetResources", ctx, tnnt, store, resourceNames).
				Return(resources, nil)
			defer resourceProvider.AssertExpectations(t)

			bk1, _ := resource.NewBackup(store, tnnt, resourceNames, "", createdAt, nil)

			backupManager := new(mockBackupManager)
			backupManager.On("Backup", ctx, bk1, resources).
				Return(&resource.BackupResult{ResourceNames: []string{"p.d.t"}}, nil)
			defer backupManager.AssertExpectations(t)

			repo := new(mockBackupRepo)
			repo.On("Create", ctx, bk1).Return(nil)
			defer repo.AssertExpectations(t)

			backupService := service.NewBackupService(repo, resourceProvider, backupManager, logger)
			result, err := backupService.Create(ctx, bk1)
			assert.NoError(t, err)
			assert.Equal(t, "p.d.t", result.ResourceNames[0])
			assert.Equal(t, "p.d.t1", result.IgnoredResources[0].Name)
			assert.Equal(t, "no resource found in namespace", result.IgnoredResources[0].Reason)
			assert.Equal(t, "p.d.t2", result.IgnoredResources[1].Name)
		})
	})
	t.Run("GetByID", func(t *testing.T) {
		t.Run("returns error when id is invalid", func(t *testing.T) {
			id := uuid.Nil
			backupID := resource.BackupID(id)

			backupService := service.NewBackupService(nil, nil, nil, logger)
			_, err := backupService.Get(ctx, backupID)
			assert.Error(t, err)
			assert.EqualError(t, err, "invalid argument for entity backup: the backup id is not valid")
		})
		t.Run("returns the backup by id", func(t *testing.T) {
			backupID := resource.BackupID(id)
			repo := new(mockBackupRepo)
			repo.On("GetByID", ctx, backupID).Return(backup, nil)
			defer repo.AssertExpectations(t)

			backupService := service.NewBackupService(repo, nil, nil, logger)
			bkup, err := backupService.Get(ctx, backupID)
			assert.NoError(t, err)
			assert.Equal(t, "p.d.t", bkup.ResourceNames()[0])
		})
	})
	t.Run("List", func(t *testing.T) {
		t.Run("returns error when error in service", func(t *testing.T) {
			repo := new(mockBackupRepo)
			repo.On("GetAll", ctx, tnnt, store).Return(nil, errors.NotFound("backup", "error in list backups"))
			defer repo.AssertExpectations(t)

			backupService := service.NewBackupService(repo, nil, nil, logger)
			_, err := backupService.List(ctx, tnnt, store)
			assert.Error(t, err)
			assert.EqualError(t, err, "not found for entity backup: error in list backups")
		})
		t.Run("returns recent backups", func(t *testing.T) {
			names := []string{"p.d.t1"}
			twoDaysAgo := time.Now().AddDate(0, 0, -2)
			bk1, err1 := resource.NewBackup(store, tnnt, names, "bak1", twoDaysAgo, map[string]string{})
			assert.Nil(t, err1)

			fourMonthsAgo := time.Now().AddDate(0, -4, 0)
			bk2, err2 := resource.NewBackup(store, tnnt, names, "bak2", fourMonthsAgo, map[string]string{})
			assert.Nil(t, err2)

			repo := new(mockBackupRepo)
			repo.On("GetAll", ctx, tnnt, store).Return([]*resource.Backup{bk1, bk2}, nil)
			defer repo.AssertExpectations(t)

			backupService := service.NewBackupService(repo, nil, nil, logger)
			lst, err := backupService.List(ctx, tnnt, store)
			assert.NoError(t, err)

			assert.Equal(t, 1, len(lst))
			assert.Equal(t, "bak1", lst[0].Description())
		})
	})
}

type mockBackupRepo struct {
	mock.Mock
}

func (m *mockBackupRepo) GetByID(ctx context.Context, id resource.BackupID) (*resource.Backup, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*resource.Backup), args.Error(1)
}

func (m *mockBackupRepo) GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Backup, error) {
	args := m.Called(ctx, tnnt, store)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*resource.Backup), args.Error(1)
}

func (m *mockBackupRepo) Create(ctx context.Context, backup *resource.Backup) error {
	return m.Called(ctx, backup).Error(0)
}

type mockResourceProvider struct {
	mock.Mock
}

func (m *mockResourceProvider) GetResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) ([]*resource.Resource, error) {
	args := m.Called(ctx, tnnt, store, names)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*resource.Resource), args.Error(1)
}

type mockBackupManager struct {
	mock.Mock
}

func (m *mockBackupManager) Backup(ctx context.Context, backup *resource.Backup, resources []*resource.Resource) (*resource.BackupResult, error) {
	args := m.Called(ctx, backup, resources)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*resource.BackupResult), args.Error(1)
}
