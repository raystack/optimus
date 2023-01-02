package v1beta1_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/resource/handler/v1beta1"
	"github.com/odpf/optimus/core/tenant"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

func TestBackupHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	store := resource.Bigquery
	tnnt, _ := tenant.NewTenant("proj", "ns")
	validID := "dda7b864-4268-4107-a096-dcf5343a0959"

	id, _ := uuid.Parse(validID)
	config := map[string]string{"ttl": "720hrs"}
	resNames := []string{"project.dataset.table1"}
	backup, err := resource.NewBackup(store, tnnt, resNames, "a new backup", time.Now(), config)
	assert.Nil(t, err)

	err = backup.UpdateID(id)
	assert.Nil(t, err)

	t.Run("CreateBackup", func(t *testing.T) {
		t.Run("returns error on invalid resource names", func(t *testing.T) {
			mockService := new(backupService)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.CreateBackupRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: store.String(),
				ResourceNames: []string{""},
				NamespaceName: "ns",
				Description:   "",
			}

			_, err := h.CreateBackup(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for "+
				"entity backup: one of resource names is empty: invalid backup request")
		})
		t.Run("returns error on invalid tenant", func(t *testing.T) {
			mockService := new(backupService)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.CreateBackupRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: store.String(),
				ResourceNames: []string{"proj.dataset.table"},
				NamespaceName: "",
				Description:   "",
			}

			_, err := h.CreateBackup(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"namespace: namespace name is empty: invalid backup request")
		})
		t.Run("returns error on invalid store", func(t *testing.T) {
			mockService := new(backupService)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.CreateBackupRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: "",
				ResourceNames: []string{"proj.dataset.table"},
				NamespaceName: "ns",
				Description:   "",
			}

			_, err := h.CreateBackup(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: unknown store : invalid backup request")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			mockService := new(backupService)
			mockService.On("Create", ctx, mock.Anything).
				Return(nil, errors.New("error in service"))
			defer mockService.AssertExpectations(t)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.CreateBackupRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: store.String(),
				ResourceNames: []string{"proj.dataset.table"},
				NamespaceName: "ns",
				Description:   "",
			}

			_, err := h.CreateBackup(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in service: error during backup")
		})
		t.Run("returns result of backup", func(t *testing.T) {
			mockService := new(backupService)
			mockService.On("Create", ctx, mock.Anything).
				Return(&resource.BackupResult{
					ResourceNames: []string{"bigquery://proj.dataset.table"},
					IgnoredResources: []resource.IgnoredResource{{
						Name:   "bigquery://proj.dataset.downstream",
						Reason: "resource is not a table",
					}},
				}, nil)
			defer mockService.AssertExpectations(t)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.CreateBackupRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: store.String(),
				ResourceNames: []string{"proj.dataset.table", "bigquery://proj.dataset.downstream"},
				NamespaceName: "ns",
				Description:   "",
			}

			backup, err := h.CreateBackup(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, "bigquery://proj.dataset.table", backup.ResourceNames[0])
			assert.Equal(t, "bigquery://proj.dataset.downstream", backup.IgnoredResources[0].Name)
			assert.Equal(t, "resource is not a table", backup.IgnoredResources[0].Reason)
		})
	})
	t.Run("ListBackups", func(t *testing.T) {
		t.Run("returns error on invalid tenant", func(t *testing.T) {
			mockService := new(backupService)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.ListBackupsRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: store.String(),
				NamespaceName: "",
			}

			_, err := h.ListBackups(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"namespace: namespace name is empty: invalid list backup request")
		})
		t.Run("returns error on invalid store", func(t *testing.T) {
			mockService := new(backupService)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.ListBackupsRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: "",
				NamespaceName: "ns",
			}

			_, err := h.ListBackups(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: unknown store : invalid list backup request")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			mockService := new(backupService)
			mockService.On("List", ctx, tnnt, store).
				Return([]*resource.Backup{}, errors.New("error in service"))
			defer mockService.AssertExpectations(t)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.ListBackupsRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: store.String(),
				NamespaceName: "ns",
			}

			_, err := h.ListBackups(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in service: "+
				"error in getting list of backup")
		})
		t.Run("returns list of backups", func(t *testing.T) {
			mockService := new(backupService)
			mockService.On("List", ctx, tnnt, store).Return([]*resource.Backup{backup}, nil)
			defer mockService.AssertExpectations(t)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.ListBackupsRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: store.String(),
				NamespaceName: "ns",
			}

			l, err := h.ListBackups(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(l.Backups))
			assert.Equal(t, "project.dataset.table1", l.Backups[0].ResourceNames[0])
		})
	})
	t.Run("GetBackup", func(t *testing.T) {
		t.Run("returns error on invalid backupID", func(t *testing.T) {
			mockService := new(backupService)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.GetBackupRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: store.String(),
				NamespaceName: "",
				Id:            "",
			}

			_, err := h.GetBackup(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"backup: invalid id for backup : invalid get backup request")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			id, _ := uuid.Parse(validID)
			mockService := new(backupService)
			mockService.On("Get", ctx, resource.BackupID(id)).
				Return(nil, errors.New("error in service"))
			defer mockService.AssertExpectations(t)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.GetBackupRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: store.String(),
				NamespaceName: "ns",
				Id:            validID,
			}

			_, err := h.GetBackup(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = error in service: invalid get "+
				"backup request for dda7b864-4268-4107-a096-dcf5343a0959")
		})
		t.Run("returns details of backup", func(t *testing.T) {
			mockService := new(backupService)
			mockService.On("Get", ctx, resource.BackupID(id)).Return(backup, nil)
			defer mockService.AssertExpectations(t)
			h := v1beta1.NewBackupHandler(logger, mockService)

			req := &pb.GetBackupRequest{
				ProjectName:   tnnt.ProjectName().String(),
				DatastoreName: store.String(),
				NamespaceName: "ns",
				Id:            validID,
			}

			b, err := h.GetBackup(ctx, req)
			assert.Nil(t, err)
			assert.Equal(t, "project.dataset.table1", b.Spec.ResourceNames[0])
		})
	})
}

type backupService struct {
	mock.Mock
}

func (b *backupService) Create(ctx context.Context, backup *resource.Backup) (*resource.BackupResult, error) {
	args := b.Called(ctx, backup)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*resource.BackupResult), args.Error(1)
}

func (b *backupService) Get(ctx context.Context, id resource.BackupID) (*resource.Backup, error) {
	args := b.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*resource.Backup), args.Error(1)
}

func (b *backupService) List(ctx context.Context, t tenant.Tenant, store resource.Store) ([]*resource.Backup, error) {
	args := b.Called(ctx, t, store)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*resource.Backup), args.Error(1)
}
