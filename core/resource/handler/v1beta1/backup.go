package v1beta1

import (
	"context"
	"time"

	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type BackupService interface {
	Create(context.Context, *resource.Backup) (*resource.BackupResult, error)
	Get(context.Context, resource.BackupID) (*resource.Backup, error)
	List(context.Context, tenant.Tenant, resource.Store) ([]*resource.Backup, error)
}

type BackupHandler struct {
	l       log.Logger
	service BackupService

	pb.UnimplementedBackupServiceServer
}

func (b BackupHandler) CreateBackup(ctx context.Context, req *pb.CreateBackupRequest) (*pb.CreateBackupResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		b.l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "invalid backup request")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		b.l.Error("invalid datastore name [%s]: %s", req.GetDatastoreName(), err)
		return nil, errors.GRPCErr(err, "invalid backup request")
	}

	backup, err := resource.NewBackup(store, tnnt, req.ResourceNames, req.Description, time.Now(), req.Config)
	if err != nil {
		b.l.Error("error initializing backup: %s", err)
		return nil, errors.GRPCErr(err, "invalid backup request")
	}

	result, err := b.service.Create(ctx, backup)
	if err != nil {
		b.l.Error("error creating backup: %s", err)
		return nil, errors.GRPCErr(err, "error during backup")
	}

	return &pb.CreateBackupResponse{
		BackupId:         backup.ID().String(),
		ResourceNames:    result.ResourceNames,
		IgnoredResources: toIgnoredResources(result.IgnoredResources),
	}, nil
}

func (b BackupHandler) ListBackups(ctx context.Context, req *pb.ListBackupsRequest) (*pb.ListBackupsResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		b.l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "invalid list backup request")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		b.l.Error("invalid datastore name [%s]: %s", req.GetDatastoreName(), err)
		return nil, errors.GRPCErr(err, "invalid list backup request")
	}

	results, err := b.service.List(ctx, tnnt, store)
	if err != nil {
		b.l.Error("error listing backups: %s", err)
		return nil, errors.GRPCErr(err, "error in getting list of backup")
	}

	var backupList []*pb.BackupSpec
	for _, result := range results {
		backupList = append(backupList, toBackupSpec(result))
	}
	return &pb.ListBackupsResponse{
		Backups: backupList,
	}, nil
}

func (b BackupHandler) GetBackup(ctx context.Context, req *pb.GetBackupRequest) (*pb.GetBackupResponse, error) {
	backupID, err := resource.BackupIDFrom(req.GetId())
	if err != nil {
		b.l.Error("cannot adapt backup id [%s]: %s", req.GetId(), err)
		return nil, errors.GRPCErr(err, "invalid get backup request")
	}

	backupDetail, err := b.service.Get(ctx, backupID)
	if err != nil {
		b.l.Error("error getting backup [%s]: %s", req.GetId(), err)
		return nil, errors.GRPCErr(err, "invalid get backup request for "+backupID.UUID().String())
	}

	return &pb.GetBackupResponse{
		Spec: toBackupSpec(backupDetail),
	}, nil
}

func toBackupSpec(detail *resource.Backup) *pb.BackupSpec {
	return &pb.BackupSpec{
		Id:            detail.ID().String(),
		ResourceNames: detail.ResourceNames(),
		CreatedAt:     timestamppb.New(detail.CreatedAt()),
		Description:   detail.Description(),
		Config:        detail.Config(),
	}
}

func toIgnoredResources(ignored []resource.IgnoredResource) []*pb.IgnoredResource {
	var ignoredResources []*pb.IgnoredResource
	for _, ig := range ignored {
		ignoredResources = append(ignoredResources, &pb.IgnoredResource{
			Name:   ig.Name,
			Reason: ig.Reason,
		})
	}
	return ignoredResources
}

func NewBackupHandler(l log.Logger, service BackupService) *BackupHandler {
	return &BackupHandler{
		l:       l,
		service: service,
	}
}
