package v1beta1

import (
	"context"

	"github.com/odpf/salt/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type BackupService interface {
	DryRun(context.Context, tenant.Tenant, resource.Store, resource.BackupConfiguration) (resource.BackupInfo, error)
	Backup(context.Context, tenant.Tenant, resource.Store, resource.BackupConfiguration) (resource.BackupInfo, error)
	Get(context.Context, tenant.Tenant, resource.Store, resource.BackupID) (*resource.BackupDetails, error)
	List(context.Context, tenant.Tenant, resource.Store) ([]*resource.BackupDetails, error)
}

type BackupHandler struct {
	l       log.Logger
	service BackupService

	pb.UnimplementedBackupServiceServer
}

func (b BackupHandler) BackupDryRun(ctx context.Context, req *pb.BackupDryRunRequest) (*pb.BackupDryRunResponse, error) {
	resName, err := resource.NameFrom(req.GetResourceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid backup dry run request")
	}

	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to dry run backup resource")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid backup dry run request")
	}

	backupConfig := resource.BackupConfiguration{
		ResourceName:                resName,
		Description:                 req.Description,
		AllowedDownstreamNamespaces: req.AllowedDownstreamNamespaces,
	}

	result, err := b.service.DryRun(ctx, tnnt, store, backupConfig)
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid backup dry run request")
	}

	return &pb.BackupDryRunResponse{
		ResourceName:     result.ResourceURNs,
		IgnoredResources: result.IgnoredResources,
	}, nil
}

func (b BackupHandler) CreateBackup(ctx context.Context, req *pb.CreateBackupRequest) (*pb.CreateBackupResponse, error) {
	resName, err := resource.NameFrom(req.GetResourceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid backup request")
	}

	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid backup request")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid backup request")
	}

	backupConfig := resource.BackupConfiguration{
		ResourceName:                resName,
		Description:                 req.Description,
		AllowedDownstreamNamespaces: req.AllowedDownstreamNamespaces,
		Config:                      req.Config,
	}

	result, err := b.service.Backup(ctx, tnnt, store, backupConfig)
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid backup dry run request")
	}

	return &pb.CreateBackupResponse{
		Urn:              result.ResourceURNs,
		IgnoredResources: result.IgnoredResources,
	}, nil
}

func (b BackupHandler) ListBackups(ctx context.Context, req *pb.ListBackupsRequest) (*pb.ListBackupsResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid list backup request")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid list backup request")
	}

	results, err := b.service.List(ctx, tnnt, store)
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid list backup request")
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
		return nil, errors.GRPCErr(err, "invalid get backup request")
	}

	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid get backup request for "+backupID.UUID().String())
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid get backup request for "+backupID.UUID().String())
	}

	backupDetail, err := b.service.Get(ctx, tnnt, store, backupID)
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid get backup request for "+backupID.UUID().String())
	}

	return &pb.GetBackupResponse{
		Spec: toBackupSpec(backupDetail),
		Urn:  backupDetail.SourceURNs(),
	}, nil
}

func toBackupSpec(detail *resource.BackupDetails) *pb.BackupSpec {
	return &pb.BackupSpec{
		Id:           detail.ID.String(),
		ResourceName: detail.ResourceName,
		CreatedAt:    timestamppb.New(detail.CreatedAt),
		Description:  detail.Description,
		Config:       detail.Config,
	}
}
