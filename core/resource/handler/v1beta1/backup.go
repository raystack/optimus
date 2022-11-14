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
	Backup(context.Context, tenant.Tenant, resource.Store, resource.BackupConfiguration) (resource.BackupInfo, error)
	Get(context.Context, tenant.Tenant, resource.Store, resource.BackupID) (*resource.BackupDetails, error)
	List(context.Context, tenant.Tenant, resource.Store) ([]*resource.BackupDetails, error)
}

type BackupHandler struct {
	l       log.Logger
	service BackupService

	pb.UnimplementedBackupServiceServer
}

func (b BackupHandler) CreateBackup(ctx context.Context, req *pb.CreateBackupRequest) (*pb.CreateBackupResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid backup request")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid backup request")
	}

	var resourceNames []resource.Name
	for _, resourceName := range req.ResourceNames {
		resName, err := resource.NameFrom(resourceName)
		if err != nil {
			return nil, errors.GRPCErr(err, "invalid backup request")
		}
		resourceNames = append(resourceNames, resName)
	}

	backupConfig := resource.BackupConfiguration{
		ResourceNames: resourceNames,
		Description:   req.Description,
		Config:        req.Config,
	}

	result, err := b.service.Backup(ctx, tnnt, store, backupConfig)
	if err != nil {
		return nil, errors.GRPCErr(err, "error during backup")
	}

	return &pb.CreateBackupResponse{
		ResourceNames:    result.ResourceNamesAsString(),
		IgnoredResources: toIgnoredResources(result.IgnoredResources),
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
	}, nil
}

func toBackupSpec(detail *resource.BackupDetails) *pb.BackupSpec {
	return &pb.BackupSpec{
		Id:            detail.ID.String(),
		ResourceNames: detail.ResourceNamesAsString(),
		CreatedAt:     timestamppb.New(detail.CreatedAt),
		Description:   detail.Description,
		Config:        detail.Config,
	}
}

func toIgnoredResources(ignored []resource.IgnoredResource) []*pb.IgnoredResource {
	var ignoredResources []*pb.IgnoredResource
	for _, ig := range ignored {
		ignoredResources = append(ignoredResources, &pb.IgnoredResource{
			Name:   ig.Name.String(),
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
