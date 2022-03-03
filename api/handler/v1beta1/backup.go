package v1beta1

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/uuid"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (sv *RuntimeServiceServer) BackupDryRun(ctx context.Context, req *pb.BackupDryRunRequest) (*pb.BackupDryRunResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(err, "unable to get namespace")
	}

	resourceSpec, err := sv.resourceSvc.ReadResource(ctx, namespaceSpec, req.DatastoreName, req.ResourceName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to read resource %s", err.Error(), req.ResourceName)
	}

	var jobSpecs []models.JobSpec
	jobSpec, err := sv.jobSvc.GetByDestination(ctx, namespaceSpec.ProjectSpec, resourceSpec.URN)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting job: %v", err)
	}
	jobSpecs = append(jobSpecs, jobSpec)

	downstreamSpecs, err := sv.jobSvc.GetDownstream(ctx, namespaceSpec.ProjectSpec, jobSpec.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting job downstream: %v", err)
	}
	jobSpecs = append(jobSpecs, downstreamSpecs...)

	//should add config
	backupRequest := models.BackupRequest{
		ResourceName:                req.ResourceName,
		Project:                     namespaceSpec.ProjectSpec,
		Namespace:                   namespaceSpec,
		Description:                 req.Description,
		AllowedDownstreamNamespaces: req.AllowedDownstreamNamespaces,
		DryRun:                      true,
	}
	backupPlan, err := sv.resourceSvc.BackupResourceDryRun(ctx, backupRequest, jobSpecs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while doing backup dry run: %v", err)
	}

	return &pb.BackupDryRunResponse{
		ResourceName:     backupPlan.Resources,
		IgnoredResources: backupPlan.IgnoredResources,
	}, nil
}

func (sv *RuntimeServiceServer) CreateBackup(ctx context.Context, req *pb.CreateBackupRequest) (*pb.CreateBackupResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(err, "unable to get namespace")
	}

	resourceSpec, err := sv.resourceSvc.ReadResource(ctx, namespaceSpec, req.DatastoreName, req.ResourceName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to read resource %s", err.Error(), req.ResourceName)
	}

	var jobSpecs []models.JobSpec
	jobSpec, err := sv.jobSvc.GetByDestination(ctx, namespaceSpec.ProjectSpec, resourceSpec.URN)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting job: %v", err)
	}
	jobSpecs = append(jobSpecs, jobSpec)

	if len(req.AllowedDownstreamNamespaces) > 0 {
		downstreamSpecs, err := sv.jobSvc.GetDownstream(ctx, namespaceSpec.ProjectSpec, jobSpec.Name)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error while getting job downstream: %v", err)
		}
		jobSpecs = append(jobSpecs, downstreamSpecs...)
	}

	backupRequest := models.BackupRequest{
		ResourceName:                req.ResourceName,
		Project:                     namespaceSpec.ProjectSpec,
		Namespace:                   namespaceSpec,
		Description:                 req.Description,
		AllowedDownstreamNamespaces: req.AllowedDownstreamNamespaces,
		DryRun:                      false,
		Config:                      req.Config,
	}
	result, err := sv.resourceSvc.BackupResource(ctx, backupRequest, jobSpecs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while doing backup: %v", err)
	}

	return &pb.CreateBackupResponse{
		Urn:              result.Resources,
		IgnoredResources: result.IgnoredResources,
	}, nil
}

func (sv *RuntimeServiceServer) ListBackups(ctx context.Context, req *pb.ListBackupsRequest) (*pb.ListBackupsResponse, error) {
	projectSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(err, fmt.Sprintf("not able to find project %s", req.GetProjectName()))
	}

	results, err := sv.resourceSvc.ListResourceBackups(ctx, projectSpec, req.DatastoreName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error while getting backup list: %v", err)
	}

	var backupList []*pb.BackupSpec
	for _, result := range results {
		backupList = append(backupList, &pb.BackupSpec{
			Id:           result.ID.String(),
			ResourceName: result.Resource.Name,
			CreatedAt:    timestamppb.New(result.CreatedAt),
			Description:  result.Description,
			Config:       result.Config,
		})
	}
	return &pb.ListBackupsResponse{
		Backups: backupList,
	}, nil
}

func (sv *RuntimeServiceServer) GetBackup(ctx context.Context, req *pb.GetBackupRequest) (*pb.GetBackupResponse, error) {
	projectSpec, err := sv.projectService.Get(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(err, fmt.Sprintf("not able to find project %s", req.GetProjectName()))
	}

	uuid, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error while parsing backup ID: %v", err)
	}

	backupDetail, err := sv.resourceSvc.GetResourceBackup(ctx, projectSpec, req.DatastoreName, uuid)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return nil, status.Errorf(codes.NotFound, "%s: backup with ID %s not found", err.Error(), uuid.String())
		}
		return nil, status.Errorf(codes.Internal, "error while getting backup detail: %v", err)
	}

	var results []string
	for _, result := range backupDetail.Result {
		backupResult, ok := result.(map[string]interface{})
		if !ok {
			return nil, status.Errorf(codes.Internal, "error while parsing backup result: %v", ok)
		}

		backupURN, ok := backupResult[models.BackupSpecKeyURN]
		if !ok {
			return nil, status.Errorf(codes.Internal, "%s is not found in backup result", models.BackupSpecKeyURN)
		}

		backupURNStr, ok := backupURN.(string)
		if !ok {
			return nil, status.Errorf(codes.Internal, "invalid backup URN: %v", backupURN)
		}

		results = append(results, backupURNStr)
	}

	return &pb.GetBackupResponse{
		Spec: &pb.BackupSpec{
			Id:           backupDetail.ID.String(),
			ResourceName: backupDetail.Resource.Name,
			CreatedAt:    timestamppb.New(backupDetail.CreatedAt),
			Description:  backupDetail.Description,
			Config:       backupDetail.Config,
		},
		Urn: results,
	}, nil
}
