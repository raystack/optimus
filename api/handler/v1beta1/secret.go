package v1beta1

import (
	"context"
	"encoding/base64"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (sv *RuntimeServiceServer) RegisterSecret(ctx context.Context, req *pb.RegisterSecretRequest) (*pb.RegisterSecretResponse, error) {
	base64Decoded, err := getDecodedSecret(req.GetValue())
	if err != nil {
		return nil, err
	}

	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(ctx, req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceSpec := models.NamespaceSpec{}
	if req.GetNamespaceName() != "" {
		namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
		namespaceSpec, err = namespaceRepo.GetByName(ctx, req.GetNamespaceName())
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespaceName())
		}
	}

	secretRepo := sv.secretRepoFactory.New(projSpec, namespaceSpec)
	if err := secretRepo.Save(ctx, models.ProjectSecretItem{
		Name:  req.GetSecretName(),
		Value: base64Decoded,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to register secret %s", err.Error(), req.GetSecretName())
	}

	return &pb.RegisterSecretResponse{
		Success: true,
	}, nil
}

func (sv *RuntimeServiceServer) UpdateSecret(ctx context.Context, req *pb.UpdateSecretRequest) (*pb.UpdateSecretResponse, error) {
	base64Decoded, err := getDecodedSecret(req.GetValue())
	if err != nil {
		return nil, err
	}

	projectRepo := sv.projectRepoFactory.New()
	projSpec, err := projectRepo.GetByName(ctx, req.GetProjectName())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%s: project %s not found", err.Error(), req.GetProjectName())
	}

	namespaceSpec := models.NamespaceSpec{}
	if req.GetNamespaceName() != "" {
		namespaceRepo := sv.namespaceRepoFactory.New(projSpec)
		namespaceSpec, err = namespaceRepo.GetByName(ctx, req.GetNamespaceName())
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "%s: namespace %s not found", err.Error(), req.GetNamespaceName())
		}
	}

	secretRepo := sv.secretRepoFactory.New(projSpec, namespaceSpec)
	if err := secretRepo.Update(ctx, models.ProjectSecretItem{
		Name:  req.GetSecretName(),
		Value: base64Decoded,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to update secret %s", err.Error(), req.GetSecretName())
	}

	return &pb.UpdateSecretResponse{
		Success: true,
	}, nil
}

func getDecodedSecret(encodedString string) (string, error) {
	if encodedString == "" {
		return "", status.Error(codes.InvalidArgument, "empty value for secret")
	}
	// decode base64
	base64Decoded, err := base64.StdEncoding.DecodeString(encodedString)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "failed to decode base64 string: \n%s", err.Error())
	}
	return string(base64Decoded), nil
}
