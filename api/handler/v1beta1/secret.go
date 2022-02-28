package v1beta1

import (
	"context"
	"encoding/base64"
	"fmt"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (sv *RuntimeServiceServer) RegisterSecret(ctx context.Context, req *pb.RegisterSecretRequest) (*pb.RegisterSecretResponse, error) {
	base64Decoded, err := getDecodedSecret(req.GetValue())
	if err != nil {
		return nil, err
	}

	if err = sv.secretService.Save(ctx, req.GetProjectName(), req.GetNamespaceName(), models.ProjectSecretItem{
		Name:  req.GetSecretName(),
		Value: base64Decoded,
	}); err != nil {
		return nil, mapToGRPCErr(err, fmt.Sprintf("failed to register secret %s", req.GetSecretName()))
	}

	return &pb.RegisterSecretResponse{}, nil
}

func (sv *RuntimeServiceServer) UpdateSecret(ctx context.Context, req *pb.UpdateSecretRequest) (*pb.UpdateSecretResponse, error) {
	base64Decoded, err := getDecodedSecret(req.GetValue())
	if err != nil {
		return nil, err
	}

	if err = sv.secretService.Update(ctx, req.GetProjectName(), req.GetNamespaceName(), models.ProjectSecretItem{
		Name:  req.GetSecretName(),
		Value: base64Decoded,
	}); err != nil {
		return nil, mapToGRPCErr(err, fmt.Sprintf("failed to update secret %s", req.GetSecretName()))
	}

	return &pb.UpdateSecretResponse{}, nil
}

func (sv *RuntimeServiceServer) ListSecrets(ctx context.Context, req *pb.ListSecretsRequest) (*pb.ListSecretsResponse, error) {
	secrets, err := sv.secretService.List(ctx, req.GetProjectName())
	if err != nil {
		return nil, mapToGRPCErr(err, "failed to list secrets")
	}

	var secretsResponse []*pb.ListSecretsResponse_Secret
	for _, s := range secrets {
		respSecret := pb.ListSecretsResponse_Secret{
			Name:      s.Name,
			Namespace: s.Namespace,
			Digest:    s.Digest,
			UpdatedAt: timestamppb.New(s.UpdatedAt),
		}
		secretsResponse = append(secretsResponse, &respSecret)
	}

	return &pb.ListSecretsResponse{Secrets: secretsResponse}, nil
}

func (sv *RuntimeServiceServer) DeleteSecret(ctx context.Context, req *pb.DeleteSecretRequest) (*pb.DeleteSecretResponse, error) {
	if err := sv.secretService.Delete(ctx, req.GetProjectName(), req.GetNamespaceName(), req.GetSecretName()); err != nil {
		return nil, mapToGRPCErr(err, fmt.Sprintf("failed to delete secret %s", req.GetSecretName()))
	}

	return &pb.DeleteSecretResponse{}, nil
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
