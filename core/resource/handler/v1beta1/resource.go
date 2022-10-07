package v1beta1

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type ResourceService interface {
	Create(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error
	Update(ctx context.Context, tnnt tenant.Tenant, res *resource.Resource) error
	Read(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resourceName resource.Name) (*resource.Resource, error)
	GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error)
	BatchUpdate(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource) error
}

type ResourceHandler struct {
	l       log.Logger
	service ResourceService

	pb.UnimplementedResourceServiceServer
}

func (rh ResourceHandler) DeployResourceSpecification(stream pb.ResourceService_DeployResourceSpecificationServer) error {
	startTime := time.Now()
	responseWriter := writer.NewDeployResourceSpecificationResponseWriter(stream)
	var errNamespaces []string

	for {
		request, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		tnnt, err := tenant.NewTenant(request.GetProjectName(), request.GetNamespaceName())
		if err != nil {
			errMsg := fmt.Sprintf("invalid deploy request for %s: %s", request.GetNamespaceName(), err.Error())
			rh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
			continue
		}

		var resourceSpecs []*resource.Resource
		for _, resourceProto := range request.GetResources() {
			adapted, err := fromResourceProto(resourceProto)
			if err != nil {
				errMsg := fmt.Sprintf("%s: cannot adapt resource %s", err.Error(), resourceProto.GetName())
				rh.l.Error(errMsg)
				responseWriter.Write(writer.LogLevelError, errMsg)
				continue
			}
			resourceSpecs = append(resourceSpecs, adapted)
		}
		if len(resourceSpecs) != len(request.GetResources()) {
			errNamespaces = append(errNamespaces, request.GetNamespaceName())
			continue
		}

		if err := rh.service.BatchUpdate(stream.Context(), tnnt, resourceSpecs); err != nil {
			errMsg := fmt.Sprintf("failed to update resources: %s", err.Error())
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.GetNamespaceName())
			continue
		}

		//runtimeDeployResourceSpecificationCounter.Add(float64(len(resourceSpecs)))
		successMsg := fmt.Sprintf("resources with namespace [%s] are deployed successfully", request.GetNamespaceName())
		responseWriter.Write(writer.LogLevelInfo, successMsg)
	}
	rh.l.Info("Finished resource deployment in", "time", time.Since(startTime))
	if len(errNamespaces) > 0 {
		namespacesWithError := strings.Join(errNamespaces, ", ")
		rh.l.Warn(fmt.Sprintf("Error while deploying namespaces: [%s]", namespacesWithError))
		return fmt.Errorf("error when deploying: [%s]", namespacesWithError)
	}
	return nil
}

func (rh ResourceHandler) ListResourceSpecification(ctx context.Context, req *pb.ListResourceSpecificationRequest) (*pb.ListResourceSpecificationResponse, error) {
	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		return nil, errors.GRPCErr(errors.InvalidArgument(resource.EntityResource, "invalid datastore name"), "invalid list resource request")
	}

	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to list resource for "+req.GetDatastoreName())
	}

	resources, err := rh.service.GetAll(ctx, tnnt, store)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to retrieve jobs for project "+req.GetProjectName())
	}

	var resourceProtos []*pb.ResourceSpecification
	for _, resourceSpec := range resources {
		resourceProto, err := toResourceProto(resourceSpec)
		if err != nil {
			return nil, errors.GRPCErr(err, "failed to parse resource "+resourceSpec.FullName())
		}
		resourceProtos = append(resourceProtos, resourceProto)
	}

	return &pb.ListResourceSpecificationResponse{
		Resources: resourceProtos,
	}, nil
}

func (rh ResourceHandler) CreateResource(ctx context.Context, req *pb.CreateResourceRequest) (*pb.CreateResourceResponse, error) {
	res, err := fromResourceProto(req.Resource)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to create resource")
	}

	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to create resource "+res.FullName())
	}

	err = rh.service.Create(ctx, tnnt, res)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to create resource "+res.FullName())
	}
	//runtimeDeployResourceSpecificationCounter.Inc()

	return &pb.CreateResourceResponse{}, nil
}

func (rh ResourceHandler) ReadResource(ctx context.Context, req *pb.ReadResourceRequest) (*pb.ReadResourceResponse, error) {
	resName, err := resource.NameFrom(req.GetResourceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid read resource request")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid read resource request")
	}

	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to read resource "+req.GetResourceName())
	}

	response, err := rh.service.Read(ctx, tnnt, store, resName)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to read resource "+req.GetResourceName())
	}

	protoResource, err := toResourceProto(response)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to read resource "+req.GetResourceName())
	}

	return &pb.ReadResourceResponse{
		Resource: protoResource,
	}, nil
}

func (rh ResourceHandler) UpdateResource(ctx context.Context, req *pb.UpdateResourceRequest) (*pb.UpdateResourceResponse, error) {
	res, err := fromResourceProto(req.Resource)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to update resource")
	}

	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to update resource "+res.FullName())
	}

	err = rh.service.Update(ctx, tnnt, res)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to update resource "+res.FullName())
	}

	//runtimeDeployResourceSpecificationCounter.Inc()
	return &pb.UpdateResourceResponse{}, nil
}

func fromResourceProto(rs *pb.ResourceSpecification) (*resource.Resource, error) {
	if rs == nil {
		return nil, errors.InvalidArgument(resource.EntityResource, "empty resource")
	}

	return nil, nil
}

func toResourceProto(res *resource.Resource) (*pb.ResourceSpecification, error) {
	return nil, nil
}
