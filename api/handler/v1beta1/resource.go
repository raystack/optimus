package v1beta1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/internal/lib/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
)

var runtimeDeployResourceSpecificationCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "runtime_deploy_resourcespec",
	Help: "Number of resources requested for deployment by runtime",
})

type ResourceServiceServer struct {
	l                log.Logger
	resourceSvc      models.DatastoreService
	namespaceService service.NamespaceService
	datastoreRepo    models.DatastoreRepo
	progressObserver progress.Observer
	pb.UnimplementedResourceServiceServer
}

func (sv *ResourceServiceServer) CreateResource(ctx context.Context, req *pb.CreateResourceRequest) (*pb.CreateResourceResponse, error) {
	logWriter := writer.NewLogWriter(sv.l)
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	optResource, err := FromResourceProto(req.Resource, req.DatastoreName, sv.datastoreRepo)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to parse resource %s", err.Error(), req.Resource.GetName())
	}

	if err := sv.resourceSvc.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{optResource}, logWriter); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to create resource %s", err.Error(), req.Resource.GetName())
	}
	runtimeDeployResourceSpecificationCounter.Inc()
	return &pb.CreateResourceResponse{
		Success: true,
	}, nil
}

func (sv *ResourceServiceServer) UpdateResource(ctx context.Context, req *pb.UpdateResourceRequest) (*pb.UpdateResourceResponse, error) {
	logWriter := writer.NewLogWriter(sv.l)
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	optResource, err := FromResourceProto(req.Resource, req.DatastoreName, sv.datastoreRepo)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to parse resource %s", err.Error(), req.Resource.GetName())
	}

	if err := sv.resourceSvc.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{optResource}, logWriter); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to create resource %s", err.Error(), req.Resource.GetName())
	}
	runtimeDeployResourceSpecificationCounter.Inc()
	return &pb.UpdateResourceResponse{
		Success: true,
	}, nil
}

func (sv *ResourceServiceServer) ReadResource(ctx context.Context, req *pb.ReadResourceRequest) (*pb.ReadResourceResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	response, err := sv.resourceSvc.ReadResource(ctx, namespaceSpec, req.DatastoreName, req.ResourceName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to read resource %s", err.Error(), req.ResourceName)
	}

	protoResource, err := ToResourceProto(response)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to adapt resource %s", err.Error(), req.ResourceName)
	}

	return &pb.ReadResourceResponse{
		Success:  true,
		Resource: protoResource,
	}, nil
}

func (sv *ResourceServiceServer) DeployResourceSpecification(stream pb.ResourceService_DeployResourceSpecificationServer) error {
	startTime := time.Now()
	responseWriter := writer.NewDeployResourceSpecificationResponseWriter(stream)
	errNamespaces := []string{}

	for {
		request, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err // immediate error returned (grpc error level)
		}
		namespaceSpec, err := sv.namespaceService.Get(stream.Context(), request.GetProjectName(), request.GetNamespaceName())
		if err != nil {
			errMsg := fmt.Sprintf("error when fetch namespace %s: %s", request.GetNamespaceName(), err.Error())
			sv.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
			continue
		}
		var resourceSpecs []models.ResourceSpec
		for _, resourceProto := range request.GetResources() {
			adapted, err := FromResourceProto(resourceProto, request.DatastoreName, sv.datastoreRepo)
			if err != nil {
				errMsg := fmt.Sprintf("%s: cannot adapt resource %s", err.Error(), resourceProto.GetName())
				sv.l.Error(errMsg)
				responseWriter.Write(writer.LogLevelError, errMsg)
				continue
			}
			resourceSpecs = append(resourceSpecs, adapted)
		}
		if len(resourceSpecs) != len(request.GetResources()) {
			// some of adapt resource from proto is failed
			errNamespaces = append(errNamespaces, request.NamespaceName)
			continue
		}

		if err := sv.resourceSvc.UpdateResource(stream.Context(), namespaceSpec, resourceSpecs, responseWriter); err != nil {
			errMsg := fmt.Sprintf("failed to update resources: %s", err.Error())
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
			continue
		}

		runtimeDeployResourceSpecificationCounter.Add(float64(len(resourceSpecs)))
		successMsg := fmt.Sprintf("resources with namespace [%s] are deployed successfully", request.NamespaceName)
		responseWriter.Write(writer.LogLevelInfo, successMsg)
	}
	sv.l.Info("finished resource deployment in", "time", time.Since(startTime))
	if len(errNamespaces) > 0 {
		sv.l.Warn(fmt.Sprintf("there's error while deploying namespaces: [%s]", strings.Join(errNamespaces, ", ")))
		return fmt.Errorf("error when deploying: [%s]", strings.Join(errNamespaces, ", "))
	}
	return nil
}

func (sv *ResourceServiceServer) ListResourceSpecification(ctx context.Context, req *pb.ListResourceSpecificationRequest) (*pb.ListResourceSpecificationResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	resourceSpecs, err := sv.resourceSvc.GetAll(ctx, namespaceSpec, req.DatastoreName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to retrieve jobs for project %s", err.Error(), req.GetProjectName())
	}

	resourceProtos := []*pb.ResourceSpecification{}
	for _, resourceSpec := range resourceSpecs {
		resourceProto, err := ToResourceProto(resourceSpec)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s: failed to parse resource spec %s", err.Error(), resourceSpec.Name)
		}
		resourceProtos = append(resourceProtos, resourceProto)
	}
	return &pb.ListResourceSpecificationResponse{
		Resources: resourceProtos,
	}, nil
}

func NewResourceServiceServer(l log.Logger, datastoreSvc models.DatastoreService, namespaceService service.NamespaceService, datastoreRepo models.DatastoreRepo, progressObserver progress.Observer) *ResourceServiceServer {
	return &ResourceServiceServer{
		l:                l,
		resourceSvc:      datastoreSvc,
		datastoreRepo:    datastoreRepo,
		namespaceService: namespaceService,
		progressObserver: progressObserver,
	}
}
