package v1beta1

import (
	"context"
	"io"
	"sync"
	"time"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (sv *RuntimeServiceServer) CreateResource(ctx context.Context, req *pb.CreateResourceRequest) (*pb.CreateResourceResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	optResource, err := sv.adapter.FromResourceProto(req.Resource, req.DatastoreName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to parse resource %s", err.Error(), req.Resource.GetName())
	}

	if err := sv.resourceSvc.CreateResource(ctx, namespaceSpec, []models.ResourceSpec{optResource}, sv.progressObserver); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to create resource %s", err.Error(), req.Resource.GetName())
	}
	runtimeDeployResourceSpecificationCounter.Inc()
	return &pb.CreateResourceResponse{
		Success: true,
	}, nil
}

func (sv *RuntimeServiceServer) UpdateResource(ctx context.Context, req *pb.UpdateResourceRequest) (*pb.UpdateResourceResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	optResource, err := sv.adapter.FromResourceProto(req.Resource, req.DatastoreName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to parse resource %s", err.Error(), req.Resource.GetName())
	}

	if err := sv.resourceSvc.UpdateResource(ctx, namespaceSpec, []models.ResourceSpec{optResource}, sv.progressObserver); err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to create resource %s", err.Error(), req.Resource.GetName())
	}
	runtimeDeployResourceSpecificationCounter.Inc()
	return &pb.UpdateResourceResponse{
		Success: true,
	}, nil
}

func (sv *RuntimeServiceServer) ReadResource(ctx context.Context, req *pb.ReadResourceRequest) (*pb.ReadResourceResponse, error) {
	namespaceSpec, err := sv.namespaceService.Get(ctx, req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, mapToGRPCErr(sv.l, err, "unable to get namespace")
	}

	response, err := sv.resourceSvc.ReadResource(ctx, namespaceSpec, req.DatastoreName, req.ResourceName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to read resource %s", err.Error(), req.ResourceName)
	}

	protoResource, err := sv.adapter.ToResourceProto(response)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s: failed to adapt resource %s", err.Error(), req.ResourceName)
	}

	return &pb.ReadResourceResponse{
		Success:  true,
		Resource: protoResource,
	}, nil
}

func (sv *RuntimeServiceServer) DeployResourceSpecification(stream pb.RuntimeService_DeployResourceSpecificationServer) error {
	startTime := time.Now()
	for {
		request, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				break
			}
			stream.Send(&pb.DeployResourceSpecificationResponse{
				Success: false,
				Ack:     true,
				Message: err.Error(),
			})
			return err
		}
		namespaceSpec, err := sv.namespaceService.Get(stream.Context(), request.GetProjectName(), request.GetNamespaceName())
		if err != nil {
			stream.Send(&pb.DeployResourceSpecificationResponse{
				Success: false,
				Ack:     true,
				Message: err.Error(),
			})
			return mapToGRPCErr(sv.l, err, "unable to get namespace")
		}
		var resourceSpecs []models.ResourceSpec
		for _, resourceProto := range request.GetResources() {
			adapted, err := sv.adapter.FromResourceProto(resourceProto, request.DatastoreName)
			if err != nil {
				stream.Send(&pb.DeployResourceSpecificationResponse{
					Success: false,
					Ack:     true,
					Message: err.Error(),
				})
				return status.Errorf(codes.Internal, "%s: cannot adapt resource %s", err.Error(), resourceProto.GetName())
			}
			resourceSpecs = append(resourceSpecs, adapted)
		}

		observers := new(progress.ObserverChain)
		observers.Join(sv.progressObserver)
		observers.Join(&resourceObserver{
			stream: stream,
			log:    sv.l,
			mu:     new(sync.Mutex),
		})

		if err := sv.resourceSvc.UpdateResource(stream.Context(), namespaceSpec, resourceSpecs, observers); err != nil {
			stream.Send(&pb.DeployResourceSpecificationResponse{
				Success: false,
				Ack:     true,
				Message: err.Error(),
			})
			return status.Errorf(codes.Internal, "failed to update resources: \n%s", err.Error())
		}

		runtimeDeployResourceSpecificationCounter.Add(float64(len(request.Resources)))
		stream.Send(&pb.DeployResourceSpecificationResponse{
			Success: true,
			Ack:     true,
			Message: "success",
		})
	}
	sv.l.Info("finished resource deployment in", "time", time.Since(startTime))
	return nil
}

func (sv *RuntimeServiceServer) DeployResourceSpecification0(req *pb.DeployResourceSpecificationRequest, respStream pb.RuntimeService_DeployResourceSpecificationServer) error {
	startTime := time.Now()

	namespaceSpec, err := sv.namespaceService.Get(respStream.Context(), req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return mapToGRPCErr(sv.l, err, "unable to get namespace")
	}
	var resourceSpecs []models.ResourceSpec
	for _, resourceProto := range req.GetResources() {
		adapted, err := sv.adapter.FromResourceProto(resourceProto, req.DatastoreName)
		if err != nil {
			return status.Errorf(codes.Internal, "%s: cannot adapt resource %s", err.Error(), resourceProto.GetName())
		}
		resourceSpecs = append(resourceSpecs, adapted)
	}

	observers := new(progress.ObserverChain)
	observers.Join(sv.progressObserver)
	observers.Join(&resourceObserver{
		stream: respStream,
		log:    sv.l,
		mu:     new(sync.Mutex),
	})

	if err := sv.resourceSvc.UpdateResource(respStream.Context(), namespaceSpec, resourceSpecs, observers); err != nil {
		return status.Errorf(codes.Internal, "failed to update resources: \n%s", err.Error())
	}

	runtimeDeployResourceSpecificationCounter.Add(float64(len(req.Resources)))
	sv.l.Info("finished resource deployment in", "time", time.Since(startTime))
	return nil
}

func (sv *RuntimeServiceServer) ListResourceSpecification(ctx context.Context, req *pb.ListResourceSpecificationRequest) (*pb.ListResourceSpecificationResponse, error) {
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
		resourceProto, err := sv.adapter.ToResourceProto(resourceSpec)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s: failed to parse resource spec %s", err.Error(), resourceSpec.Name)
		}
		resourceProtos = append(resourceProtos, resourceProto)
	}
	return &pb.ListResourceSpecificationResponse{
		Resources: resourceProtos,
	}, nil
}
