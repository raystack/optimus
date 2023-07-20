package v1beta1

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/raystack/salt/log"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/internal/errors"
	"github.com/raystack/optimus/internal/telemetry"
	"github.com/raystack/optimus/internal/writer"
	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
)

const (
	metricResourceEvents             = "resource_events_total"
	metricResourcesUploadAllDuration = "resource_upload_all_duration_seconds_total"
)

type ResourceService interface {
	Create(ctx context.Context, res *resource.Resource) error
	Update(ctx context.Context, res *resource.Resource, logWriter writer.LogWriter) error
	ChangeNamespace(ctx context.Context, datastore resource.Store, resourceFullName string, oldTenant, newTenant tenant.Tenant) error
	Get(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resourceName string) (*resource.Resource, error)
	GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error)
	Deploy(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resources []*resource.Resource, logWriter writer.LogWriter) error
	SyncResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) (*resource.SyncResponse, error)
}

type ResourceHandler struct {
	l       log.Logger
	service ResourceService

	pb.UnimplementedResourceServiceServer
}

func (rh ResourceHandler) DeployResourceSpecification(stream pb.ResourceService_DeployResourceSpecificationServer) error {
	responseWriter := writer.NewDeployResourceSpecificationResponseWriter(stream)
	var errNamespaces []string

	for {
		request, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			errMsg := fmt.Sprintf("error encountered when receiving stream request: %s", err)
			rh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			return err
		}

		startTime := time.Now()
		tnnt, err := tenant.NewTenant(request.GetProjectName(), request.GetNamespaceName())
		if err != nil {
			errMsg := fmt.Sprintf("invalid tenant information request project [%s] namespace [%s]: %s", request.GetProjectName(), request.GetNamespaceName(), err)
			rh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
			continue
		}

		store, err := resource.FromStringToStore(request.GetDatastoreName())
		if err != nil {
			errMsg := fmt.Sprintf("invalid store name [%s]: %s", request.GetDatastoreName(), err)
			rh.l.Error(errMsg)
			responseWriter.Write(writer.LogLevelError, errMsg)
			errNamespaces = append(errNamespaces, request.NamespaceName)
			continue
		}

		var resourceSpecs []*resource.Resource
		for _, resourceProto := range request.GetResources() {
			adapted, err := fromResourceProto(resourceProto, tnnt, store)
			if err != nil {
				errMsg := fmt.Sprintf("error adapting resource [%s]: %s", resourceProto.GetName(), err)
				rh.l.Error(errMsg)
				responseWriter.Write(writer.LogLevelError, errMsg)
				continue
			}
			resourceSpecs = append(resourceSpecs, adapted)
		}
		if len(resourceSpecs) != len(request.GetResources()) {
			errNamespaces = append(errNamespaces, request.GetNamespaceName())
		}

		err = rh.service.Deploy(stream.Context(), tnnt, store, resourceSpecs, responseWriter)
		successResources := getResourcesByStatuses(resourceSpecs, resource.StatusSuccess)
		skippedResources := getResourcesByStatuses(resourceSpecs, resource.StatusSkipped)
		failureResources := getResourcesByStatuses(resourceSpecs, resource.StatusCreateFailure, resource.StatusUpdateFailure, resource.StatusValidationFailure)

		writeResourcesStatus(successResources, func(msg string) {
			responseWriter.Write(writer.LogLevelInfo, msg)
			rh.l.Info(msg)
		})
		writeResourcesStatus(skippedResources, func(msg string) {
			responseWriter.Write(writer.LogLevelWarning, msg)
			rh.l.Warn(msg)
		})
		writeResourcesStatus(failureResources, func(msg string) {
			responseWriter.Write(writer.LogLevelError, msg)
			rh.l.Error(msg)
		})
		writeError(responseWriter, err)

		if err != nil {
			errNamespaces = append(errNamespaces, request.GetNamespaceName())
			continue
		}

		successMsg := fmt.Sprintf("[%d] resources with namespace [%s] are deployed successfully", len(resourceSpecs), request.GetNamespaceName())
		responseWriter.Write(writer.LogLevelInfo, successMsg)

		for _, resourceSpec := range resourceSpecs {
			raiseResourceDatastoreEventMetric(tnnt, resourceSpec.Store().String(), resourceSpec.Kind(), resourceSpec.Status().String())
		}

		processDuration := time.Since(startTime)
		telemetry.NewGauge(metricResourcesUploadAllDuration, map[string]string{
			"project":   tnnt.ProjectName().String(),
			"namespace": tnnt.NamespaceName().String(),
		}).Add(processDuration.Seconds())
	}

	if len(errNamespaces) > 0 {
		namespacesWithError := strings.Join(errNamespaces, ", ")
		rh.l.Error("error when deploying namespaces: [%s]", namespacesWithError)
		return fmt.Errorf("error when deploying: [%s]", namespacesWithError)
	}
	return nil
}

func (rh ResourceHandler) ListResourceSpecification(ctx context.Context, req *pb.ListResourceSpecificationRequest) (*pb.ListResourceSpecificationResponse, error) {
	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		rh.l.Error("invalid store name [%s]: %s", req.GetDatastoreName(), err)
		return nil, errors.GRPCErr(err, "invalid list resource request")
	}

	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		rh.l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "failed to list resource for "+req.GetDatastoreName())
	}

	resources, err := rh.service.GetAll(ctx, tnnt, store)
	if err != nil {
		rh.l.Error("error getting all resources: %s", err)
		return nil, errors.GRPCErr(err, "failed to list resource for "+req.GetDatastoreName())
	}

	var resourceProtos []*pb.ResourceSpecification
	for _, resourceSpec := range resources {
		resourceProto, err := toResourceProto(resourceSpec)
		if err != nil {
			rh.l.Error("error adapting resource [%s]: %s", resourceSpec.FullName(), err)
			return nil, errors.GRPCErr(err, "failed to parse resource "+resourceSpec.FullName())
		}
		resourceProtos = append(resourceProtos, resourceProto)
	}

	return &pb.ListResourceSpecificationResponse{
		Resources: resourceProtos,
	}, nil
}

func (rh ResourceHandler) CreateResource(ctx context.Context, req *pb.CreateResourceRequest) (*pb.CreateResourceResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		rh.l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "failed to create resource")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		rh.l.Error("invalid datastore name [%s]: %s", req.GetDatastoreName(), err)
		return nil, errors.GRPCErr(err, "invalid create resource request")
	}

	res, err := fromResourceProto(req.Resource, tnnt, store)
	if err != nil {
		rh.l.Error("error adapting resource [%s]: %s", req.GetResource().GetName(), err)
		return nil, errors.GRPCErr(err, "failed to create resource")
	}

	err = rh.service.Create(ctx, res)
	raiseResourceDatastoreEventMetric(tnnt, res.Store().String(), res.Kind(), res.Status().String())
	if err != nil {
		rh.l.Error("error creating resource [%s]: %s", res.FullName(), err)
		return nil, errors.GRPCErr(err, "failed to create resource "+res.FullName())
	}

	return &pb.CreateResourceResponse{}, nil
}

func (rh ResourceHandler) ReadResource(ctx context.Context, req *pb.ReadResourceRequest) (*pb.ReadResourceResponse, error) {
	if req.GetResourceName() == "" {
		rh.l.Error("resource name is empty")
		return nil, errors.GRPCErr(errors.InvalidArgument(resource.EntityResource, "empty resource name"), "invalid read resource request")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		rh.l.Error("invalid datastore name [%s]: %s", req.GetDatastoreName(), err)
		return nil, errors.GRPCErr(err, "invalid read resource request")
	}

	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		rh.l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "failed to read resource "+req.GetResourceName())
	}

	response, err := rh.service.Get(ctx, tnnt, store, req.GetResourceName())
	if err != nil {
		rh.l.Error("error getting resource [%s]: %s", req.GetResourceName(), err)
		return nil, errors.GRPCErr(err, "failed to read resource "+req.GetResourceName())
	}

	protoResource, err := toResourceProto(response)
	if err != nil {
		rh.l.Error("error adapting resource [%s]: %s", req.GetResourceName(), err)
		return nil, errors.GRPCErr(err, "failed to read resource "+req.GetResourceName())
	}

	return &pb.ReadResourceResponse{
		Resource: protoResource,
	}, nil
}

func (rh ResourceHandler) UpdateResource(ctx context.Context, req *pb.UpdateResourceRequest) (*pb.UpdateResourceResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		rh.l.Error("invalid tenant information request project [%s] namespace [%s]: %s", req.GetProjectName(), req.GetNamespaceName(), err)
		return nil, errors.GRPCErr(err, "failed to update resource")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		rh.l.Error("invalid datastore name [%s]: %s", req.GetDatastoreName(), err)
		return nil, errors.GRPCErr(err, "invalid update resource request")
	}

	res, err := fromResourceProto(req.Resource, tnnt, store)
	if err != nil {
		rh.l.Error("error adapting resource [%s]: %s", req.GetResource().GetName(), err)
		return nil, errors.GRPCErr(err, "failed to update resource")
	}

	logWriter := writer.NewLogWriter(rh.l)

	err = rh.service.Update(ctx, res, logWriter)
	raiseResourceDatastoreEventMetric(tnnt, res.Store().String(), res.Kind(), res.Status().String())
	if err != nil {
		rh.l.Error("error updating resource [%s]: %s", res.FullName(), err)
		return nil, errors.GRPCErr(err, "failed to update resource "+res.FullName())
	}

	return &pb.UpdateResourceResponse{}, nil
}

func (rh ResourceHandler) ChangeResourceNamespace(ctx context.Context, req *pb.ChangeResourceNamespaceRequest) (*pb.ChangeResourceNamespaceResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to adapt to existing tenant details")
	}

	newTnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNewNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to adapt to new tenant")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid Datastore Name")
	}

	err = rh.service.ChangeNamespace(ctx, store, req.GetResourceName(), tnnt, newTnnt)
	if err != nil {
		return nil, errors.GRPCErr(err, "failed to update resource "+req.GetResourceName())
	}

	telemetry.NewCounter("resource_namespace_migrations_total", map[string]string{
		"project":               tnnt.ProjectName().String(),
		"namespace_source":      tnnt.NamespaceName().String(),
		"namespace_destination": newTnnt.NamespaceName().String(),
	}).Inc()

	return &pb.ChangeResourceNamespaceResponse{}, nil
}

func (rh ResourceHandler) ApplyResources(ctx context.Context, req *pb.ApplyResourcesRequest) (*pb.ApplyResourcesResponse, error) {
	tnnt, err := tenant.NewTenant(req.GetProjectName(), req.GetNamespaceName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid tenant details")
	}

	store, err := resource.FromStringToStore(req.GetDatastoreName())
	if err != nil {
		return nil, errors.GRPCErr(err, "invalid datastore Name")
	}

	if len(req.ResourceNames) == 0 {
		return nil, errors.GRPCErr(errors.InvalidArgument(resource.EntityResource, "empty resource names"), "unable to apply resources")
	}

	statuses, err := rh.service.SyncResources(ctx, tnnt, store, req.ResourceNames)
	if err != nil {
		return nil, errors.GRPCErr(err, "unable to sync to datastore")
	}

	var respStatuses []*pb.ApplyResourcesResponse_ResourceStatus
	for _, r := range statuses.ResourceNames {
		respStatuses = append(respStatuses, &pb.ApplyResourcesResponse_ResourceStatus{
			ResourceName: r,
			Status:       "success",
		})
	}
	for _, r := range statuses.IgnoredResources {
		respStatuses = append(respStatuses, &pb.ApplyResourcesResponse_ResourceStatus{
			ResourceName: r.Name,
			Status:       "failure",
			Reason:       r.Reason,
		})
	}
	return &pb.ApplyResourcesResponse{Statuses: respStatuses}, nil
}

func writeError(logWriter writer.LogWriter, err error) {
	if err == nil {
		return
	}
	var me *errors.MultiError
	if errors.As(err, &me) {
		for _, e := range me.Errors {
			writeError(logWriter, e)
		}
	} else {
		var de *errors.DomainError
		if errors.As(err, &de) {
			logWriter.Write(writer.LogLevelError, de.DebugString())
		} else {
			logWriter.Write(writer.LogLevelError, err.Error())
		}
	}
}

func writeResourcesStatus(resources []*resource.Resource, writeFn func(msg string)) {
	for _, r := range resources {
		msg := fmt.Sprintf("[%s] %s", r.Status(), r.FullName())
		writeFn(msg)
	}
}

func getResourcesByStatuses(resources []*resource.Resource, statuses ...resource.Status) []*resource.Resource {
	acceptedStatus := make(map[resource.Status]bool)
	for _, s := range statuses {
		acceptedStatus[s] = true
	}
	var output []*resource.Resource
	for _, r := range resources {
		if acceptedStatus[r.Status()] {
			output = append(output, r)
		}
	}
	return output
}

func fromResourceProto(rs *pb.ResourceSpecification, tnnt tenant.Tenant, store resource.Store) (*resource.Resource, error) {
	if rs == nil {
		return nil, errors.InvalidArgument(resource.EntityResource, "empty resource")
	}

	if rs.GetSpec() == nil {
		return nil, errors.InvalidArgument(resource.EntityResource, "empty resource spec for "+rs.Name)
	}

	if rs.GetType() == "" {
		return nil, errors.InvalidArgument(resource.EntityResource, "empty resource type for "+rs.Name)
	}

	spec := rs.GetSpec().AsMap()

	var description string
	if protoSpecField, ok := rs.Spec.Fields["description"]; ok {
		description = strings.TrimSpace(protoSpecField.GetStringValue())
	}
	metadata := resource.Metadata{
		Version:     rs.Version,
		Description: description,
		Labels:      rs.Labels,
	}

	return resource.NewResource(rs.Name, rs.GetType(), store, tnnt, &metadata, spec)
}

func toResourceProto(res *resource.Resource) (*pb.ResourceSpecification, error) {
	meta := res.Metadata()
	if meta == nil {
		return nil, errors.InvalidArgument(resource.EntityResource, "missing resource metadata")
	}

	pbStruct, err := structpb.NewStruct(res.Spec())
	if err != nil {
		return nil, errors.InvalidArgument(resource.EntityResource, "unable to convert spec to proto struct")
	}

	return &pb.ResourceSpecification{
		Version: meta.Version,
		Name:    res.FullName(),
		Type:    res.Kind(),
		Spec:    pbStruct,
		Assets:  nil,
		Labels:  meta.Labels,
	}, nil
}

func raiseResourceDatastoreEventMetric(jobTenant tenant.Tenant, datastoreName, resourceKind, state string) {
	telemetry.NewCounter(metricResourceEvents, map[string]string{
		"project":   jobTenant.ProjectName().String(),
		"namespace": jobTenant.NamespaceName().String(),
		"datastore": datastoreName,
		"type":      resourceKind,
		"status":    state,
	}).Inc()
}

func NewResourceHandler(l log.Logger, resourceService ResourceService) *ResourceHandler {
	return &ResourceHandler{
		l:       l,
		service: resourceService,
	}
}
