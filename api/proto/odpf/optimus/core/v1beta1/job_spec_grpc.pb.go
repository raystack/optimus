// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             (unknown)
// source: odpf/optimus/core/v1beta1/job_spec.proto

package optimus

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// JobSpecificationServiceClient is the client API for JobSpecificationService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type JobSpecificationServiceClient interface {
	// DeployJobSpecification schedules jobs for execution
	// returns a stream of messages which can be used to track the progress
	// of deployments. Message containing ack are status events other are progress
	// events
	// State of the world request
	DeployJobSpecification(ctx context.Context, opts ...grpc.CallOption) (JobSpecificationService_DeployJobSpecificationClient, error)
	// JobExplain return a new jobSpec for a namespace which belongs to a project
	JobExplain(ctx context.Context, in *JobExplainRequest, opts ...grpc.CallOption) (*JobExplainResponse, error)
	// CreateJobSpecification registers a new job for a namespace which belongs to a project
	CreateJobSpecification(ctx context.Context, in *CreateJobSpecificationRequest, opts ...grpc.CallOption) (*CreateJobSpecificationResponse, error)
	// GetJobSpecification reads a provided job spec of a namespace
	GetJobSpecification(ctx context.Context, in *GetJobSpecificationRequest, opts ...grpc.CallOption) (*GetJobSpecificationResponse, error)
	// GetJobSpecifications read a job spec for provided filters
	GetJobSpecifications(ctx context.Context, in *GetJobSpecificationsRequest, opts ...grpc.CallOption) (*GetJobSpecificationsResponse, error)
	// DeleteJobSpecification deletes a job spec of a namespace
	DeleteJobSpecification(ctx context.Context, in *DeleteJobSpecificationRequest, opts ...grpc.CallOption) (*DeleteJobSpecificationResponse, error)
	// ListJobSpecification returns list of jobs created in a project
	ListJobSpecification(ctx context.Context, in *ListJobSpecificationRequest, opts ...grpc.CallOption) (*ListJobSpecificationResponse, error)
	// CheckJobSpecification checks if a job specification is valid
	CheckJobSpecification(ctx context.Context, in *CheckJobSpecificationRequest, opts ...grpc.CallOption) (*CheckJobSpecificationResponse, error)
	// CheckJobSpecifications checks if the job specifications are valid
	CheckJobSpecifications(ctx context.Context, in *CheckJobSpecificationsRequest, opts ...grpc.CallOption) (JobSpecificationService_CheckJobSpecificationsClient, error)
	// RefreshJobs do redeployment using the current persisted state.
	// It will returns a stream of messages which can be used to track the progress.
	RefreshJobs(ctx context.Context, in *RefreshJobsRequest, opts ...grpc.CallOption) (JobSpecificationService_RefreshJobsClient, error)
	// GetDeployJobsStatus check status of job deployment.
	// It will returns status of the job deployment and the failure details.
	GetDeployJobsStatus(ctx context.Context, in *GetDeployJobsStatusRequest, opts ...grpc.CallOption) (*GetDeployJobsStatusResponse, error)
}

type jobSpecificationServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewJobSpecificationServiceClient(cc grpc.ClientConnInterface) JobSpecificationServiceClient {
	return &jobSpecificationServiceClient{cc}
}

func (c *jobSpecificationServiceClient) DeployJobSpecification(ctx context.Context, opts ...grpc.CallOption) (JobSpecificationService_DeployJobSpecificationClient, error) {
	stream, err := c.cc.NewStream(ctx, &JobSpecificationService_ServiceDesc.Streams[0], "/odpf.optimus.core.v1beta1.JobSpecificationService/DeployJobSpecification", opts...)
	if err != nil {
		return nil, err
	}
	x := &jobSpecificationServiceDeployJobSpecificationClient{stream}
	return x, nil
}

type JobSpecificationService_DeployJobSpecificationClient interface {
	Send(*DeployJobSpecificationRequest) error
	Recv() (*DeployJobSpecificationResponse, error)
	grpc.ClientStream
}

type jobSpecificationServiceDeployJobSpecificationClient struct {
	grpc.ClientStream
}

func (x *jobSpecificationServiceDeployJobSpecificationClient) Send(m *DeployJobSpecificationRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *jobSpecificationServiceDeployJobSpecificationClient) Recv() (*DeployJobSpecificationResponse, error) {
	m := new(DeployJobSpecificationResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *jobSpecificationServiceClient) JobExplain(ctx context.Context, in *JobExplainRequest, opts ...grpc.CallOption) (*JobExplainResponse, error) {
	out := new(JobExplainResponse)
	err := c.cc.Invoke(ctx, "/odpf.optimus.core.v1beta1.JobSpecificationService/JobExplain", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobSpecificationServiceClient) CreateJobSpecification(ctx context.Context, in *CreateJobSpecificationRequest, opts ...grpc.CallOption) (*CreateJobSpecificationResponse, error) {
	out := new(CreateJobSpecificationResponse)
	err := c.cc.Invoke(ctx, "/odpf.optimus.core.v1beta1.JobSpecificationService/CreateJobSpecification", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobSpecificationServiceClient) GetJobSpecification(ctx context.Context, in *GetJobSpecificationRequest, opts ...grpc.CallOption) (*GetJobSpecificationResponse, error) {
	out := new(GetJobSpecificationResponse)
	err := c.cc.Invoke(ctx, "/odpf.optimus.core.v1beta1.JobSpecificationService/GetJobSpecification", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobSpecificationServiceClient) GetJobSpecifications(ctx context.Context, in *GetJobSpecificationsRequest, opts ...grpc.CallOption) (*GetJobSpecificationsResponse, error) {
	out := new(GetJobSpecificationsResponse)
	err := c.cc.Invoke(ctx, "/odpf.optimus.core.v1beta1.JobSpecificationService/GetJobSpecifications", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobSpecificationServiceClient) DeleteJobSpecification(ctx context.Context, in *DeleteJobSpecificationRequest, opts ...grpc.CallOption) (*DeleteJobSpecificationResponse, error) {
	out := new(DeleteJobSpecificationResponse)
	err := c.cc.Invoke(ctx, "/odpf.optimus.core.v1beta1.JobSpecificationService/DeleteJobSpecification", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobSpecificationServiceClient) ListJobSpecification(ctx context.Context, in *ListJobSpecificationRequest, opts ...grpc.CallOption) (*ListJobSpecificationResponse, error) {
	out := new(ListJobSpecificationResponse)
	err := c.cc.Invoke(ctx, "/odpf.optimus.core.v1beta1.JobSpecificationService/ListJobSpecification", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobSpecificationServiceClient) CheckJobSpecification(ctx context.Context, in *CheckJobSpecificationRequest, opts ...grpc.CallOption) (*CheckJobSpecificationResponse, error) {
	out := new(CheckJobSpecificationResponse)
	err := c.cc.Invoke(ctx, "/odpf.optimus.core.v1beta1.JobSpecificationService/CheckJobSpecification", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobSpecificationServiceClient) CheckJobSpecifications(ctx context.Context, in *CheckJobSpecificationsRequest, opts ...grpc.CallOption) (JobSpecificationService_CheckJobSpecificationsClient, error) {
	stream, err := c.cc.NewStream(ctx, &JobSpecificationService_ServiceDesc.Streams[1], "/odpf.optimus.core.v1beta1.JobSpecificationService/CheckJobSpecifications", opts...)
	if err != nil {
		return nil, err
	}
	x := &jobSpecificationServiceCheckJobSpecificationsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type JobSpecificationService_CheckJobSpecificationsClient interface {
	Recv() (*CheckJobSpecificationsResponse, error)
	grpc.ClientStream
}

type jobSpecificationServiceCheckJobSpecificationsClient struct {
	grpc.ClientStream
}

func (x *jobSpecificationServiceCheckJobSpecificationsClient) Recv() (*CheckJobSpecificationsResponse, error) {
	m := new(CheckJobSpecificationsResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *jobSpecificationServiceClient) RefreshJobs(ctx context.Context, in *RefreshJobsRequest, opts ...grpc.CallOption) (JobSpecificationService_RefreshJobsClient, error) {
	stream, err := c.cc.NewStream(ctx, &JobSpecificationService_ServiceDesc.Streams[2], "/odpf.optimus.core.v1beta1.JobSpecificationService/RefreshJobs", opts...)
	if err != nil {
		return nil, err
	}
	x := &jobSpecificationServiceRefreshJobsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type JobSpecificationService_RefreshJobsClient interface {
	Recv() (*RefreshJobsResponse, error)
	grpc.ClientStream
}

type jobSpecificationServiceRefreshJobsClient struct {
	grpc.ClientStream
}

func (x *jobSpecificationServiceRefreshJobsClient) Recv() (*RefreshJobsResponse, error) {
	m := new(RefreshJobsResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *jobSpecificationServiceClient) GetDeployJobsStatus(ctx context.Context, in *GetDeployJobsStatusRequest, opts ...grpc.CallOption) (*GetDeployJobsStatusResponse, error) {
	out := new(GetDeployJobsStatusResponse)
	err := c.cc.Invoke(ctx, "/odpf.optimus.core.v1beta1.JobSpecificationService/GetDeployJobsStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// JobSpecificationServiceServer is the server API for JobSpecificationService service.
// All implementations must embed UnimplementedJobSpecificationServiceServer
// for forward compatibility
type JobSpecificationServiceServer interface {
	// DeployJobSpecification schedules jobs for execution
	// returns a stream of messages which can be used to track the progress
	// of deployments. Message containing ack are status events other are progress
	// events
	// State of the world request
	DeployJobSpecification(JobSpecificationService_DeployJobSpecificationServer) error
	// JobExplain return a new jobSpec for a namespace which belongs to a project
	JobExplain(context.Context, *JobExplainRequest) (*JobExplainResponse, error)
	// CreateJobSpecification registers a new job for a namespace which belongs to a project
	CreateJobSpecification(context.Context, *CreateJobSpecificationRequest) (*CreateJobSpecificationResponse, error)
	// GetJobSpecification reads a provided job spec of a namespace
	GetJobSpecification(context.Context, *GetJobSpecificationRequest) (*GetJobSpecificationResponse, error)
	// GetJobSpecifications read a job spec for provided filters
	GetJobSpecifications(context.Context, *GetJobSpecificationsRequest) (*GetJobSpecificationsResponse, error)
	// DeleteJobSpecification deletes a job spec of a namespace
	DeleteJobSpecification(context.Context, *DeleteJobSpecificationRequest) (*DeleteJobSpecificationResponse, error)
	// ListJobSpecification returns list of jobs created in a project
	ListJobSpecification(context.Context, *ListJobSpecificationRequest) (*ListJobSpecificationResponse, error)
	// CheckJobSpecification checks if a job specification is valid
	CheckJobSpecification(context.Context, *CheckJobSpecificationRequest) (*CheckJobSpecificationResponse, error)
	// CheckJobSpecifications checks if the job specifications are valid
	CheckJobSpecifications(*CheckJobSpecificationsRequest, JobSpecificationService_CheckJobSpecificationsServer) error
	// RefreshJobs do redeployment using the current persisted state.
	// It will returns a stream of messages which can be used to track the progress.
	RefreshJobs(*RefreshJobsRequest, JobSpecificationService_RefreshJobsServer) error
	// GetDeployJobsStatus check status of job deployment.
	// It will returns status of the job deployment and the failure details.
	GetDeployJobsStatus(context.Context, *GetDeployJobsStatusRequest) (*GetDeployJobsStatusResponse, error)
	mustEmbedUnimplementedJobSpecificationServiceServer()
}

// UnimplementedJobSpecificationServiceServer must be embedded to have forward compatible implementations.
type UnimplementedJobSpecificationServiceServer struct {
}

func (UnimplementedJobSpecificationServiceServer) DeployJobSpecification(JobSpecificationService_DeployJobSpecificationServer) error {
	return status.Errorf(codes.Unimplemented, "method DeployJobSpecification not implemented")
}
func (UnimplementedJobSpecificationServiceServer) JobExplain(context.Context, *JobExplainRequest) (*JobExplainResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method JobExplain not implemented")
}
func (UnimplementedJobSpecificationServiceServer) CreateJobSpecification(context.Context, *CreateJobSpecificationRequest) (*CreateJobSpecificationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateJobSpecification not implemented")
}
func (UnimplementedJobSpecificationServiceServer) GetJobSpecification(context.Context, *GetJobSpecificationRequest) (*GetJobSpecificationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJobSpecification not implemented")
}
func (UnimplementedJobSpecificationServiceServer) GetJobSpecifications(context.Context, *GetJobSpecificationsRequest) (*GetJobSpecificationsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJobSpecifications not implemented")
}
func (UnimplementedJobSpecificationServiceServer) DeleteJobSpecification(context.Context, *DeleteJobSpecificationRequest) (*DeleteJobSpecificationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteJobSpecification not implemented")
}
func (UnimplementedJobSpecificationServiceServer) ListJobSpecification(context.Context, *ListJobSpecificationRequest) (*ListJobSpecificationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListJobSpecification not implemented")
}
func (UnimplementedJobSpecificationServiceServer) CheckJobSpecification(context.Context, *CheckJobSpecificationRequest) (*CheckJobSpecificationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CheckJobSpecification not implemented")
}
func (UnimplementedJobSpecificationServiceServer) CheckJobSpecifications(*CheckJobSpecificationsRequest, JobSpecificationService_CheckJobSpecificationsServer) error {
	return status.Errorf(codes.Unimplemented, "method CheckJobSpecifications not implemented")
}
func (UnimplementedJobSpecificationServiceServer) RefreshJobs(*RefreshJobsRequest, JobSpecificationService_RefreshJobsServer) error {
	return status.Errorf(codes.Unimplemented, "method RefreshJobs not implemented")
}
func (UnimplementedJobSpecificationServiceServer) GetDeployJobsStatus(context.Context, *GetDeployJobsStatusRequest) (*GetDeployJobsStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetDeployJobsStatus not implemented")
}
func (UnimplementedJobSpecificationServiceServer) mustEmbedUnimplementedJobSpecificationServiceServer() {
}

// UnsafeJobSpecificationServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to JobSpecificationServiceServer will
// result in compilation errors.
type UnsafeJobSpecificationServiceServer interface {
	mustEmbedUnimplementedJobSpecificationServiceServer()
}

func RegisterJobSpecificationServiceServer(s grpc.ServiceRegistrar, srv JobSpecificationServiceServer) {
	s.RegisterService(&JobSpecificationService_ServiceDesc, srv)
}

func _JobSpecificationService_DeployJobSpecification_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(JobSpecificationServiceServer).DeployJobSpecification(&jobSpecificationServiceDeployJobSpecificationServer{stream})
}

type JobSpecificationService_DeployJobSpecificationServer interface {
	Send(*DeployJobSpecificationResponse) error
	Recv() (*DeployJobSpecificationRequest, error)
	grpc.ServerStream
}

type jobSpecificationServiceDeployJobSpecificationServer struct {
	grpc.ServerStream
}

func (x *jobSpecificationServiceDeployJobSpecificationServer) Send(m *DeployJobSpecificationResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *jobSpecificationServiceDeployJobSpecificationServer) Recv() (*DeployJobSpecificationRequest, error) {
	m := new(DeployJobSpecificationRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _JobSpecificationService_JobExplain_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JobExplainRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobSpecificationServiceServer).JobExplain(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/odpf.optimus.core.v1beta1.JobSpecificationService/JobExplain",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobSpecificationServiceServer).JobExplain(ctx, req.(*JobExplainRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobSpecificationService_CreateJobSpecification_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateJobSpecificationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobSpecificationServiceServer).CreateJobSpecification(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/odpf.optimus.core.v1beta1.JobSpecificationService/CreateJobSpecification",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobSpecificationServiceServer).CreateJobSpecification(ctx, req.(*CreateJobSpecificationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobSpecificationService_GetJobSpecification_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetJobSpecificationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobSpecificationServiceServer).GetJobSpecification(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/odpf.optimus.core.v1beta1.JobSpecificationService/GetJobSpecification",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobSpecificationServiceServer).GetJobSpecification(ctx, req.(*GetJobSpecificationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobSpecificationService_GetJobSpecifications_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetJobSpecificationsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobSpecificationServiceServer).GetJobSpecifications(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/odpf.optimus.core.v1beta1.JobSpecificationService/GetJobSpecifications",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobSpecificationServiceServer).GetJobSpecifications(ctx, req.(*GetJobSpecificationsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobSpecificationService_DeleteJobSpecification_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteJobSpecificationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobSpecificationServiceServer).DeleteJobSpecification(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/odpf.optimus.core.v1beta1.JobSpecificationService/DeleteJobSpecification",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobSpecificationServiceServer).DeleteJobSpecification(ctx, req.(*DeleteJobSpecificationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobSpecificationService_ListJobSpecification_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListJobSpecificationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobSpecificationServiceServer).ListJobSpecification(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/odpf.optimus.core.v1beta1.JobSpecificationService/ListJobSpecification",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobSpecificationServiceServer).ListJobSpecification(ctx, req.(*ListJobSpecificationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobSpecificationService_CheckJobSpecification_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CheckJobSpecificationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobSpecificationServiceServer).CheckJobSpecification(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/odpf.optimus.core.v1beta1.JobSpecificationService/CheckJobSpecification",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobSpecificationServiceServer).CheckJobSpecification(ctx, req.(*CheckJobSpecificationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobSpecificationService_CheckJobSpecifications_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(CheckJobSpecificationsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(JobSpecificationServiceServer).CheckJobSpecifications(m, &jobSpecificationServiceCheckJobSpecificationsServer{stream})
}

type JobSpecificationService_CheckJobSpecificationsServer interface {
	Send(*CheckJobSpecificationsResponse) error
	grpc.ServerStream
}

type jobSpecificationServiceCheckJobSpecificationsServer struct {
	grpc.ServerStream
}

func (x *jobSpecificationServiceCheckJobSpecificationsServer) Send(m *CheckJobSpecificationsResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _JobSpecificationService_RefreshJobs_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(RefreshJobsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(JobSpecificationServiceServer).RefreshJobs(m, &jobSpecificationServiceRefreshJobsServer{stream})
}

type JobSpecificationService_RefreshJobsServer interface {
	Send(*RefreshJobsResponse) error
	grpc.ServerStream
}

type jobSpecificationServiceRefreshJobsServer struct {
	grpc.ServerStream
}

func (x *jobSpecificationServiceRefreshJobsServer) Send(m *RefreshJobsResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _JobSpecificationService_GetDeployJobsStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetDeployJobsStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobSpecificationServiceServer).GetDeployJobsStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/odpf.optimus.core.v1beta1.JobSpecificationService/GetDeployJobsStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobSpecificationServiceServer).GetDeployJobsStatus(ctx, req.(*GetDeployJobsStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// JobSpecificationService_ServiceDesc is the grpc.ServiceDesc for JobSpecificationService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var JobSpecificationService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "odpf.optimus.core.v1beta1.JobSpecificationService",
	HandlerType: (*JobSpecificationServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "JobExplain",
			Handler:    _JobSpecificationService_JobExplain_Handler,
		},
		{
			MethodName: "CreateJobSpecification",
			Handler:    _JobSpecificationService_CreateJobSpecification_Handler,
		},
		{
			MethodName: "GetJobSpecification",
			Handler:    _JobSpecificationService_GetJobSpecification_Handler,
		},
		{
			MethodName: "GetJobSpecifications",
			Handler:    _JobSpecificationService_GetJobSpecifications_Handler,
		},
		{
			MethodName: "DeleteJobSpecification",
			Handler:    _JobSpecificationService_DeleteJobSpecification_Handler,
		},
		{
			MethodName: "ListJobSpecification",
			Handler:    _JobSpecificationService_ListJobSpecification_Handler,
		},
		{
			MethodName: "CheckJobSpecification",
			Handler:    _JobSpecificationService_CheckJobSpecification_Handler,
		},
		{
			MethodName: "GetDeployJobsStatus",
			Handler:    _JobSpecificationService_GetDeployJobsStatus_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "DeployJobSpecification",
			Handler:       _JobSpecificationService_DeployJobSpecification_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "CheckJobSpecifications",
			Handler:       _JobSpecificationService_CheckJobSpecifications_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "RefreshJobs",
			Handler:       _JobSpecificationService_RefreshJobs_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "odpf/optimus/core/v1beta1/job_spec.proto",
}
