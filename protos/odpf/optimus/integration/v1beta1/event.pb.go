// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        (unknown)
// source: odpf/optimus/integration/v1beta1/event.proto

package optimus

import (
	v1beta1 "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type OptimusChangeEvent_EventType int32

const (
	OptimusChangeEvent_EVENT_TYPE_TYPE_UNSPECIFIED  OptimusChangeEvent_EventType = 0
	OptimusChangeEvent_EVENT_TYPE_RESOURCE_CREATE   OptimusChangeEvent_EventType = 1
	OptimusChangeEvent_EVENT_TYPE_RESOURCE_UPDATE   OptimusChangeEvent_EventType = 2
	OptimusChangeEvent_EVENT_TYPE_JOB_CREATE        OptimusChangeEvent_EventType = 3
	OptimusChangeEvent_EVENT_TYPE_JOB_UPDATE        OptimusChangeEvent_EventType = 4
	OptimusChangeEvent_EVENT_TYPE_JOB_DELETE        OptimusChangeEvent_EventType = 5
	OptimusChangeEvent_EVENT_TYPE_JOB_WAIT_UPSTREAM OptimusChangeEvent_EventType = 6
	OptimusChangeEvent_EVENT_TYPE_JOB_IN_PROGRESS   OptimusChangeEvent_EventType = 7
	OptimusChangeEvent_EVENT_TYPE_JOB_SUCCESS       OptimusChangeEvent_EventType = 8
	OptimusChangeEvent_EVENT_TYPE_JOB_FAILURE       OptimusChangeEvent_EventType = 9
)

// Enum value maps for OptimusChangeEvent_EventType.
var (
	OptimusChangeEvent_EventType_name = map[int32]string{
		0: "EVENT_TYPE_TYPE_UNSPECIFIED",
		1: "EVENT_TYPE_RESOURCE_CREATE",
		2: "EVENT_TYPE_RESOURCE_UPDATE",
		3: "EVENT_TYPE_JOB_CREATE",
		4: "EVENT_TYPE_JOB_UPDATE",
		5: "EVENT_TYPE_JOB_DELETE",
		6: "EVENT_TYPE_JOB_WAIT_UPSTREAM",
		7: "EVENT_TYPE_JOB_IN_PROGRESS",
		8: "EVENT_TYPE_JOB_SUCCESS",
		9: "EVENT_TYPE_JOB_FAILURE",
	}
	OptimusChangeEvent_EventType_value = map[string]int32{
		"EVENT_TYPE_TYPE_UNSPECIFIED":  0,
		"EVENT_TYPE_RESOURCE_CREATE":   1,
		"EVENT_TYPE_RESOURCE_UPDATE":   2,
		"EVENT_TYPE_JOB_CREATE":        3,
		"EVENT_TYPE_JOB_UPDATE":        4,
		"EVENT_TYPE_JOB_DELETE":        5,
		"EVENT_TYPE_JOB_WAIT_UPSTREAM": 6,
		"EVENT_TYPE_JOB_IN_PROGRESS":   7,
		"EVENT_TYPE_JOB_SUCCESS":       8,
		"EVENT_TYPE_JOB_FAILURE":       9,
	}
)

func (x OptimusChangeEvent_EventType) Enum() *OptimusChangeEvent_EventType {
	p := new(OptimusChangeEvent_EventType)
	*p = x
	return p
}

func (x OptimusChangeEvent_EventType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (OptimusChangeEvent_EventType) Descriptor() protoreflect.EnumDescriptor {
	return file_odpf_optimus_integration_v1beta1_event_proto_enumTypes[0].Descriptor()
}

func (OptimusChangeEvent_EventType) Type() protoreflect.EnumType {
	return &file_odpf_optimus_integration_v1beta1_event_proto_enumTypes[0]
}

func (x OptimusChangeEvent_EventType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use OptimusChangeEvent_EventType.Descriptor instead.
func (OptimusChangeEvent_EventType) EnumDescriptor() ([]byte, []int) {
	return file_odpf_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{3, 0}
}

type ResourceChangePayload struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DatastoreName string                         `protobuf:"bytes,1,opt,name=datastore_name,json=datastoreName,proto3" json:"datastore_name,omitempty"`
	Resource      *v1beta1.ResourceSpecification `protobuf:"bytes,2,opt,name=resource,proto3" json:"resource,omitempty"`
}

func (x *ResourceChangePayload) Reset() {
	*x = ResourceChangePayload{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ResourceChangePayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ResourceChangePayload) ProtoMessage() {}

func (x *ResourceChangePayload) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ResourceChangePayload.ProtoReflect.Descriptor instead.
func (*ResourceChangePayload) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{0}
}

func (x *ResourceChangePayload) GetDatastoreName() string {
	if x != nil {
		return x.DatastoreName
	}
	return ""
}

func (x *ResourceChangePayload) GetResource() *v1beta1.ResourceSpecification {
	if x != nil {
		return x.Resource
	}
	return nil
}

type JobChangePayload struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	JobName string                    `protobuf:"bytes,1,opt,name=job_name,json=jobName,proto3" json:"job_name,omitempty"`
	JobSpec *v1beta1.JobSpecification `protobuf:"bytes,2,opt,name=job_spec,json=jobSpec,proto3" json:"job_spec,omitempty"`
}

func (x *JobChangePayload) Reset() {
	*x = JobChangePayload{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JobChangePayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JobChangePayload) ProtoMessage() {}

func (x *JobChangePayload) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JobChangePayload.ProtoReflect.Descriptor instead.
func (*JobChangePayload) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{1}
}

func (x *JobChangePayload) GetJobName() string {
	if x != nil {
		return x.JobName
	}
	return ""
}

func (x *JobChangePayload) GetJobSpec() *v1beta1.JobSpecification {
	if x != nil {
		return x.JobSpec
	}
	return nil
}

type JobRunPayload struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	JobName     string                 `protobuf:"bytes,1,opt,name=job_name,json=jobName,proto3" json:"job_name,omitempty"`
	ScheduledAt *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=scheduled_at,json=scheduledAt,proto3" json:"scheduled_at,omitempty"`
	JobRunId    string                 `protobuf:"bytes,3,opt,name=job_run_id,json=jobRunId,proto3" json:"job_run_id,omitempty"`
	StartTime   *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`
}

func (x *JobRunPayload) Reset() {
	*x = JobRunPayload{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JobRunPayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JobRunPayload) ProtoMessage() {}

func (x *JobRunPayload) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JobRunPayload.ProtoReflect.Descriptor instead.
func (*JobRunPayload) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{2}
}

func (x *JobRunPayload) GetJobName() string {
	if x != nil {
		return x.JobName
	}
	return ""
}

func (x *JobRunPayload) GetScheduledAt() *timestamppb.Timestamp {
	if x != nil {
		return x.ScheduledAt
	}
	return nil
}

func (x *JobRunPayload) GetJobRunId() string {
	if x != nil {
		return x.JobRunId
	}
	return ""
}

func (x *JobRunPayload) GetStartTime() *timestamppb.Timestamp {
	if x != nil {
		return x.StartTime
	}
	return nil
}

type OptimusChangeEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	EventId       string                       `protobuf:"bytes,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	OccurredAt    *timestamppb.Timestamp       `protobuf:"bytes,2,opt,name=occurred_at,json=occurredAt,proto3" json:"occurred_at,omitempty"`
	ProjectName   string                       `protobuf:"bytes,3,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	NamespaceName string                       `protobuf:"bytes,4,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
	EventType     OptimusChangeEvent_EventType `protobuf:"varint,5,opt,name=event_type,json=eventType,proto3,enum=odpf.optimus.integration.v1beta1.OptimusChangeEvent_EventType" json:"event_type,omitempty"`
	// Types that are assignable to Payload:
	//
	//	*OptimusChangeEvent_JobChange
	//	*OptimusChangeEvent_ResourceChange
	//	*OptimusChangeEvent_JobRun
	Payload isOptimusChangeEvent_Payload `protobuf_oneof:"payload"`
}

func (x *OptimusChangeEvent) Reset() {
	*x = OptimusChangeEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OptimusChangeEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OptimusChangeEvent) ProtoMessage() {}

func (x *OptimusChangeEvent) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OptimusChangeEvent.ProtoReflect.Descriptor instead.
func (*OptimusChangeEvent) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{3}
}

func (x *OptimusChangeEvent) GetEventId() string {
	if x != nil {
		return x.EventId
	}
	return ""
}

func (x *OptimusChangeEvent) GetOccurredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.OccurredAt
	}
	return nil
}

func (x *OptimusChangeEvent) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *OptimusChangeEvent) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

func (x *OptimusChangeEvent) GetEventType() OptimusChangeEvent_EventType {
	if x != nil {
		return x.EventType
	}
	return OptimusChangeEvent_EVENT_TYPE_TYPE_UNSPECIFIED
}

func (m *OptimusChangeEvent) GetPayload() isOptimusChangeEvent_Payload {
	if m != nil {
		return m.Payload
	}
	return nil
}

func (x *OptimusChangeEvent) GetJobChange() *JobChangePayload {
	if x, ok := x.GetPayload().(*OptimusChangeEvent_JobChange); ok {
		return x.JobChange
	}
	return nil
}

func (x *OptimusChangeEvent) GetResourceChange() *ResourceChangePayload {
	if x, ok := x.GetPayload().(*OptimusChangeEvent_ResourceChange); ok {
		return x.ResourceChange
	}
	return nil
}

func (x *OptimusChangeEvent) GetJobRun() *JobRunPayload {
	if x, ok := x.GetPayload().(*OptimusChangeEvent_JobRun); ok {
		return x.JobRun
	}
	return nil
}

type isOptimusChangeEvent_Payload interface {
	isOptimusChangeEvent_Payload()
}

type OptimusChangeEvent_JobChange struct {
	JobChange *JobChangePayload `protobuf:"bytes,6,opt,name=job_change,json=jobChange,proto3,oneof"`
}

type OptimusChangeEvent_ResourceChange struct {
	ResourceChange *ResourceChangePayload `protobuf:"bytes,7,opt,name=resource_change,json=resourceChange,proto3,oneof"`
}

type OptimusChangeEvent_JobRun struct {
	JobRun *JobRunPayload `protobuf:"bytes,8,opt,name=job_run,json=jobRun,proto3,oneof"`
}

func (*OptimusChangeEvent_JobChange) isOptimusChangeEvent_Payload() {}

func (*OptimusChangeEvent_ResourceChange) isOptimusChangeEvent_Payload() {}

func (*OptimusChangeEvent_JobRun) isOptimusChangeEvent_Payload() {}

var File_odpf_optimus_integration_v1beta1_event_proto protoreflect.FileDescriptor

var file_odpf_optimus_integration_v1beta1_event_proto_rawDesc = []byte{
	0x0a, 0x2c, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2f, 0x69,
	0x6e, 0x74, 0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74,
	0x61, 0x31, 0x2f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x20,
	0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74,
	0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31,
	0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x28, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2f,
	0x63, 0x6f, 0x72, 0x65, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x72, 0x65, 0x73,
	0x6f, 0x75, 0x72, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x28, 0x6f, 0x64, 0x70,
	0x66, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2f, 0x63, 0x6f, 0x72, 0x65, 0x2f, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x6a, 0x6f, 0x62, 0x5f, 0x73, 0x70, 0x65, 0x63, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x8c, 0x01, 0x0a, 0x15, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72,
	0x63, 0x65, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12,
	0x25, 0x0a, 0x0e, 0x64, 0x61, 0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x5f, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x64, 0x61, 0x74, 0x61, 0x73, 0x74, 0x6f,
	0x72, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x4c, 0x0a, 0x08, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72,
	0x63, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x30, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e,
	0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62,
	0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x53, 0x70, 0x65,
	0x63, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x08, 0x72, 0x65, 0x73, 0x6f,
	0x75, 0x72, 0x63, 0x65, 0x22, 0x75, 0x0a, 0x10, 0x4a, 0x6f, 0x62, 0x43, 0x68, 0x61, 0x6e, 0x67,
	0x65, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x6a, 0x6f, 0x62, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6a, 0x6f, 0x62, 0x4e,
	0x61, 0x6d, 0x65, 0x12, 0x46, 0x0a, 0x08, 0x6a, 0x6f, 0x62, 0x5f, 0x73, 0x70, 0x65, 0x63, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2b, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74,
	0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61,
	0x31, 0x2e, 0x4a, 0x6f, 0x62, 0x53, 0x70, 0x65, 0x63, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x52, 0x07, 0x6a, 0x6f, 0x62, 0x53, 0x70, 0x65, 0x63, 0x22, 0xc2, 0x01, 0x0a, 0x0d,
	0x4a, 0x6f, 0x62, 0x52, 0x75, 0x6e, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x19, 0x0a,
	0x08, 0x6a, 0x6f, 0x62, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x07, 0x6a, 0x6f, 0x62, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x3d, 0x0a, 0x0c, 0x73, 0x63, 0x68, 0x65,
	0x64, 0x75, 0x6c, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0b, 0x73, 0x63, 0x68, 0x65,
	0x64, 0x75, 0x6c, 0x65, 0x64, 0x41, 0x74, 0x12, 0x1c, 0x0a, 0x0a, 0x6a, 0x6f, 0x62, 0x5f, 0x72,
	0x75, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x6a, 0x6f, 0x62,
	0x52, 0x75, 0x6e, 0x49, 0x64, 0x12, 0x39, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74,
	0x69, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x69, 0x6d, 0x65,
	0x22, 0xdf, 0x06, 0x0a, 0x12, 0x4f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x43, 0x68, 0x61, 0x6e,
	0x67, 0x65, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x12, 0x19, 0x0a, 0x08, 0x65, 0x76, 0x65, 0x6e, 0x74,
	0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x65, 0x76, 0x65, 0x6e, 0x74,
	0x49, 0x64, 0x12, 0x3b, 0x0a, 0x0b, 0x6f, 0x63, 0x63, 0x75, 0x72, 0x72, 0x65, 0x64, 0x5f, 0x61,
	0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x52, 0x0a, 0x6f, 0x63, 0x63, 0x75, 0x72, 0x72, 0x65, 0x64, 0x41, 0x74, 0x12,
	0x21, 0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61,
	0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65,
	0x73, 0x70, 0x61, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x5d, 0x0a, 0x0a, 0x65, 0x76, 0x65,
	0x6e, 0x74, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x3e, 0x2e,
	0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74,
	0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31,
	0x2e, 0x4f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x45, 0x76,
	0x65, 0x6e, 0x74, 0x2e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x54, 0x79, 0x70, 0x65, 0x52, 0x09, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x54, 0x79, 0x70, 0x65, 0x12, 0x53, 0x0a, 0x0a, 0x6a, 0x6f, 0x62, 0x5f,
	0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x32, 0x2e, 0x6f,
	0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74, 0x65,
	0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e,
	0x4a, 0x6f, 0x62, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64,
	0x48, 0x00, 0x52, 0x09, 0x6a, 0x6f, 0x62, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x62, 0x0a,
	0x0f, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65,
	0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x37, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72,
	0x63, 0x65, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x48,
	0x00, 0x52, 0x0e, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x43, 0x68, 0x61, 0x6e, 0x67,
	0x65, 0x12, 0x4a, 0x0a, 0x07, 0x6a, 0x6f, 0x62, 0x5f, 0x72, 0x75, 0x6e, 0x18, 0x08, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x2f, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75,
	0x73, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31,
	0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4a, 0x6f, 0x62, 0x52, 0x75, 0x6e, 0x50, 0x61, 0x79, 0x6c,
	0x6f, 0x61, 0x64, 0x48, 0x00, 0x52, 0x06, 0x6a, 0x6f, 0x62, 0x52, 0x75, 0x6e, 0x22, 0xb7, 0x02,
	0x0a, 0x09, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1f, 0x0a, 0x1b, 0x45,
	0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x55,
	0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x1e, 0x0a, 0x1a,
	0x45, 0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x52, 0x45, 0x53, 0x4f, 0x55,
	0x52, 0x43, 0x45, 0x5f, 0x43, 0x52, 0x45, 0x41, 0x54, 0x45, 0x10, 0x01, 0x12, 0x1e, 0x0a, 0x1a,
	0x45, 0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x52, 0x45, 0x53, 0x4f, 0x55,
	0x52, 0x43, 0x45, 0x5f, 0x55, 0x50, 0x44, 0x41, 0x54, 0x45, 0x10, 0x02, 0x12, 0x19, 0x0a, 0x15,
	0x45, 0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x43,
	0x52, 0x45, 0x41, 0x54, 0x45, 0x10, 0x03, 0x12, 0x19, 0x0a, 0x15, 0x45, 0x56, 0x45, 0x4e, 0x54,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x55, 0x50, 0x44, 0x41, 0x54, 0x45,
	0x10, 0x04, 0x12, 0x19, 0x0a, 0x15, 0x45, 0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45,
	0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x44, 0x45, 0x4c, 0x45, 0x54, 0x45, 0x10, 0x05, 0x12, 0x20, 0x0a,
	0x1c, 0x45, 0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f,
	0x57, 0x41, 0x49, 0x54, 0x5f, 0x55, 0x50, 0x53, 0x54, 0x52, 0x45, 0x41, 0x4d, 0x10, 0x06, 0x12,
	0x1e, 0x0a, 0x1a, 0x45, 0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f,
	0x42, 0x5f, 0x49, 0x4e, 0x5f, 0x50, 0x52, 0x4f, 0x47, 0x52, 0x45, 0x53, 0x53, 0x10, 0x07, 0x12,
	0x1a, 0x0a, 0x16, 0x45, 0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f,
	0x42, 0x5f, 0x53, 0x55, 0x43, 0x43, 0x45, 0x53, 0x53, 0x10, 0x08, 0x12, 0x1a, 0x0a, 0x16, 0x45,
	0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x46, 0x41,
	0x49, 0x4c, 0x55, 0x52, 0x45, 0x10, 0x09, 0x42, 0x09, 0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f,
	0x61, 0x64, 0x42, 0x41, 0x0a, 0x16, 0x69, 0x6f, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x6e, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x42, 0x05, 0x45, 0x76,
	0x65, 0x6e, 0x74, 0x50, 0x01, 0x5a, 0x1e, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2f, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_odpf_optimus_integration_v1beta1_event_proto_rawDescOnce sync.Once
	file_odpf_optimus_integration_v1beta1_event_proto_rawDescData = file_odpf_optimus_integration_v1beta1_event_proto_rawDesc
)

func file_odpf_optimus_integration_v1beta1_event_proto_rawDescGZIP() []byte {
	file_odpf_optimus_integration_v1beta1_event_proto_rawDescOnce.Do(func() {
		file_odpf_optimus_integration_v1beta1_event_proto_rawDescData = protoimpl.X.CompressGZIP(file_odpf_optimus_integration_v1beta1_event_proto_rawDescData)
	})
	return file_odpf_optimus_integration_v1beta1_event_proto_rawDescData
}

var file_odpf_optimus_integration_v1beta1_event_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_odpf_optimus_integration_v1beta1_event_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_odpf_optimus_integration_v1beta1_event_proto_goTypes = []interface{}{
	(OptimusChangeEvent_EventType)(0),     // 0: odpf.optimus.integration.v1beta1.OptimusChangeEvent.EventType
	(*ResourceChangePayload)(nil),         // 1: odpf.optimus.integration.v1beta1.ResourceChangePayload
	(*JobChangePayload)(nil),              // 2: odpf.optimus.integration.v1beta1.JobChangePayload
	(*JobRunPayload)(nil),                 // 3: odpf.optimus.integration.v1beta1.JobRunPayload
	(*OptimusChangeEvent)(nil),            // 4: odpf.optimus.integration.v1beta1.OptimusChangeEvent
	(*v1beta1.ResourceSpecification)(nil), // 5: odpf.optimus.core.v1beta1.ResourceSpecification
	(*v1beta1.JobSpecification)(nil),      // 6: odpf.optimus.core.v1beta1.JobSpecification
	(*timestamppb.Timestamp)(nil),         // 7: google.protobuf.Timestamp
}
var file_odpf_optimus_integration_v1beta1_event_proto_depIdxs = []int32{
	5, // 0: odpf.optimus.integration.v1beta1.ResourceChangePayload.resource:type_name -> odpf.optimus.core.v1beta1.ResourceSpecification
	6, // 1: odpf.optimus.integration.v1beta1.JobChangePayload.job_spec:type_name -> odpf.optimus.core.v1beta1.JobSpecification
	7, // 2: odpf.optimus.integration.v1beta1.JobRunPayload.scheduled_at:type_name -> google.protobuf.Timestamp
	7, // 3: odpf.optimus.integration.v1beta1.JobRunPayload.start_time:type_name -> google.protobuf.Timestamp
	7, // 4: odpf.optimus.integration.v1beta1.OptimusChangeEvent.occurred_at:type_name -> google.protobuf.Timestamp
	0, // 5: odpf.optimus.integration.v1beta1.OptimusChangeEvent.event_type:type_name -> odpf.optimus.integration.v1beta1.OptimusChangeEvent.EventType
	2, // 6: odpf.optimus.integration.v1beta1.OptimusChangeEvent.job_change:type_name -> odpf.optimus.integration.v1beta1.JobChangePayload
	1, // 7: odpf.optimus.integration.v1beta1.OptimusChangeEvent.resource_change:type_name -> odpf.optimus.integration.v1beta1.ResourceChangePayload
	3, // 8: odpf.optimus.integration.v1beta1.OptimusChangeEvent.job_run:type_name -> odpf.optimus.integration.v1beta1.JobRunPayload
	9, // [9:9] is the sub-list for method output_type
	9, // [9:9] is the sub-list for method input_type
	9, // [9:9] is the sub-list for extension type_name
	9, // [9:9] is the sub-list for extension extendee
	0, // [0:9] is the sub-list for field type_name
}

func init() { file_odpf_optimus_integration_v1beta1_event_proto_init() }
func file_odpf_optimus_integration_v1beta1_event_proto_init() {
	if File_odpf_optimus_integration_v1beta1_event_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ResourceChangePayload); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JobChangePayload); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JobRunPayload); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OptimusChangeEvent); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_odpf_optimus_integration_v1beta1_event_proto_msgTypes[3].OneofWrappers = []interface{}{
		(*OptimusChangeEvent_JobChange)(nil),
		(*OptimusChangeEvent_ResourceChange)(nil),
		(*OptimusChangeEvent_JobRun)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_odpf_optimus_integration_v1beta1_event_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_odpf_optimus_integration_v1beta1_event_proto_goTypes,
		DependencyIndexes: file_odpf_optimus_integration_v1beta1_event_proto_depIdxs,
		EnumInfos:         file_odpf_optimus_integration_v1beta1_event_proto_enumTypes,
		MessageInfos:      file_odpf_optimus_integration_v1beta1_event_proto_msgTypes,
	}.Build()
	File_odpf_optimus_integration_v1beta1_event_proto = out.File
	file_odpf_optimus_integration_v1beta1_event_proto_rawDesc = nil
	file_odpf_optimus_integration_v1beta1_event_proto_goTypes = nil
	file_odpf_optimus_integration_v1beta1_event_proto_depIdxs = nil
}
