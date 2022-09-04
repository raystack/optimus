// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        (unknown)
// source: odpf/optimus/core/v1beta1/secret.proto

package optimus

import (
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2/options"
	_ "google.golang.org/genproto/googleapis/api/annotations"
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

type RegisterSecretRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName   string `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	SecretName    string `protobuf:"bytes,2,opt,name=secret_name,json=secretName,proto3" json:"secret_name,omitempty"`
	Value         string `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"` // base64 encoded secret value
	NamespaceName string `protobuf:"bytes,4,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
}

func (x *RegisterSecretRequest) Reset() {
	*x = RegisterSecretRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RegisterSecretRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterSecretRequest) ProtoMessage() {}

func (x *RegisterSecretRequest) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RegisterSecretRequest.ProtoReflect.Descriptor instead.
func (*RegisterSecretRequest) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_core_v1beta1_secret_proto_rawDescGZIP(), []int{0}
}

func (x *RegisterSecretRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *RegisterSecretRequest) GetSecretName() string {
	if x != nil {
		return x.SecretName
	}
	return ""
}

func (x *RegisterSecretRequest) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *RegisterSecretRequest) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

type RegisterSecretResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *RegisterSecretResponse) Reset() {
	*x = RegisterSecretResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RegisterSecretResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterSecretResponse) ProtoMessage() {}

func (x *RegisterSecretResponse) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RegisterSecretResponse.ProtoReflect.Descriptor instead.
func (*RegisterSecretResponse) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_core_v1beta1_secret_proto_rawDescGZIP(), []int{1}
}

type UpdateSecretRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName   string `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	SecretName    string `protobuf:"bytes,2,opt,name=secret_name,json=secretName,proto3" json:"secret_name,omitempty"`
	Value         string `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"` // base64 encoded secret value
	NamespaceName string `protobuf:"bytes,4,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
}

func (x *UpdateSecretRequest) Reset() {
	*x = UpdateSecretRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UpdateSecretRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateSecretRequest) ProtoMessage() {}

func (x *UpdateSecretRequest) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateSecretRequest.ProtoReflect.Descriptor instead.
func (*UpdateSecretRequest) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_core_v1beta1_secret_proto_rawDescGZIP(), []int{2}
}

func (x *UpdateSecretRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *UpdateSecretRequest) GetSecretName() string {
	if x != nil {
		return x.SecretName
	}
	return ""
}

func (x *UpdateSecretRequest) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *UpdateSecretRequest) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

type UpdateSecretResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *UpdateSecretResponse) Reset() {
	*x = UpdateSecretResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UpdateSecretResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateSecretResponse) ProtoMessage() {}

func (x *UpdateSecretResponse) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateSecretResponse.ProtoReflect.Descriptor instead.
func (*UpdateSecretResponse) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_core_v1beta1_secret_proto_rawDescGZIP(), []int{3}
}

type ListSecretsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName string `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
}

func (x *ListSecretsRequest) Reset() {
	*x = ListSecretsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListSecretsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListSecretsRequest) ProtoMessage() {}

func (x *ListSecretsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListSecretsRequest.ProtoReflect.Descriptor instead.
func (*ListSecretsRequest) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_core_v1beta1_secret_proto_rawDescGZIP(), []int{4}
}

func (x *ListSecretsRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

type ListSecretsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Secrets []*ListSecretsResponse_Secret `protobuf:"bytes,1,rep,name=secrets,proto3" json:"secrets,omitempty"`
}

func (x *ListSecretsResponse) Reset() {
	*x = ListSecretsResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListSecretsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListSecretsResponse) ProtoMessage() {}

func (x *ListSecretsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListSecretsResponse.ProtoReflect.Descriptor instead.
func (*ListSecretsResponse) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_core_v1beta1_secret_proto_rawDescGZIP(), []int{5}
}

func (x *ListSecretsResponse) GetSecrets() []*ListSecretsResponse_Secret {
	if x != nil {
		return x.Secrets
	}
	return nil
}

type DeleteSecretRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName   string `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	SecretName    string `protobuf:"bytes,2,opt,name=secret_name,json=secretName,proto3" json:"secret_name,omitempty"`
	NamespaceName string `protobuf:"bytes,3,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
}

func (x *DeleteSecretRequest) Reset() {
	*x = DeleteSecretRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteSecretRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteSecretRequest) ProtoMessage() {}

func (x *DeleteSecretRequest) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteSecretRequest.ProtoReflect.Descriptor instead.
func (*DeleteSecretRequest) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_core_v1beta1_secret_proto_rawDescGZIP(), []int{6}
}

func (x *DeleteSecretRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *DeleteSecretRequest) GetSecretName() string {
	if x != nil {
		return x.SecretName
	}
	return ""
}

func (x *DeleteSecretRequest) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

type DeleteSecretResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *DeleteSecretResponse) Reset() {
	*x = DeleteSecretResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteSecretResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteSecretResponse) ProtoMessage() {}

func (x *DeleteSecretResponse) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteSecretResponse.ProtoReflect.Descriptor instead.
func (*DeleteSecretResponse) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_core_v1beta1_secret_proto_rawDescGZIP(), []int{7}
}

type ListSecretsResponse_Secret struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name      string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Digest    string                 `protobuf:"bytes,2,opt,name=digest,proto3" json:"digest,omitempty"`
	Namespace string                 `protobuf:"bytes,3,opt,name=namespace,proto3" json:"namespace,omitempty"`
	UpdatedAt *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=updated_at,json=updatedAt,proto3" json:"updated_at,omitempty"`
}

func (x *ListSecretsResponse_Secret) Reset() {
	*x = ListSecretsResponse_Secret{}
	if protoimpl.UnsafeEnabled {
		mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListSecretsResponse_Secret) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListSecretsResponse_Secret) ProtoMessage() {}

func (x *ListSecretsResponse_Secret) ProtoReflect() protoreflect.Message {
	mi := &file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListSecretsResponse_Secret.ProtoReflect.Descriptor instead.
func (*ListSecretsResponse_Secret) Descriptor() ([]byte, []int) {
	return file_odpf_optimus_core_v1beta1_secret_proto_rawDescGZIP(), []int{5, 0}
}

func (x *ListSecretsResponse_Secret) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *ListSecretsResponse_Secret) GetDigest() string {
	if x != nil {
		return x.Digest
	}
	return ""
}

func (x *ListSecretsResponse_Secret) GetNamespace() string {
	if x != nil {
		return x.Namespace
	}
	return ""
}

func (x *ListSecretsResponse_Secret) GetUpdatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UpdatedAt
	}
	return nil
}

var File_odpf_optimus_core_v1beta1_secret_proto protoreflect.FileDescriptor

var file_odpf_optimus_core_v1beta1_secret_proto_rawDesc = []byte{
	0x0a, 0x26, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2f, 0x63,
	0x6f, 0x72, 0x65, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x73, 0x65, 0x63, 0x72,
	0x65, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x19, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f,
	0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65,
	0x74, 0x61, 0x31, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f,
	0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x2d, 0x67, 0x65, 0x6e, 0x2d, 0x6f,
	0x70, 0x65, 0x6e, 0x61, 0x70, 0x69, 0x76, 0x32, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x2f, 0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x98, 0x01, 0x0a, 0x15, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x53,
	0x65, 0x63, 0x72, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c,
	0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x12,
	0x1f, 0x0a, 0x0b, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x4e, 0x61, 0x6d, 0x65,
	0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70,
	0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d,
	0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x18, 0x0a,
	0x16, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x96, 0x01, 0x0a, 0x13, 0x55, 0x70, 0x64, 0x61,
	0x74, 0x65, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x21, 0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61,
	0x6d, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x5f, 0x6e, 0x61, 0x6d,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x4e,
	0x61, 0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x6e, 0x61, 0x6d,
	0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65,
	0x22, 0x16, 0x0a, 0x14, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x37, 0x0a, 0x12, 0x4c, 0x69, 0x73, 0x74,
	0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21,
	0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d,
	0x65, 0x22, 0xf6, 0x01, 0x0a, 0x13, 0x4c, 0x69, 0x73, 0x74, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74,
	0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x4f, 0x0a, 0x07, 0x73, 0x65, 0x63,
	0x72, 0x65, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x35, 0x2e, 0x6f, 0x64, 0x70,
	0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x53, 0x65, 0x63, 0x72, 0x65,
	0x74, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x53, 0x65, 0x63, 0x72, 0x65,
	0x74, 0x52, 0x07, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x73, 0x1a, 0x8d, 0x01, 0x0a, 0x06, 0x53,
	0x65, 0x63, 0x72, 0x65, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x64, 0x69, 0x67,
	0x65, 0x73, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x64, 0x69, 0x67, 0x65, 0x73,
	0x74, 0x12, 0x1c, 0x0a, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x12,
	0x39, 0x0a, 0x0a, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52,
	0x09, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x41, 0x74, 0x22, 0x80, 0x01, 0x0a, 0x13, 0x44,
	0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61,
	0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63,
	0x74, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x73, 0x65, 0x63, 0x72,
	0x65, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70,
	0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d,
	0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x16, 0x0a,
	0x14, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x32, 0xca, 0x05, 0x0a, 0x0d, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74,
	0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0xb6, 0x01, 0x0a, 0x0e, 0x52, 0x65, 0x67, 0x69,
	0x73, 0x74, 0x65, 0x72, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x12, 0x30, 0x2e, 0x6f, 0x64, 0x70,
	0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x53,
	0x65, 0x63, 0x72, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x31, 0x2e, 0x6f,
	0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65,
	0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65,
	0x72, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
	0x3f, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x39, 0x22, 0x34, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61,
	0x31, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b, 0x70, 0x72, 0x6f, 0x6a, 0x65,
	0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x2f,
	0x7b, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x3a, 0x01, 0x2a,
	0x12, 0xb0, 0x01, 0x0a, 0x0c, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x53, 0x65, 0x63, 0x72, 0x65,
	0x74, 0x12, 0x2e, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73,
	0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x55, 0x70,
	0x64, 0x61, 0x74, 0x65, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x2f, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73,
	0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x55, 0x70,
	0x64, 0x61, 0x74, 0x65, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x22, 0x3f, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x39, 0x1a, 0x34, 0x2f, 0x76, 0x31, 0x62,
	0x65, 0x74, 0x61, 0x31, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b, 0x70, 0x72,
	0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x73, 0x65, 0x63, 0x72,
	0x65, 0x74, 0x2f, 0x7b, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d,
	0x3a, 0x01, 0x2a, 0x12, 0x9c, 0x01, 0x0a, 0x0b, 0x4c, 0x69, 0x73, 0x74, 0x53, 0x65, 0x63, 0x72,
	0x65, 0x74, 0x73, 0x12, 0x2d, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d,
	0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e,
	0x4c, 0x69, 0x73, 0x74, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x2e, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75,
	0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4c,
	0x69, 0x73, 0x74, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x22, 0x2e, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x28, 0x12, 0x26, 0x2f, 0x76, 0x31, 0x62,
	0x65, 0x74, 0x61, 0x31, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b, 0x70, 0x72,
	0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x73, 0x65, 0x63, 0x72,
	0x65, 0x74, 0x12, 0xad, 0x01, 0x0a, 0x0c, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x65, 0x63,
	0x72, 0x65, 0x74, 0x12, 0x2e, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d,
	0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e,
	0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x2f, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d,
	0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e,
	0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0x3c, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x36, 0x2a, 0x34, 0x2f, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b,
	0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x73, 0x65,
	0x63, 0x72, 0x65, 0x74, 0x2f, 0x7b, 0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x5f, 0x6e, 0x61, 0x6d,
	0x65, 0x7d, 0x42, 0x98, 0x01, 0x0a, 0x16, 0x69, 0x6f, 0x2e, 0x6f, 0x64, 0x70, 0x66, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x42, 0x14, 0x53,
	0x65, 0x63, 0x72, 0x65, 0x74, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4d, 0x61, 0x6e, 0x61,
	0x67, 0x65, 0x72, 0x50, 0x01, 0x5a, 0x1e, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x6f, 0x64, 0x70, 0x66, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2f, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x92, 0x41, 0x45, 0x12, 0x05, 0x32, 0x03, 0x30, 0x2e, 0x31, 0x1a,
	0x0e, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x3a, 0x39, 0x31, 0x30, 0x30, 0x22,
	0x04, 0x2f, 0x61, 0x70, 0x69, 0x2a, 0x01, 0x01, 0x72, 0x23, 0x0a, 0x21, 0x4f, 0x70, 0x74, 0x69,
	0x6d, 0x75, 0x73, 0x20, 0x53, 0x65, 0x63, 0x72, 0x65, 0x74, 0x20, 0x4d, 0x61, 0x6e, 0x61, 0x67,
	0x65, 0x6d, 0x65, 0x6e, 0x74, 0x20, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_odpf_optimus_core_v1beta1_secret_proto_rawDescOnce sync.Once
	file_odpf_optimus_core_v1beta1_secret_proto_rawDescData = file_odpf_optimus_core_v1beta1_secret_proto_rawDesc
)

func file_odpf_optimus_core_v1beta1_secret_proto_rawDescGZIP() []byte {
	file_odpf_optimus_core_v1beta1_secret_proto_rawDescOnce.Do(func() {
		file_odpf_optimus_core_v1beta1_secret_proto_rawDescData = protoimpl.X.CompressGZIP(file_odpf_optimus_core_v1beta1_secret_proto_rawDescData)
	})
	return file_odpf_optimus_core_v1beta1_secret_proto_rawDescData
}

var file_odpf_optimus_core_v1beta1_secret_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_odpf_optimus_core_v1beta1_secret_proto_goTypes = []interface{}{
	(*RegisterSecretRequest)(nil),      // 0: odpf.optimus.core.v1beta1.RegisterSecretRequest
	(*RegisterSecretResponse)(nil),     // 1: odpf.optimus.core.v1beta1.RegisterSecretResponse
	(*UpdateSecretRequest)(nil),        // 2: odpf.optimus.core.v1beta1.UpdateSecretRequest
	(*UpdateSecretResponse)(nil),       // 3: odpf.optimus.core.v1beta1.UpdateSecretResponse
	(*ListSecretsRequest)(nil),         // 4: odpf.optimus.core.v1beta1.ListSecretsRequest
	(*ListSecretsResponse)(nil),        // 5: odpf.optimus.core.v1beta1.ListSecretsResponse
	(*DeleteSecretRequest)(nil),        // 6: odpf.optimus.core.v1beta1.DeleteSecretRequest
	(*DeleteSecretResponse)(nil),       // 7: odpf.optimus.core.v1beta1.DeleteSecretResponse
	(*ListSecretsResponse_Secret)(nil), // 8: odpf.optimus.core.v1beta1.ListSecretsResponse.Secret
	(*timestamppb.Timestamp)(nil),      // 9: google.protobuf.Timestamp
}
var file_odpf_optimus_core_v1beta1_secret_proto_depIdxs = []int32{
	8, // 0: odpf.optimus.core.v1beta1.ListSecretsResponse.secrets:type_name -> odpf.optimus.core.v1beta1.ListSecretsResponse.Secret
	9, // 1: odpf.optimus.core.v1beta1.ListSecretsResponse.Secret.updated_at:type_name -> google.protobuf.Timestamp
	0, // 2: odpf.optimus.core.v1beta1.SecretService.RegisterSecret:input_type -> odpf.optimus.core.v1beta1.RegisterSecretRequest
	2, // 3: odpf.optimus.core.v1beta1.SecretService.UpdateSecret:input_type -> odpf.optimus.core.v1beta1.UpdateSecretRequest
	4, // 4: odpf.optimus.core.v1beta1.SecretService.ListSecrets:input_type -> odpf.optimus.core.v1beta1.ListSecretsRequest
	6, // 5: odpf.optimus.core.v1beta1.SecretService.DeleteSecret:input_type -> odpf.optimus.core.v1beta1.DeleteSecretRequest
	1, // 6: odpf.optimus.core.v1beta1.SecretService.RegisterSecret:output_type -> odpf.optimus.core.v1beta1.RegisterSecretResponse
	3, // 7: odpf.optimus.core.v1beta1.SecretService.UpdateSecret:output_type -> odpf.optimus.core.v1beta1.UpdateSecretResponse
	5, // 8: odpf.optimus.core.v1beta1.SecretService.ListSecrets:output_type -> odpf.optimus.core.v1beta1.ListSecretsResponse
	7, // 9: odpf.optimus.core.v1beta1.SecretService.DeleteSecret:output_type -> odpf.optimus.core.v1beta1.DeleteSecretResponse
	6, // [6:10] is the sub-list for method output_type
	2, // [2:6] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_odpf_optimus_core_v1beta1_secret_proto_init() }
func file_odpf_optimus_core_v1beta1_secret_proto_init() {
	if File_odpf_optimus_core_v1beta1_secret_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RegisterSecretRequest); i {
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
		file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RegisterSecretResponse); i {
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
		file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UpdateSecretRequest); i {
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
		file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UpdateSecretResponse); i {
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
		file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListSecretsRequest); i {
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
		file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListSecretsResponse); i {
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
		file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteSecretRequest); i {
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
		file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteSecretResponse); i {
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
		file_odpf_optimus_core_v1beta1_secret_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListSecretsResponse_Secret); i {
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
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_odpf_optimus_core_v1beta1_secret_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   9,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_odpf_optimus_core_v1beta1_secret_proto_goTypes,
		DependencyIndexes: file_odpf_optimus_core_v1beta1_secret_proto_depIdxs,
		MessageInfos:      file_odpf_optimus_core_v1beta1_secret_proto_msgTypes,
	}.Build()
	File_odpf_optimus_core_v1beta1_secret_proto = out.File
	file_odpf_optimus_core_v1beta1_secret_proto_rawDesc = nil
	file_odpf_optimus_core_v1beta1_secret_proto_goTypes = nil
	file_odpf_optimus_core_v1beta1_secret_proto_depIdxs = nil
}
