// Code generated by protoc-gen-go. DO NOT EDIT.
// source: rpc.proto

package rpc

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	empty "github.com/golang/protobuf/ptypes/empty"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type BackingImageSpec struct {
	Name                 string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Url                  string   `protobuf:"bytes,2,opt,name=url,proto3" json:"url,omitempty"`
	Uuid                 string   `protobuf:"bytes,3,opt,name=uuid,proto3" json:"uuid,omitempty"`
	Size                 int64    `protobuf:"varint,4,opt,name=size,proto3" json:"size,omitempty"`
	Directory            string   `protobuf:"bytes,5,opt,name=directory,proto3" json:"directory,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *BackingImageSpec) Reset()         { *m = BackingImageSpec{} }
func (m *BackingImageSpec) String() string { return proto.CompactTextString(m) }
func (*BackingImageSpec) ProtoMessage()    {}
func (*BackingImageSpec) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{0}
}

func (m *BackingImageSpec) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BackingImageSpec.Unmarshal(m, b)
}
func (m *BackingImageSpec) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BackingImageSpec.Marshal(b, m, deterministic)
}
func (m *BackingImageSpec) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BackingImageSpec.Merge(m, src)
}
func (m *BackingImageSpec) XXX_Size() int {
	return xxx_messageInfo_BackingImageSpec.Size(m)
}
func (m *BackingImageSpec) XXX_DiscardUnknown() {
	xxx_messageInfo_BackingImageSpec.DiscardUnknown(m)
}

var xxx_messageInfo_BackingImageSpec proto.InternalMessageInfo

func (m *BackingImageSpec) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *BackingImageSpec) GetUrl() string {
	if m != nil {
		return m.Url
	}
	return ""
}

func (m *BackingImageSpec) GetUuid() string {
	if m != nil {
		return m.Uuid
	}
	return ""
}

func (m *BackingImageSpec) GetSize() int64 {
	if m != nil {
		return m.Size
	}
	return 0
}

func (m *BackingImageSpec) GetDirectory() string {
	if m != nil {
		return m.Directory
	}
	return ""
}

type BackingImageStatus struct {
	State                string   `protobuf:"bytes,1,opt,name=state,proto3" json:"state,omitempty"`
	ErrorMsg             string   `protobuf:"bytes,2,opt,name=error_msg,json=errorMsg,proto3" json:"error_msg,omitempty"`
	SendingReference     int32    `protobuf:"varint,3,opt,name=sending_reference,json=sendingReference,proto3" json:"sending_reference,omitempty"`
	SenderManagerAddress string   `protobuf:"bytes,4,opt,name=sender_manager_address,json=senderManagerAddress,proto3" json:"sender_manager_address,omitempty"`
	DownloadProgress     int32    `protobuf:"varint,5,opt,name=download_progress,json=downloadProgress,proto3" json:"download_progress,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *BackingImageStatus) Reset()         { *m = BackingImageStatus{} }
func (m *BackingImageStatus) String() string { return proto.CompactTextString(m) }
func (*BackingImageStatus) ProtoMessage()    {}
func (*BackingImageStatus) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{1}
}

func (m *BackingImageStatus) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BackingImageStatus.Unmarshal(m, b)
}
func (m *BackingImageStatus) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BackingImageStatus.Marshal(b, m, deterministic)
}
func (m *BackingImageStatus) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BackingImageStatus.Merge(m, src)
}
func (m *BackingImageStatus) XXX_Size() int {
	return xxx_messageInfo_BackingImageStatus.Size(m)
}
func (m *BackingImageStatus) XXX_DiscardUnknown() {
	xxx_messageInfo_BackingImageStatus.DiscardUnknown(m)
}

var xxx_messageInfo_BackingImageStatus proto.InternalMessageInfo

func (m *BackingImageStatus) GetState() string {
	if m != nil {
		return m.State
	}
	return ""
}

func (m *BackingImageStatus) GetErrorMsg() string {
	if m != nil {
		return m.ErrorMsg
	}
	return ""
}

func (m *BackingImageStatus) GetSendingReference() int32 {
	if m != nil {
		return m.SendingReference
	}
	return 0
}

func (m *BackingImageStatus) GetSenderManagerAddress() string {
	if m != nil {
		return m.SenderManagerAddress
	}
	return ""
}

func (m *BackingImageStatus) GetDownloadProgress() int32 {
	if m != nil {
		return m.DownloadProgress
	}
	return 0
}

type BackingImageResponse struct {
	Spec                 *BackingImageSpec   `protobuf:"bytes,1,opt,name=spec,proto3" json:"spec,omitempty"`
	Status               *BackingImageStatus `protobuf:"bytes,2,opt,name=status,proto3" json:"status,omitempty"`
	XXX_NoUnkeyedLiteral struct{}            `json:"-"`
	XXX_unrecognized     []byte              `json:"-"`
	XXX_sizecache        int32               `json:"-"`
}

func (m *BackingImageResponse) Reset()         { *m = BackingImageResponse{} }
func (m *BackingImageResponse) String() string { return proto.CompactTextString(m) }
func (*BackingImageResponse) ProtoMessage()    {}
func (*BackingImageResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{2}
}

func (m *BackingImageResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_BackingImageResponse.Unmarshal(m, b)
}
func (m *BackingImageResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_BackingImageResponse.Marshal(b, m, deterministic)
}
func (m *BackingImageResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_BackingImageResponse.Merge(m, src)
}
func (m *BackingImageResponse) XXX_Size() int {
	return xxx_messageInfo_BackingImageResponse.Size(m)
}
func (m *BackingImageResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_BackingImageResponse.DiscardUnknown(m)
}

var xxx_messageInfo_BackingImageResponse proto.InternalMessageInfo

func (m *BackingImageResponse) GetSpec() *BackingImageSpec {
	if m != nil {
		return m.Spec
	}
	return nil
}

func (m *BackingImageResponse) GetStatus() *BackingImageStatus {
	if m != nil {
		return m.Status
	}
	return nil
}

type PullRequest struct {
	Spec                 *BackingImageSpec `protobuf:"bytes,1,opt,name=spec,proto3" json:"spec,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *PullRequest) Reset()         { *m = PullRequest{} }
func (m *PullRequest) String() string { return proto.CompactTextString(m) }
func (*PullRequest) ProtoMessage()    {}
func (*PullRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{3}
}

func (m *PullRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PullRequest.Unmarshal(m, b)
}
func (m *PullRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PullRequest.Marshal(b, m, deterministic)
}
func (m *PullRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PullRequest.Merge(m, src)
}
func (m *PullRequest) XXX_Size() int {
	return xxx_messageInfo_PullRequest.Size(m)
}
func (m *PullRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PullRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PullRequest proto.InternalMessageInfo

func (m *PullRequest) GetSpec() *BackingImageSpec {
	if m != nil {
		return m.Spec
	}
	return nil
}

type DeleteRequest struct {
	Name                 string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *DeleteRequest) Reset()         { *m = DeleteRequest{} }
func (m *DeleteRequest) String() string { return proto.CompactTextString(m) }
func (*DeleteRequest) ProtoMessage()    {}
func (*DeleteRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{4}
}

func (m *DeleteRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_DeleteRequest.Unmarshal(m, b)
}
func (m *DeleteRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_DeleteRequest.Marshal(b, m, deterministic)
}
func (m *DeleteRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DeleteRequest.Merge(m, src)
}
func (m *DeleteRequest) XXX_Size() int {
	return xxx_messageInfo_DeleteRequest.Size(m)
}
func (m *DeleteRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_DeleteRequest.DiscardUnknown(m)
}

var xxx_messageInfo_DeleteRequest proto.InternalMessageInfo

func (m *DeleteRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type GetRequest struct {
	Name                 string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetRequest) Reset()         { *m = GetRequest{} }
func (m *GetRequest) String() string { return proto.CompactTextString(m) }
func (*GetRequest) ProtoMessage()    {}
func (*GetRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{5}
}

func (m *GetRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetRequest.Unmarshal(m, b)
}
func (m *GetRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetRequest.Marshal(b, m, deterministic)
}
func (m *GetRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetRequest.Merge(m, src)
}
func (m *GetRequest) XXX_Size() int {
	return xxx_messageInfo_GetRequest.Size(m)
}
func (m *GetRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetRequest proto.InternalMessageInfo

func (m *GetRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type ListResponse struct {
	BackingImages        map[string]*BackingImageResponse `protobuf:"bytes,1,rep,name=backing_images,json=backingImages,proto3" json:"backing_images,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}                         `json:"-"`
	XXX_unrecognized     []byte                           `json:"-"`
	XXX_sizecache        int32                            `json:"-"`
}

func (m *ListResponse) Reset()         { *m = ListResponse{} }
func (m *ListResponse) String() string { return proto.CompactTextString(m) }
func (*ListResponse) ProtoMessage()    {}
func (*ListResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{6}
}

func (m *ListResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ListResponse.Unmarshal(m, b)
}
func (m *ListResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ListResponse.Marshal(b, m, deterministic)
}
func (m *ListResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ListResponse.Merge(m, src)
}
func (m *ListResponse) XXX_Size() int {
	return xxx_messageInfo_ListResponse.Size(m)
}
func (m *ListResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_ListResponse.DiscardUnknown(m)
}

var xxx_messageInfo_ListResponse proto.InternalMessageInfo

func (m *ListResponse) GetBackingImages() map[string]*BackingImageResponse {
	if m != nil {
		return m.BackingImages
	}
	return nil
}

type SendRequest struct {
	Name                 string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	ToAddress            string   `protobuf:"bytes,2,opt,name=to_address,json=toAddress,proto3" json:"to_address,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SendRequest) Reset()         { *m = SendRequest{} }
func (m *SendRequest) String() string { return proto.CompactTextString(m) }
func (*SendRequest) ProtoMessage()    {}
func (*SendRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{7}
}

func (m *SendRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SendRequest.Unmarshal(m, b)
}
func (m *SendRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SendRequest.Marshal(b, m, deterministic)
}
func (m *SendRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SendRequest.Merge(m, src)
}
func (m *SendRequest) XXX_Size() int {
	return xxx_messageInfo_SendRequest.Size(m)
}
func (m *SendRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_SendRequest.DiscardUnknown(m)
}

var xxx_messageInfo_SendRequest proto.InternalMessageInfo

func (m *SendRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *SendRequest) GetToAddress() string {
	if m != nil {
		return m.ToAddress
	}
	return ""
}

type SyncRequest struct {
	BackingImageSpec     *BackingImageSpec `protobuf:"bytes,1,opt,name=backing_image_spec,json=backingImageSpec,proto3" json:"backing_image_spec,omitempty"`
	FromHost             string            `protobuf:"bytes,2,opt,name=from_host,json=fromHost,proto3" json:"from_host,omitempty"`
	ToHost               string            `protobuf:"bytes,3,opt,name=to_host,json=toHost,proto3" json:"to_host,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *SyncRequest) Reset()         { *m = SyncRequest{} }
func (m *SyncRequest) String() string { return proto.CompactTextString(m) }
func (*SyncRequest) ProtoMessage()    {}
func (*SyncRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{8}
}

func (m *SyncRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SyncRequest.Unmarshal(m, b)
}
func (m *SyncRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SyncRequest.Marshal(b, m, deterministic)
}
func (m *SyncRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SyncRequest.Merge(m, src)
}
func (m *SyncRequest) XXX_Size() int {
	return xxx_messageInfo_SyncRequest.Size(m)
}
func (m *SyncRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_SyncRequest.DiscardUnknown(m)
}

var xxx_messageInfo_SyncRequest proto.InternalMessageInfo

func (m *SyncRequest) GetBackingImageSpec() *BackingImageSpec {
	if m != nil {
		return m.BackingImageSpec
	}
	return nil
}

func (m *SyncRequest) GetFromHost() string {
	if m != nil {
		return m.FromHost
	}
	return ""
}

func (m *SyncRequest) GetToHost() string {
	if m != nil {
		return m.ToHost
	}
	return ""
}

type VersionResponse struct {
	Version                          string   `protobuf:"bytes,1,opt,name=version,proto3" json:"version,omitempty"`
	GitCommit                        string   `protobuf:"bytes,2,opt,name=gitCommit,proto3" json:"gitCommit,omitempty"`
	BuildDate                        string   `protobuf:"bytes,3,opt,name=buildDate,proto3" json:"buildDate,omitempty"`
	BackingImageManagerApiVersion    int64    `protobuf:"varint,4,opt,name=backing_image_manager_api_version,json=backingImageManagerApiVersion,proto3" json:"backing_image_manager_api_version,omitempty"`
	BackingImageManagerApiMinVersion int64    `protobuf:"varint,5,opt,name=backing_image_manager_api_min_version,json=backingImageManagerApiMinVersion,proto3" json:"backing_image_manager_api_min_version,omitempty"`
	XXX_NoUnkeyedLiteral             struct{} `json:"-"`
	XXX_unrecognized                 []byte   `json:"-"`
	XXX_sizecache                    int32    `json:"-"`
}

func (m *VersionResponse) Reset()         { *m = VersionResponse{} }
func (m *VersionResponse) String() string { return proto.CompactTextString(m) }
func (*VersionResponse) ProtoMessage()    {}
func (*VersionResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_77a6da22d6a3feb1, []int{9}
}

func (m *VersionResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_VersionResponse.Unmarshal(m, b)
}
func (m *VersionResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_VersionResponse.Marshal(b, m, deterministic)
}
func (m *VersionResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_VersionResponse.Merge(m, src)
}
func (m *VersionResponse) XXX_Size() int {
	return xxx_messageInfo_VersionResponse.Size(m)
}
func (m *VersionResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_VersionResponse.DiscardUnknown(m)
}

var xxx_messageInfo_VersionResponse proto.InternalMessageInfo

func (m *VersionResponse) GetVersion() string {
	if m != nil {
		return m.Version
	}
	return ""
}

func (m *VersionResponse) GetGitCommit() string {
	if m != nil {
		return m.GitCommit
	}
	return ""
}

func (m *VersionResponse) GetBuildDate() string {
	if m != nil {
		return m.BuildDate
	}
	return ""
}

func (m *VersionResponse) GetBackingImageManagerApiVersion() int64 {
	if m != nil {
		return m.BackingImageManagerApiVersion
	}
	return 0
}

func (m *VersionResponse) GetBackingImageManagerApiMinVersion() int64 {
	if m != nil {
		return m.BackingImageManagerApiMinVersion
	}
	return 0
}

func init() {
	proto.RegisterType((*BackingImageSpec)(nil), "BackingImageSpec")
	proto.RegisterType((*BackingImageStatus)(nil), "BackingImageStatus")
	proto.RegisterType((*BackingImageResponse)(nil), "BackingImageResponse")
	proto.RegisterType((*PullRequest)(nil), "PullRequest")
	proto.RegisterType((*DeleteRequest)(nil), "DeleteRequest")
	proto.RegisterType((*GetRequest)(nil), "GetRequest")
	proto.RegisterType((*ListResponse)(nil), "ListResponse")
	proto.RegisterMapType((map[string]*BackingImageResponse)(nil), "ListResponse.BackingImagesEntry")
	proto.RegisterType((*SendRequest)(nil), "SendRequest")
	proto.RegisterType((*SyncRequest)(nil), "SyncRequest")
	proto.RegisterType((*VersionResponse)(nil), "VersionResponse")
}

func init() { proto.RegisterFile("rpc.proto", fileDescriptor_77a6da22d6a3feb1) }

var fileDescriptor_77a6da22d6a3feb1 = []byte{
	// 729 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x54, 0xdd, 0x4e, 0xdb, 0x48,
	0x14, 0xc6, 0xe4, 0x87, 0xcd, 0x09, 0xb0, 0x61, 0x96, 0x65, 0xa3, 0xb0, 0x48, 0x59, 0xaf, 0x90,
	0x90, 0xa2, 0x35, 0xab, 0x14, 0x55, 0x55, 0x7b, 0xd1, 0x96, 0x82, 0xa0, 0x52, 0x51, 0x91, 0x23,
	0x95, 0x4b, 0xcb, 0xb1, 0x0f, 0x66, 0x8a, 0xed, 0x71, 0x67, 0xc6, 0x54, 0xe9, 0x45, 0x2f, 0xaa,
	0x3e, 0x50, 0xfb, 0x34, 0x7d, 0x9d, 0x6a, 0x66, 0xec, 0xfc, 0x40, 0x12, 0xf5, 0x6e, 0xe6, 0x3b,
	0xdf, 0xf9, 0x9d, 0xf3, 0x0d, 0x34, 0x78, 0x16, 0x38, 0x19, 0x67, 0x92, 0x75, 0x76, 0x23, 0xc6,
	0xa2, 0x18, 0x0f, 0xf5, 0x6d, 0x98, 0x5f, 0x1f, 0x62, 0x92, 0xc9, 0x91, 0x31, 0xda, 0x9f, 0xa1,
	0x75, 0xec, 0x07, 0xb7, 0x34, 0x8d, 0x5e, 0x27, 0x7e, 0x84, 0x83, 0x0c, 0x03, 0x42, 0xa0, 0x9a,
	0xfa, 0x09, 0xb6, 0xad, 0xae, 0x75, 0xd0, 0x70, 0xf5, 0x99, 0xb4, 0xa0, 0x92, 0xf3, 0xb8, 0xbd,
	0xaa, 0x21, 0x75, 0x54, 0xac, 0x3c, 0xa7, 0x61, 0xbb, 0x62, 0x58, 0xea, 0xac, 0x30, 0x41, 0x3f,
	0x61, 0xbb, 0xda, 0xb5, 0x0e, 0x2a, 0xae, 0x3e, 0x93, 0xbf, 0xa1, 0x11, 0x52, 0x8e, 0x81, 0x64,
	0x7c, 0xd4, 0xae, 0x69, 0xf2, 0x04, 0xb0, 0x7f, 0x58, 0x40, 0x66, 0x0a, 0x90, 0xbe, 0xcc, 0x05,
	0xd9, 0x86, 0x9a, 0x90, 0xbe, 0x2c, 0x6b, 0x30, 0x17, 0xb2, 0x0b, 0x0d, 0xe4, 0x9c, 0x71, 0x2f,
	0x11, 0x51, 0x51, 0xca, 0x6f, 0x1a, 0xb8, 0x10, 0x11, 0xe9, 0xc1, 0x96, 0xc0, 0x34, 0xa4, 0x69,
	0xe4, 0x71, 0xbc, 0x46, 0x8e, 0x69, 0x80, 0xba, 0xb8, 0x9a, 0xdb, 0x2a, 0x0c, 0x6e, 0x89, 0x93,
	0x23, 0xd8, 0x51, 0x18, 0x72, 0x2f, 0xf1, 0x53, 0x3f, 0x42, 0xee, 0xf9, 0x61, 0xc8, 0x51, 0x08,
	0x5d, 0x7a, 0xc3, 0xdd, 0x36, 0xd6, 0x0b, 0x63, 0x7c, 0x69, 0x6c, 0x2a, 0x45, 0xc8, 0x3e, 0xa6,
	0x31, 0xf3, 0x43, 0x2f, 0xe3, 0x2c, 0xd2, 0x0e, 0x35, 0x93, 0xa2, 0x34, 0x5c, 0x16, 0xb8, 0xfd,
	0x1e, 0xb6, 0xa7, 0x1b, 0x73, 0x51, 0x64, 0x2c, 0x15, 0x48, 0xf6, 0xa1, 0x2a, 0x32, 0x0c, 0x74,
	0x67, 0xcd, 0xfe, 0x96, 0x73, 0x7f, 0xfc, 0xae, 0x36, 0x93, 0x1e, 0xd4, 0x85, 0x9e, 0x85, 0x6e,
	0xb4, 0xd9, 0xff, 0xc3, 0x79, 0x38, 0x26, 0xb7, 0xa0, 0xd8, 0x47, 0xd0, 0xbc, 0xcc, 0xe3, 0xd8,
	0xc5, 0x0f, 0x39, 0x0a, 0xf9, 0x8b, 0x29, 0xec, 0x7f, 0x61, 0xe3, 0x04, 0x63, 0x94, 0x58, 0xfa,
	0xcd, 0x79, 0x78, 0xbb, 0x0b, 0x70, 0x86, 0x72, 0x19, 0xe3, 0x9b, 0x05, 0xeb, 0x6f, 0xa8, 0x90,
	0xe3, 0x0e, 0xcf, 0x60, 0x73, 0x68, 0x32, 0x7a, 0x54, 0xa5, 0x14, 0x6d, 0xab, 0x5b, 0x39, 0x68,
	0xf6, 0xbb, 0xce, 0x34, 0x6d, 0xa6, 0x2a, 0x71, 0x9a, 0x4a, 0x3e, 0x72, 0x37, 0x86, 0xd3, 0x58,
	0xe7, 0x6a, 0x76, 0x37, 0x0c, 0x49, 0xad, 0xe2, 0x2d, 0x8e, 0x8a, 0x12, 0xd4, 0x91, 0xf4, 0xa0,
	0x76, 0xe7, 0xc7, 0x39, 0x16, 0xa3, 0xfa, 0xd3, 0x99, 0x37, 0x78, 0xd7, 0x70, 0x9e, 0xae, 0x3e,
	0xb1, 0xec, 0x17, 0xd0, 0x1c, 0x60, 0x1a, 0x2e, 0xe9, 0x8a, 0xec, 0x01, 0x48, 0x36, 0xde, 0x0a,
	0xb3, 0x6c, 0x0d, 0xc9, 0x8a, 0x55, 0xb0, 0xbf, 0x5a, 0xd0, 0x1c, 0x8c, 0xd2, 0xa0, 0x0c, 0xf1,
	0x1c, 0xc8, 0x4c, 0xcf, 0xde, 0xf2, 0x07, 0x68, 0x0d, 0xef, 0x8b, 0x6e, 0x17, 0x1a, 0xd7, 0x9c,
	0x25, 0xde, 0x0d, 0x13, 0xb2, 0xdc, 0x6d, 0x05, 0x9c, 0x33, 0x21, 0xc9, 0x5f, 0xb0, 0x26, 0x99,
	0x31, 0x19, 0xb9, 0xd5, 0x25, 0x53, 0x06, 0xfb, 0xcb, 0x2a, 0xfc, 0xfe, 0x0e, 0xb9, 0xa0, 0x2c,
	0x1d, 0x8f, 0xbf, 0x0d, 0x6b, 0x77, 0x06, 0x2a, 0x1a, 0x2a, 0xaf, 0x4a, 0x8a, 0x11, 0x95, 0xaf,
	0x58, 0x92, 0xd0, 0x32, 0xc7, 0x04, 0x50, 0xd6, 0x61, 0x4e, 0xe3, 0xf0, 0x44, 0xe9, 0xce, 0xa4,
	0x99, 0x00, 0xe4, 0x1c, 0xfe, 0x99, 0x6d, 0x70, 0x2c, 0x9c, 0x8c, 0x7a, 0x65, 0x3e, 0xa3, 0xfb,
	0xbd, 0xe9, 0xe6, 0x4a, 0x09, 0x65, 0xb4, 0xa8, 0x93, 0xbc, 0x85, 0xfd, 0xc5, 0x91, 0x12, 0x9a,
	0x8e, 0xa3, 0xd5, 0x74, 0xb4, 0xee, 0xfc, 0x68, 0x17, 0x34, 0x2d, 0x02, 0xf6, 0xbf, 0x57, 0xa0,
	0x73, 0xfc, 0x90, 0x34, 0x40, 0x7e, 0x47, 0x03, 0x24, 0xff, 0x41, 0x55, 0x89, 0x83, 0xac, 0x3b,
	0x53, 0x1a, 0xe9, 0xcc, 0x5f, 0x12, 0x7b, 0x45, 0xd1, 0xd5, 0xc3, 0x92, 0x75, 0x67, 0xea, 0x7d,
	0x17, 0xd3, 0x1d, 0xa8, 0xaa, 0x55, 0x52, 0xf4, 0xc9, 0x46, 0x75, 0x76, 0x1c, 0xf3, 0xe9, 0x3a,
	0xe5, 0xa7, 0xeb, 0x9c, 0xaa, 0x4f, 0xd7, 0x5e, 0x21, 0x7d, 0xa8, 0x1b, 0xd1, 0x91, 0x4d, 0x67,
	0x46, 0x7d, 0x4b, 0x7c, 0x7a, 0x50, 0x39, 0x43, 0x49, 0x9a, 0xce, 0x44, 0x89, 0x8b, 0x0b, 0x3a,
	0x84, 0xaa, 0x92, 0x19, 0x59, 0x10, 0xae, 0xb3, 0x31, 0xa3, 0x42, 0x7b, 0x85, 0x3c, 0x06, 0x28,
	0x26, 0xa9, 0x92, 0x2c, 0x72, 0x6b, 0x39, 0xf7, 0xf7, 0xec, 0x19, 0xd4, 0xae, 0x7c, 0x19, 0xdc,
	0x2c, 0x74, 0x59, 0xd8, 0xd0, 0xff, 0xd6, 0xb0, 0xae, 0xb1, 0x47, 0x3f, 0x03, 0x00, 0x00, 0xff,
	0xff, 0x47, 0x95, 0x47, 0xb3, 0xa8, 0x06, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// BackingImageManagerServiceClient is the client API for BackingImageManagerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type BackingImageManagerServiceClient interface {
	Pull(ctx context.Context, in *PullRequest, opts ...grpc.CallOption) (*BackingImageResponse, error)
	Sync(ctx context.Context, in *SyncRequest, opts ...grpc.CallOption) (*BackingImageResponse, error)
	Send(ctx context.Context, in *SendRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*empty.Empty, error)
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*BackingImageResponse, error)
	List(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*ListResponse, error)
	VersionGet(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*VersionResponse, error)
	Watch(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (BackingImageManagerService_WatchClient, error)
}

type backingImageManagerServiceClient struct {
	cc *grpc.ClientConn
}

func NewBackingImageManagerServiceClient(cc *grpc.ClientConn) BackingImageManagerServiceClient {
	return &backingImageManagerServiceClient{cc}
}

func (c *backingImageManagerServiceClient) Pull(ctx context.Context, in *PullRequest, opts ...grpc.CallOption) (*BackingImageResponse, error) {
	out := new(BackingImageResponse)
	err := c.cc.Invoke(ctx, "/BackingImageManagerService/Pull", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backingImageManagerServiceClient) Sync(ctx context.Context, in *SyncRequest, opts ...grpc.CallOption) (*BackingImageResponse, error) {
	out := new(BackingImageResponse)
	err := c.cc.Invoke(ctx, "/BackingImageManagerService/Sync", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backingImageManagerServiceClient) Send(ctx context.Context, in *SendRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/BackingImageManagerService/Send", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backingImageManagerServiceClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
	out := new(empty.Empty)
	err := c.cc.Invoke(ctx, "/BackingImageManagerService/Delete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backingImageManagerServiceClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*BackingImageResponse, error) {
	out := new(BackingImageResponse)
	err := c.cc.Invoke(ctx, "/BackingImageManagerService/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backingImageManagerServiceClient) List(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*ListResponse, error) {
	out := new(ListResponse)
	err := c.cc.Invoke(ctx, "/BackingImageManagerService/List", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backingImageManagerServiceClient) VersionGet(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*VersionResponse, error) {
	out := new(VersionResponse)
	err := c.cc.Invoke(ctx, "/BackingImageManagerService/VersionGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *backingImageManagerServiceClient) Watch(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (BackingImageManagerService_WatchClient, error) {
	stream, err := c.cc.NewStream(ctx, &_BackingImageManagerService_serviceDesc.Streams[0], "/BackingImageManagerService/Watch", opts...)
	if err != nil {
		return nil, err
	}
	x := &backingImageManagerServiceWatchClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type BackingImageManagerService_WatchClient interface {
	Recv() (*empty.Empty, error)
	grpc.ClientStream
}

type backingImageManagerServiceWatchClient struct {
	grpc.ClientStream
}

func (x *backingImageManagerServiceWatchClient) Recv() (*empty.Empty, error) {
	m := new(empty.Empty)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// BackingImageManagerServiceServer is the server API for BackingImageManagerService service.
type BackingImageManagerServiceServer interface {
	Pull(context.Context, *PullRequest) (*BackingImageResponse, error)
	Sync(context.Context, *SyncRequest) (*BackingImageResponse, error)
	Send(context.Context, *SendRequest) (*empty.Empty, error)
	Delete(context.Context, *DeleteRequest) (*empty.Empty, error)
	Get(context.Context, *GetRequest) (*BackingImageResponse, error)
	List(context.Context, *empty.Empty) (*ListResponse, error)
	VersionGet(context.Context, *empty.Empty) (*VersionResponse, error)
	Watch(*empty.Empty, BackingImageManagerService_WatchServer) error
}

// UnimplementedBackingImageManagerServiceServer can be embedded to have forward compatible implementations.
type UnimplementedBackingImageManagerServiceServer struct {
}

func (*UnimplementedBackingImageManagerServiceServer) Pull(ctx context.Context, req *PullRequest) (*BackingImageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Pull not implemented")
}
func (*UnimplementedBackingImageManagerServiceServer) Sync(ctx context.Context, req *SyncRequest) (*BackingImageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Sync not implemented")
}
func (*UnimplementedBackingImageManagerServiceServer) Send(ctx context.Context, req *SendRequest) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Send not implemented")
}
func (*UnimplementedBackingImageManagerServiceServer) Delete(ctx context.Context, req *DeleteRequest) (*empty.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Delete not implemented")
}
func (*UnimplementedBackingImageManagerServiceServer) Get(ctx context.Context, req *GetRequest) (*BackingImageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (*UnimplementedBackingImageManagerServiceServer) List(ctx context.Context, req *empty.Empty) (*ListResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method List not implemented")
}
func (*UnimplementedBackingImageManagerServiceServer) VersionGet(ctx context.Context, req *empty.Empty) (*VersionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method VersionGet not implemented")
}
func (*UnimplementedBackingImageManagerServiceServer) Watch(req *empty.Empty, srv BackingImageManagerService_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "method Watch not implemented")
}

func RegisterBackingImageManagerServiceServer(s *grpc.Server, srv BackingImageManagerServiceServer) {
	s.RegisterService(&_BackingImageManagerService_serviceDesc, srv)
}

func _BackingImageManagerService_Pull_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PullRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackingImageManagerServiceServer).Pull(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/BackingImageManagerService/Pull",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackingImageManagerServiceServer).Pull(ctx, req.(*PullRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackingImageManagerService_Sync_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SyncRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackingImageManagerServiceServer).Sync(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/BackingImageManagerService/Sync",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackingImageManagerServiceServer).Sync(ctx, req.(*SyncRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackingImageManagerService_Send_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SendRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackingImageManagerServiceServer).Send(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/BackingImageManagerService/Send",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackingImageManagerServiceServer).Send(ctx, req.(*SendRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackingImageManagerService_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackingImageManagerServiceServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/BackingImageManagerService/Delete",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackingImageManagerServiceServer).Delete(ctx, req.(*DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackingImageManagerService_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackingImageManagerServiceServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/BackingImageManagerService/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackingImageManagerServiceServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackingImageManagerService_List_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackingImageManagerServiceServer).List(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/BackingImageManagerService/List",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackingImageManagerServiceServer).List(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackingImageManagerService_VersionGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BackingImageManagerServiceServer).VersionGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/BackingImageManagerService/VersionGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BackingImageManagerServiceServer).VersionGet(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _BackingImageManagerService_Watch_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(empty.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(BackingImageManagerServiceServer).Watch(m, &backingImageManagerServiceWatchServer{stream})
}

type BackingImageManagerService_WatchServer interface {
	Send(*empty.Empty) error
	grpc.ServerStream
}

type backingImageManagerServiceWatchServer struct {
	grpc.ServerStream
}

func (x *backingImageManagerServiceWatchServer) Send(m *empty.Empty) error {
	return x.ServerStream.SendMsg(m)
}

var _BackingImageManagerService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "BackingImageManagerService",
	HandlerType: (*BackingImageManagerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Pull",
			Handler:    _BackingImageManagerService_Pull_Handler,
		},
		{
			MethodName: "Sync",
			Handler:    _BackingImageManagerService_Sync_Handler,
		},
		{
			MethodName: "Send",
			Handler:    _BackingImageManagerService_Send_Handler,
		},
		{
			MethodName: "Delete",
			Handler:    _BackingImageManagerService_Delete_Handler,
		},
		{
			MethodName: "Get",
			Handler:    _BackingImageManagerService_Get_Handler,
		},
		{
			MethodName: "List",
			Handler:    _BackingImageManagerService_List_Handler,
		},
		{
			MethodName: "VersionGet",
			Handler:    _BackingImageManagerService_VersionGet_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Watch",
			Handler:       _BackingImageManagerService_Watch_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "rpc.proto",
}
