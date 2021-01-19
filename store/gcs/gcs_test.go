package gcs_test

import (
	"context"
	"io"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/iterator"
)

type badReader int

func (r badReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("bad reader")
}

type storageClientMock struct {
	stiface.Client
	mock.Mock
}

func (s *storageClientMock) Bucket(name string) stiface.BucketHandle {
	args := s.Called(name)
	return args.Get(0).(stiface.BucketHandle)
}

func (s *storageClientMock) Buckets(ctx context.Context, projectID string) stiface.BucketIterator {
	panic("not implemented")
}

func (s *storageClientMock) Close() error {
	panic("not implemented")
}

func (s *storageClientMock) embedToIncludeNewMethods() {
	panic("not implemented")
}

type storageBucketMock struct {
	stiface.BucketHandle
	mock.Mock
}

func (b *storageBucketMock) Create(context.Context, string, *storage.BucketAttrs) error {
	panic("not implemented")
}
func (b *storageBucketMock) Delete(context.Context) error {
	panic("not implemented")
}
func (b *storageBucketMock) DefaultObjectACL() stiface.ACLHandle {
	panic("not implemented")
}
func (b *storageBucketMock) Object(name string) stiface.ObjectHandle {
	args := b.Called(name)
	return args.Get(0).(stiface.ObjectHandle)
}
func (b *storageBucketMock) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	args := b.Called(ctx)
	return args.Get(0).(*storage.BucketAttrs), args.Error(1)
}
func (b *storageBucketMock) Update(context.Context, storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error) {
	panic("not implemented")
}
func (b *storageBucketMock) If(storage.BucketConditions) stiface.BucketHandle {
	panic("not implemented")
}

func (b *storageBucketMock) Objects(ctx context.Context, query *storage.Query) stiface.ObjectIterator {
	args := b.Called(ctx, query)
	return args.Get(0).(stiface.ObjectIterator)
}

func (b *storageBucketMock) ACL() stiface.ACLHandle {
	panic("not implemented")
}
func (b *storageBucketMock) IAM() *iam.Handle {
	panic("not implemented")
}
func (b *storageBucketMock) UserProject(projectID string) stiface.BucketHandle {
	panic("not implemented")
}
func (b *storageBucketMock) Notifications(context.Context) (map[string]*storage.Notification, error) {
	panic("not implemented")
}
func (b *storageBucketMock) AddNotification(context.Context, *storage.Notification) (*storage.Notification, error) {
	panic("not implemented")
}
func (b *storageBucketMock) DeleteNotification(context.Context, string) error {
	panic("not implemented")
}
func (b *storageBucketMock) LockRetentionPolicy(context.Context) error {
	panic("not implemented")
}

func (b *storageBucketMock) embedToIncludeNewMethods() {
	panic("not implemented")
}

type objectHandleMock struct {
	stiface.ObjectHandle
	mock.Mock
}

func (objHandle *objectHandleMock) ACL() stiface.ACLHandle {
	panic("not implemented")
}
func (objHandle *objectHandleMock) Generation(int64) stiface.ObjectHandle {
	panic("not implemented")
}
func (objHandle *objectHandleMock) If(storage.Conditions) stiface.ObjectHandle {
	panic("not implemented")
}
func (objHandle *objectHandleMock) Key([]byte) stiface.ObjectHandle {
	panic("not implemented")
}
func (objHandle *objectHandleMock) ReadCompressed(bool) stiface.ObjectHandle {
	panic("not implemented")
}
func (objHandle *objectHandleMock) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	args := objHandle.Called(ctx)
	return args.Get(0).(*storage.ObjectAttrs), args.Error(1)
}
func (objHandle *objectHandleMock) Update(context.Context, storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error) {
	panic("not implemented")
}
func (objHandle *objectHandleMock) NewReader(context.Context) (stiface.Reader, error) {
	panic("not implemented")
}
func (objHandle *objectHandleMock) NewRangeReader(context.Context, int64, int64) (stiface.Reader, error) {
	panic("not implemented")
}
func (objHandle *objectHandleMock) NewWriter(context.Context) stiface.Writer {
	panic("not implemented")
}
func (objHandle *objectHandleMock) Delete(ctx context.Context) error {
	return objHandle.Called(ctx).Error(0)
}
func (objHandle *objectHandleMock) CopierFrom(stiface.ObjectHandle) stiface.Copier {
	panic("not implemented")
}
func (objHandle *objectHandleMock) ComposerFrom(...stiface.ObjectHandle) stiface.Composer {
	panic("not implemented")
}

func (objHandle *objectHandleMock) embedToIncludeNewMethods() {
	panic("not implemented")
}

type objectIteratorMock struct {
	stiface.ObjectIterator
	mock.Mock
	current     int
	objAttrList []*storage.ObjectAttrs
	err         error
}

func (objIt *objectIteratorMock) Next() (*storage.ObjectAttrs, error) {
	if objIt.err != nil {
		return nil, objIt.err
	}

	if objIt.current < len(objIt.objAttrList) {
		currentObjAttrs := objIt.objAttrList[objIt.current]
		objIt.current = objIt.current + 1
		return currentObjAttrs, nil
	}
	return nil, iterator.Done
}

func (objIt *objectIteratorMock) PageInfo() *iterator.PageInfo {
	panic("not implemented")
}

func (objIt *objectIteratorMock) embedToIncludeNewMethods() {
	panic("not implemented")
}

func newObjectIteratorMock(objAttrs []*storage.ObjectAttrs) *objectIteratorMock {
	return &objectIteratorMock{
		objAttrList: objAttrs,
		current:     0,
	}
}

func newErroneousObjectIteratorMock(err error) stiface.ObjectIterator {
	return &objectIteratorMock{
		err: err,
	}
}

type objectReaderMock struct {
	mock.Mock
}

func (of *objectReaderMock) NewReader(bucket, path string) (io.ReadCloser, error) {
	args := of.Called(bucket, path)
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

type mockrc struct {
	mock.Mock
}

func (rc *mockrc) Read(p []byte) (n int, err error) {
	args := rc.Called()
	err = args.Error(1)
	if err != nil {
		return
	}
	return args.Get(0).(io.Reader).Read(p)
}

func (rc *mockrc) Close() error {
	args := rc.Called()
	return args.Error(0)
}
