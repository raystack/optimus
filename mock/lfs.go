package mock

import (
	"io"
	"net/http"
	"os"

	"github.com/stretchr/testify/mock"
	"github.com/odpf/optimus/core/fs"
)

type File struct {
	mock.Mock
}

func (file *File) Read(p []byte) (n int, err error) {
	args := file.Called()
	buf := args.Get(0).(io.Reader)
	return buf.Read(p)
}

func (file *File) Write(p []byte) (n int, err error) {
	args := file.Called()
	buf := args.Get(0).(io.Writer)
	return buf.Write(p)
}

func (file *File) Close() error {
	return file.Called().Error(0)
}

func (file *File) Readdirnames(n int) ([]string, error) {
	args := file.Called(n)
	return args.Get(0).([]string), args.Error(1)
}

func (file *File) IsDir() (bool, error) {
	args := file.Called()
	return args.Get(0).(bool), args.Error(1)
}

type FileSystem struct {
	mock.Mock
}

func (mfs *FileSystem) Open(file string) (fs.File, error) {
	args := mfs.Called(file)
	return args.Get(0).(fs.File), args.Error(1)
}

func (mfs *FileSystem) Create(file string) (fs.File, error) {
	args := mfs.Called(file)
	return args.Get(0).(fs.File), args.Error(1)
}

func (mfs *FileSystem) OpenForWrite(file string) (fs.File, error) {
	args := mfs.Called(file)
	return args.Get(0).(fs.File), args.Error(1)
}

func (mfs *FileSystem) Remove(file string) error {
	args := mfs.Called(file)
	return args.Error(0)
}

type HTTPFileSystem struct {
	mock.Mock
}

func (mfs *HTTPFileSystem) Open(file string) (http.File, error) {
	args := mfs.Called(file)
	return args.Get(0).(http.File), args.Error(1)
}

type HTTPFile struct {
	mock.Mock
}

func (file *HTTPFile) Read(p []byte) (n int, err error) {
	args := file.Called()
	buf := args.Get(0).(io.Reader)
	return buf.Read(p)
}

func (file *HTTPFile) Stat() (os.FileInfo, error) {
	args := file.Called()
	return args.Get(0).(os.FileInfo), args.Error(1)
}

func (file *HTTPFile) Seek(offset int64, whence int) (int64, error) {
	args := file.Called(offset, whence)
	return args.Get(0).(int64), args.Error(1)
}

func (file *HTTPFile) Close() error {
	return file.Called().Error(0)
}

func (file *HTTPFile) Readdir(n int) ([]os.FileInfo, error) {
	args := file.Called(n)
	return args.Get(0).([]os.FileInfo), args.Error(1)
}
