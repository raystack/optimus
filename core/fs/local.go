package fs

import (
	"os"
	"path"
)

type file struct {
	*os.File
	readOnly bool
}

func (f file) Readdirnames(n int) ([]string, error) {
	fi, err := f.Stat()
	if err == nil && fi.IsDir() == false {
		return []string{}, ErrInvalidDirectory
	}
	return f.File.Readdirnames(n)
}

func (f file) IsDir() (bool, error) {
	fi, err := f.Stat()
	if err != nil {
		return false, err
	}
	return fi.IsDir(), err
}

func (f file) Write(p []byte) (n int, err error) {
	if f.readOnly {
		return 0, errNotWritable
	}
	return f.File.Write(p)
}

// LocalFileSystem implements a FileSystem as a wrapper
// over os.* Functions
type LocalFileSystem struct {
	BasePath string
	readOnly bool
}

func (fs *LocalFileSystem) Open(filePath string) (File, error) {
	filePath = path.Join(fs.BasePath, filePath)
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNoSuchFile
		}
		return nil, err
	}
	return file{File: f, readOnly: fs.readOnly}, err
}

func (fs *LocalFileSystem) Create(filePath string) (File, error) {
	if fs.readOnly {
		return nil, errCantModify
	}
	filePath = path.Join(fs.BasePath, filePath)
	_, err := os.Stat(filePath)
	if err != nil || os.IsNotExist(err) {
		dirName := path.Dir(filePath)
		os.MkdirAll(dirName, os.ModeDir|os.ModePerm)
		f, err := os.Create(filePath)
		return file{File: f}, err
	}
	return nil, os.ErrExist
}

func (fs *LocalFileSystem) Remove(filePath string) error {
	if fs.readOnly {
		return errCantModify
	}
	filePath = path.Join(fs.BasePath, filePath)
	_, err := os.Stat(filePath)
	if err != nil || os.IsNotExist(err) {
		return ErrNoSuchFile
	}

	err = os.Remove(filePath)
	if err != nil {
		return err
	}

	return nil
}

func (fs *LocalFileSystem) OpenForWrite(filePath string) (File, error) {
	if fs.readOnly {
		return nil, errCantModify
	}
	filePath = path.Join(fs.BasePath, filePath)
	dirName := path.Dir(filePath)
	os.MkdirAll(dirName, os.ModeDir|os.ModePerm)
	f, err := os.Create(filePath)
	return file{File: f}, err
}
