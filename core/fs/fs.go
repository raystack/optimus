package fs

import (
	"errors"
	"io"
)

var (
	// ErrNoSuchFile is return by FileSystem when the target
	// of an Open call does not exist
	ErrNoSuchFile = errors.New("file not found")

	// ErrInvalidDirectory is returned by File.Readdirnames when
	// the file in question is not a directory
	ErrInvalidDirectory = errors.New("file is not a directory")
)

// File represents a file on a storage system
type File interface {
	io.ReadWriteCloser

	// Readdirnames returns a list of files within the directory
	// if the file is NOT a directory, it should return ErrInvalidDirectory
	Readdirnames(n int) ([]string, error)
}

// FileSystem implements interface to a storage system
type FileSystem interface {
	// Open should return an existing File
	// It should return ErrNoSuchFile in case a file does not
	// exist.
	Open(string) (File, error)

	// Create should open a file for writing. If the file
	// already exists, it SHOULD return os.ErrExist
	Create(string) (File, error)

	// OpenForWrite is similar to Create, however it doesn't
	// fail if the file already exists, instead it truncates the
	// files and opens it for writing
	OpenForWrite(string) (File, error)

	// Remove should delete a file
	// will return err.ErrIsNotExist if the file is not exist
	Remove(string) error
}
