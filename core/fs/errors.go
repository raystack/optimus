package fs

import "errors"

var (
	errNotWritable = errors.New("file not open for writing")
	errCantModify  = errors.New("file system doesn't support modification")
)
