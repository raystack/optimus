package gcs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

type objectWriter interface {
	NewWriter(bucket, path string) (io.WriteCloser, error)
}

type gcsObjectWriter struct {
	c *storage.Client
}

func (gcs *gcsObjectWriter) NewWriter(bucket, path string) (io.WriteCloser, error) {
	b := gcs.c.Bucket(bucket)
	if _, err := b.Attrs(context.Background()); err != nil {
		return nil, err
	}
	return b.Object(path).NewWriter(context.Background()), nil
}

type objectReader interface {
	NewReader(bucket, path string) (io.ReadCloser, error)
}

type gcsObjectReader struct {
	c *storage.Client
}

func (gcs *gcsObjectReader) NewReader(bucket, path string) (io.ReadCloser, error) {
	b := gcs.c.Bucket(bucket)
	if _, err := b.Attrs(context.Background()); err != nil {
		return nil, err
	}

	reader, err := b.Object(path).NewReader(context.Background())
	if err != nil {
		return nil, err
	}
	return reader, nil
}
