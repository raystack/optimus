package gcs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

type GcsObjectWriter struct {
	Client *storage.Client
}

func (gcs *GcsObjectWriter) NewWriter(ctx context.Context, bucket, path string) (io.WriteCloser, error) {
	b := gcs.Client.Bucket(bucket)
	if _, err := b.Attrs(ctx); err != nil {
		return nil, err
	}
	return b.Object(path).NewWriter(ctx), nil
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
