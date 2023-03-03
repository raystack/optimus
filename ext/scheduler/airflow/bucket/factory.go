package bucket

import (
	"context"
	"net/url"

	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/memblob"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/scheduler/airflow"
	"github.com/goto/optimus/internal/errors"
)

const (
	storagePathKey = "STORAGE_PATH"
)

type Factory struct {
	secretsGetter airflow.SecretGetter
	projectGetter airflow.ProjectGetter
}

func (f *Factory) New(ctx context.Context, tnnt tenant.Tenant) (airflow.Bucket, error) {
	parsedURL, err := f.storageURL(ctx, tnnt)
	if err != nil {
		return nil, err
	}

	switch parsedURL.Scheme {
	case "gs":
		return f.GetGCSBucket(ctx, tnnt, parsedURL)

	case "file":
		return fileblob.OpenBucket(parsedURL.Path, &fileblob.Options{
			CreateDir: true,
			Metadata:  fileblob.MetadataDontWrite,
		})

	case "mem":
		return memblob.OpenBucket(nil), nil
	}
	return nil, errors.InvalidArgument("airflow", "unsupported storage config "+parsedURL.String())
}

func (f *Factory) storageURL(ctx context.Context, tnnt tenant.Tenant) (*url.URL, error) {
	project, err := f.projectGetter.Get(ctx, tnnt.ProjectName())
	if err != nil {
		return nil, err
	}

	storagePath, err := project.GetConfig(storagePathKey)
	if err != nil {
		return nil, err
	}

	parsedURL, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.InternalError("airflow", "unable to parse url "+storagePath, err)
	}
	return parsedURL, nil
}

func NewFactory(projectGetter airflow.ProjectGetter, secretsGetter airflow.SecretGetter) *Factory {
	return &Factory{
		secretsGetter: secretsGetter,
		projectGetter: projectGetter,
	}
}
