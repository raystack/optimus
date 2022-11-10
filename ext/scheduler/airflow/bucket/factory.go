package bucket

import (
	"context"
	"net/url"

	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/memblob"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/scheduler/airflow"
	"github.com/odpf/optimus/internal/errors"
)

const (
	storagePathKey = "STORAGE_PATH"
)

type SecretsGetter interface {
	Get(ctx context.Context, projName tenant.ProjectName, namespaceName, name string) (*tenant.PlainTextSecret, error)
}

type ProjectGetter interface {
	Get(context.Context, tenant.ProjectName) (*tenant.Project, error)
}

type Factory struct {
	secretsGetter SecretsGetter
	projectGetter ProjectGetter
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
