package bucket

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"

	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/scheduler/airflow"
)

const (
	gcsStorageKey = "STORAGE"
	scope         = "https://www.googleapis.com/auth/cloud-platform"
)

func (f *Factory) GetGCSBucket(ctx context.Context, tnnt tenant.Tenant, parsedURL *url.URL) (airflow.Bucket, error) {
	spanCtx, span := otel.Tracer("airflow/bucketFactory").Start(ctx, "GetGCSBucket")
	defer span.End()

	storageSecret, err := f.secretsGetter.Get(spanCtx, tnnt.ProjectName(), tnnt.NamespaceName().String(), gcsStorageKey)
	if err != nil {
		return nil, err
	}

	creds, err := google.CredentialsFromJSON(spanCtx, []byte(storageSecret.Value()), scope)
	if err != nil {
		return nil, err
	}

	client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))
	if err != nil {
		return nil, err
	}

	gcsBucket, err := gcsblob.OpenBucket(spanCtx, client, parsedURL.Host, nil)
	if err != nil {
		return nil, err
	}

	if parsedURL.Path == "" {
		return gcsBucket, nil
	}

	prefix := fmt.Sprintf("%s/", strings.Trim(parsedURL.Path, "/\\"))
	return blob.PrefixedBucket(gcsBucket, prefix), nil
}
