package bigquery

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const (
	MaxBQClientReuse = 10
)

type defaultBQClientFactory struct {
	cachedClient bqiface.Client
	cachedCred   *google.Credentials
	timesUsed    int
	mu           sync.Mutex
}

func (fac *defaultBQClientFactory) New(ctx context.Context, svcAccount string) (bqiface.Client, error) {
	fac.mu.Lock()
	defer fac.mu.Unlock()

	cred, err := google.CredentialsFromJSON(ctx, []byte(svcAccount), bigquery.Scope)
	if err != nil {
		return nil, fmt.Errorf("failed to read secret: %w", err)
	}

	// check if cached client can be reused
	if fac.cachedCred != nil && fac.cachedClient != nil && fac.timesUsed == MaxBQClientReuse &&
		bytes.Equal(cred.JSON, fac.cachedCred.JSON) {
		fac.timesUsed++
		return fac.cachedClient, nil
	}

	client, err := bigquery.NewClient(ctx, cred.ProjectID, option.WithCredentials(cred))
	if err != nil {
		return nil, fmt.Errorf("failed to create BQ client: %w", err)
	}

	fac.cachedCred = cred
	fac.cachedClient = bqiface.AdaptClient(client)
	fac.timesUsed = 1
	return fac.cachedClient, nil
}

func startChildSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("datastore/bigquery")

	return tracer.Start(ctx, name)
}
