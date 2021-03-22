package bq2bq

import (
	"context"

	"google.golang.org/api/option"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
)

type defaultBQClientFactory struct{}

func (fac *defaultBQClientFactory) New(ctx context.Context, svcAccount string) (bqiface.Client, error) {
	cred, err := google.CredentialsFromJSON(ctx, []byte(svcAccount), bigquery.Scope)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read secret")
	}
	client, err := bigquery.NewClient(ctx, cred.ProjectID, option.WithCredentials(cred))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create BQ client")
	}
	return bqiface.AdaptClient(client), nil
}
