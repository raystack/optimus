package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
)

type BqClientProvider struct{}

func NewClientProvider() *BqClientProvider {
	return &BqClientProvider{}
}

func (BqClientProvider) Get(ctx context.Context, account string) (Client, error) {
	return NewClient(ctx, account)
}

type BqClient struct {
	bq *bigquery.Client
}

func NewClient(ctx context.Context, svcAccount string) (*BqClient, error) {
	cred, err := google.CredentialsFromJSON(ctx, []byte(svcAccount), bigquery.Scope)
	if err != nil {
		return nil, errors.InternalError(store, "failed to read account", err)
	}

	c, err := bigquery.NewClient(ctx, cred.ProjectID, option.WithCredentials(cred))
	if err != nil {
		return nil, errors.InternalError(store, "failed to create BQ client", err)
	}

	return &BqClient{bq: c}, nil
}

func (c *BqClient) DatasetHandleFrom(dataset resource.Dataset) ResourceHandle {
	ds := c.bq.Dataset(dataset.Schema)
	return NewDatasetHandle(ds)
}

func (c *BqClient) TableHandleFrom(dataset resource.Dataset, name resource.Name) TableResourceHandle {
	t := c.bq.Dataset(dataset.Schema).Table(name.String())
	return NewTableHandle(t)
}

func (c *BqClient) ExternalTableHandleFrom(dataset resource.Dataset, name resource.Name) ResourceHandle {
	t := c.bq.Dataset(dataset.Schema).Table(name.String())
	return NewExternalTableHandle(t)
}

func (c *BqClient) ViewHandleFrom(dataset resource.Dataset, name resource.Name) ResourceHandle {
	t := c.bq.Dataset(dataset.Schema).Table(name.String())
	return NewViewHandle(t)
}

func (c *BqClient) Close() {
	c.bq.Close()
}
