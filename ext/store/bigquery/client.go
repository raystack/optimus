package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

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

func (c *BqClient) DatasetHandleFrom(ds Dataset) ResourceHandle {
	dsHandle := c.bq.DatasetInProject(ds.Project, ds.DatasetName)
	return NewDatasetHandle(dsHandle)
}

func (c *BqClient) TableHandleFrom(ds Dataset, name string) ResourceHandle {
	t := c.bq.DatasetInProject(ds.Project, ds.DatasetName).Table(name)
	return NewTableHandle(t)
}

func (c *BqClient) ExternalTableHandleFrom(ds Dataset, name string) ResourceHandle {
	t := c.bq.DatasetInProject(ds.Project, ds.DatasetName).Table(name)
	return NewExternalTableHandle(t)
}

func (c *BqClient) ViewHandleFrom(ds Dataset, name string) ResourceHandle {
	t := c.bq.DatasetInProject(ds.Project, ds.DatasetName).Table(name)
	return NewViewHandle(t)
}

func (c *BqClient) Close() {
	c.bq.Close()
}
