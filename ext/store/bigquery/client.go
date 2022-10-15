package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
)

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

func (c *BqClient) DatasetHandleFrom(dataset resource.Dataset) *DatasetHandle {
	ds := c.bq.Dataset(dataset.Schema)
	return NewDatasetHandle(ds)
}

func (c *BqClient) TableHandleFrom(dataset resource.Dataset, name resource.Name) *TableHandle {
	t := c.bq.Dataset(dataset.Schema).Table(name.String())
	return NewTableHandle(t)
}

func (c *BqClient) ExternalTableHandleFrom(res *resource.Resource) *ExternalTableHandle {
	t := c.bq.Dataset(res.Dataset().Schema).Table(res.Name().String())
	return NewExternalTableHandle(t)
}

func (c *BqClient) ViewHandleFrom(res *resource.Resource) *ViewHandle {
	t := c.bq.Dataset(res.Dataset().Schema).Table(res.Name().String())
	return NewViewHandle(t)
}

func (c *BqClient) Close() {
	c.bq.Close()
}
