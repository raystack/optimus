package bigquery

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	accountKey = "DATASTORE_BIGQUERY"
	store      = "BigqueryStore"
)

type ResourceHandle interface {
	Create(ctx context.Context, res *resource.Resource) error
	Update(ctx context.Context, res *resource.Resource) error
}

type Client interface {
	DatasetHandleFrom(resource.Dataset) ResourceHandle
	ExternalTableHandleFrom(resource.Dataset, resource.Name) ResourceHandle
	TableHandleFrom(resource.Dataset, resource.Name) ResourceHandle
	ViewHandleFrom(resource.Dataset, resource.Name) ResourceHandle
	Close()
}

type ClientProvider interface {
	Get(ctx context.Context, account string) (Client, error)
}

type SecretProvider interface {
	GetSecret(ctx context.Context, tnnt tenant.Tenant, key string) (*tenant.PlainTextSecret, error)
}

type Store struct {
	secretProvider SecretProvider
	clientProvider ClientProvider
}

func (s Store) Create(ctx context.Context, res *resource.Resource) error {
	spanCtx, span := startChildSpan(ctx, "bigquery/CreateResource")
	defer span.End()

	account, err := s.secretProvider.GetSecret(spanCtx, res.Tenant(), accountKey)
	if err != nil {
		return err
	}

	client, err := s.clientProvider.Get(spanCtx, account.Value())
	if err != nil {
		return err
	}
	defer client.Close()

	switch res.Kind() {
	case resource.KindDataset:
		handle := client.DatasetHandleFrom(res.Dataset())
		return handle.Create(spanCtx, res)

	case resource.KindTable:
		handle := client.TableHandleFrom(res.Dataset(), res.Name())
		return handle.Create(spanCtx, res)

	case resource.KindExternalTable:
		handle := client.ExternalTableHandleFrom(res.Dataset(), res.Name())
		return handle.Create(spanCtx, res)

	case resource.KindView:
		handle := client.ViewHandleFrom(res.Dataset(), res.Name())
		return handle.Create(spanCtx, res)

	default:
		return errors.InvalidArgument(store, "invalid kind for bigquery resource "+res.Kind().String())
	}
}

func (s Store) Update(ctx context.Context, res *resource.Resource) error {
	spanCtx, span := startChildSpan(ctx, "bigquery/UpdateResource")
	defer span.End()

	account, err := s.secretProvider.GetSecret(spanCtx, res.Tenant(), accountKey)
	if err != nil {
		return err
	}

	client, err := s.clientProvider.Get(spanCtx, account.Value())
	if err != nil {
		return err
	}
	defer client.Close()

	switch res.Kind() {
	case resource.KindDataset:
		handle := client.DatasetHandleFrom(res.Dataset())
		return handle.Update(spanCtx, res)

	case resource.KindTable:
		handle := client.TableHandleFrom(res.Dataset(), res.Name())
		return handle.Update(spanCtx, res)

	case resource.KindExternalTable:
		handle := client.ExternalTableHandleFrom(res.Dataset(), res.Name())
		return handle.Update(spanCtx, res)

	case resource.KindView:
		handle := client.ViewHandleFrom(res.Dataset(), res.Name())
		return handle.Update(spanCtx, res)

	default:
		return errors.InvalidArgument(store, "invalid kind for bigquery resource "+res.Kind().String())
	}
}

func (Store) BatchUpdate(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource) error {
	panic("implement me")
}

func startChildSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("datastore/bigquery")

	return tracer.Start(ctx, name)
}

func NewBigqueryDataStore(secretProvider SecretProvider, clientProvider ClientProvider) *Store {
	return &Store{
		secretProvider: secretProvider,
		clientProvider: clientProvider,
	}
}
