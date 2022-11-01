package bigquery

import (
	"context"

	"github.com/kushsharma/parallel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	accountKey = "DATASTORE_BIGQUERY"
	store      = "BigqueryStore"

	ConcurrentTicketPerSec = 5
	ConcurrentLimit        = 20
)

type ResourceHandle interface {
	Create(ctx context.Context, res *resource.Resource) error
	Update(ctx context.Context, res *resource.Resource) error
	Exists(ctx context.Context) bool
}

type Client interface {
	DatasetHandleFrom(*resource.Resource) ResourceHandle
	ExternalTableHandleFrom(*resource.Resource) ResourceHandle
	TableHandleFrom(*resource.Resource) ResourceHandle
	ViewHandleFrom(*resource.Resource) ResourceHandle
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
		handle := client.DatasetHandleFrom(res)
		return handle.Create(spanCtx, res)

	case resource.KindTable:
		handle := client.TableHandleFrom(res)
		return handle.Create(spanCtx, res)

	case resource.KindExternalTable:
		handle := client.ExternalTableHandleFrom(res)
		return handle.Create(spanCtx, res)

	case resource.KindView:
		handle := client.ViewHandleFrom(res)
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
		handle := client.DatasetHandleFrom(res)
		return handle.Update(spanCtx, res)

	case resource.KindTable:
		handle := client.TableHandleFrom(res)
		return handle.Update(spanCtx, res)

	case resource.KindExternalTable:
		handle := client.ExternalTableHandleFrom(res)
		return handle.Update(spanCtx, res)

	case resource.KindView:
		handle := client.ViewHandleFrom(res)
		return handle.Update(spanCtx, res)

	default:
		return errors.InvalidArgument(store, "invalid kind for bigquery resource "+res.Kind().String())
	}
}

func (s Store) BatchUpdate(ctx context.Context, resources []*resource.Resource) error {
	spanCtx, span := startChildSpan(ctx, "bigquery/BatchUpdate")
	defer span.End()

	if len(resources) == 0 {
		return nil
	}

	tnnt := resources[0].Tenant()
	account, err := s.secretProvider.GetSecret(spanCtx, tnnt, accountKey)
	if err != nil {
		return err
	}

	batches := BatchesFrom(resources, s.clientProvider)
	runner := parallel.NewRunner(parallel.WithLimit(ConcurrentLimit), parallel.WithTicket(ConcurrentTicketPerSec))
	for _, batch := range batches {
		err = batch.QueueJobs(ctx, account.Value(), runner)
		if err != nil {
			return err
		}
	}

	states := runner.Run()
	me := errors.NewMultiError("error while resource batch update")
	for _, s := range states {
		me.Append(s.Err)
	}

	return errors.MultiToError(me)
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
