package bigquery

import (
	"context"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/kushsharma/parallel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
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

type TableResourceHandle interface {
	ResourceHandle
	GetBQTable() (*bq.Table, error)
	CopierFrom(source TableResourceHandle) (TableCopier, error)
	UpdateExpiry(ctx context.Context, name string, expiry time.Time) error
}

type Client interface {
	DatasetHandleFrom(dataset Dataset) ResourceHandle
	TableHandleFrom(dataset Dataset, name string) TableResourceHandle
	ExternalTableHandleFrom(dataset Dataset, name string) ResourceHandle
	ViewHandleFrom(dataset Dataset, name string) ResourceHandle
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

	dataset, err := DataSetFor(res)
	if err != nil {
		return err
	}
	resourceName, err := ResourceNameFor(res)
	if err != nil {
		return err
	}

	switch res.Kind() {
	case KindDataset:
		handle := client.DatasetHandleFrom(dataset)
		return handle.Create(spanCtx, res)

	case KindTable:
		handle := client.TableHandleFrom(dataset, resourceName)
		return handle.Create(spanCtx, res)

	case KindExternalTable:
		handle := client.ExternalTableHandleFrom(dataset, resourceName)
		return handle.Create(spanCtx, res)

	case KindView:
		handle := client.ViewHandleFrom(dataset, resourceName)
		return handle.Create(spanCtx, res)

	default:
		return errors.InvalidArgument(store, "invalid kind for bigquery resource "+res.Kind())
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

	dataset, err := DataSetFor(res)
	if err != nil {
		return err
	}
	resourceName, err := ResourceNameFor(res)
	if err != nil {
		return err
	}

	switch res.Kind() {
	case KindDataset:
		handle := client.DatasetHandleFrom(dataset)
		return handle.Update(spanCtx, res)

	case KindTable:
		handle := client.TableHandleFrom(dataset, resourceName)
		return handle.Update(spanCtx, res)

	case KindExternalTable:
		handle := client.ExternalTableHandleFrom(dataset, resourceName)
		return handle.Update(spanCtx, res)

	case KindView:
		handle := client.ViewHandleFrom(dataset, resourceName)
		return handle.Update(spanCtx, res)

	default:
		return errors.InvalidArgument(store, "invalid kind for bigquery resource "+res.Kind())
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

	me := errors.NewMultiError("error while resource batch update")

	batches, err := BatchesFrom(resources, s.clientProvider)
	me.Append(err)

	runner := parallel.NewRunner(parallel.WithLimit(ConcurrentLimit), parallel.WithTicket(ConcurrentTicketPerSec))
	for _, batch := range batches {
		err = batch.QueueJobs(ctx, account.Value(), runner)
		if err != nil {
			return err
		}
	}

	states := runner.Run()
	for _, state := range states {
		me.Append(state.Err)
	}

	return errors.MultiToError(me)
}

func (Store) Validate(r *resource.Resource) error {
	err := ValidateName(r)
	if err != nil {
		return err
	}

	switch r.Kind() {
	case KindTable:
		table, err := ConvertSpecTo[Table](r)
		if err != nil {
			return err
		}
		table.Name = r.Name()
		return table.Validate()

	case KindExternalTable:
		externalTable, err := ConvertSpecTo[ExternalTable](r)
		if err != nil {
			return err
		}
		externalTable.Name = r.Name()
		return externalTable.Validate()

	case KindView:
		view, err := ConvertSpecTo[View](r)
		if err != nil {
			return err
		}
		view.Name = r.Name()
		return view.Validate()

	case KindDataset:
		ds, err := ConvertSpecTo[DatasetDetails](r)
		if err != nil {
			return err
		}
		return ds.Validate()

	default:
		return errors.InvalidArgument(resource.EntityResource, "unknown kind")
	}
}

func (Store) GetURN(res *resource.Resource) (string, error) {
	return URNFor(res)
}

func (s Store) Backup(ctx context.Context, backup *resource.Backup, resources []*resource.Resource) (*resource.BackupResult, error) {
	account, err := s.secretProvider.GetSecret(ctx, backup.Tenant(), accountKey)
	if err != nil {
		return nil, err
	}

	client, err := s.clientProvider.Get(ctx, account.Value())
	if err != nil {
		return nil, err
	}
	defer client.Close()

	return BackupResources(ctx, backup, resources, client)
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
