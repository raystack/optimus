package bigquery

import (
	"context"

	"github.com/kushsharma/parallel"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type Batch struct {
	dataset        resource.Dataset
	datasetDetails *resource.Resource

	provider ClientProvider

	tables         []*resource.Resource
	externalTables []*resource.Resource
	views          []*resource.Resource
}

func (b *Batch) QueueJobs(ctx context.Context, account string, runner *parallel.Runner) error {
	client, err := b.provider.Get(ctx, account)
	if err != nil {
		return err
	}

	dataset, err := b.DatasetOrDefault()
	if err != nil {
		return err
	}

	runner.Add(func(res *resource.Resource) func() (interface{}, error) {
		return func() (interface{}, error) {
			dsHandle := client.DatasetHandleFrom(res)
			err = checkOrCreateDataset(ctx, dsHandle, res)
			return nil, err
		}
	}(dataset))

	for _, table := range b.tables {
		runner.Add(func(res *resource.Resource) func() (interface{}, error) {
			return func() (interface{}, error) {
				handle := client.TableHandleFrom(res)
				err = createOrUpdate(ctx, handle, res)
				return nil, err
			}
		}(table))
	}

	for _, extTables := range b.externalTables {
		runner.Add(func(res *resource.Resource) func() (interface{}, error) {
			return func() (interface{}, error) {
				handle := client.TableHandleFrom(res)
				err = createOrUpdate(ctx, handle, res)
				return nil, err
			}
		}(extTables))
	}

	for _, view := range b.views {
		runner.Add(func(res *resource.Resource) func() (interface{}, error) {
			return func() (interface{}, error) {
				handle := client.TableHandleFrom(res)
				err = createOrUpdate(ctx, handle, res)
				return nil, err
			}
		}(view))
	}
	return nil
}

func checkOrCreateDataset(ctx context.Context, handle ResourceHandle, res *resource.Resource) error {
	if res.Status() == resource.StatusToUpdate {
		return update(ctx, handle, res)
	}
	// Can be to create or status unknown
	return create(ctx, handle, res)
}

func createOrUpdate(ctx context.Context, handle ResourceHandle, res *resource.Resource) error {
	if res.Status() == resource.StatusToUpdate {
		return update(ctx, handle, res)
	} else if res.Status() == resource.StatusToCreate {
		return create(ctx, handle, res)
	}
	return nil
}

func create(ctx context.Context, handle ResourceHandle, res *resource.Resource) error {
	if handle.Exists(ctx) {
		return res.MarkSuccess()
	}

	err := handle.Create(ctx, res)
	if !errors.IsErrorType(err, errors.ErrAlreadyExists) {
		return res.MarkFailed()
	}

	return res.MarkSuccess()
}

func update(ctx context.Context, handle ResourceHandle, res *resource.Resource) error {
	err := handle.Update(ctx, res)
	if err != nil {
		res.MarkFailed()
		return err
	}
	return res.MarkSuccess()
}

func (b *Batch) DatasetOrDefault() (*resource.Resource, error) {
	if b.datasetDetails != nil {
		return b.datasetDetails, nil
	}

	fakeTnnt := tenant.Tenant{}
	fakeMeta := &resource.Metadata{
		Description: "dataset created by optimus",
		Labels:      map[string]string{"created_by": "optimus"},
	}
	spec := map[string]any{}
	r, err := resource.NewResource(b.dataset.FullName(), resource.KindDataset, resource.Bigquery, fakeTnnt, fakeMeta, spec)
	if err != nil {
		return nil, err
	}
	resToCreate := resource.FromExisting(r, resource.ReplaceStatus(resource.StatusToCreate))
	return resToCreate, nil
}

func BatchesFrom(resources []*resource.Resource, provider ClientProvider) map[string]Batch {
	var mapping map[string]Batch

	for _, res := range resources {
		datasetName := res.Dataset().FullName()

		batch, ok := mapping[datasetName]
		if !ok {
			batch = Batch{
				dataset:  res.Dataset(),
				provider: provider,
			}
		}

		switch res.Kind() {
		case resource.KindDataset:
			batch.datasetDetails = res
		case resource.KindView:
			batch.views = append(batch.views, res)
		case resource.KindExternalTable:
			batch.externalTables = append(batch.externalTables, res)
		case resource.KindTable:
			batch.tables = append(batch.tables, res)
		default:
		}

		mapping[datasetName] = batch
	}
	return mapping
}
