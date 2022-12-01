package bigquery

import (
	"context"

	"github.com/kushsharma/parallel"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type Batch struct {
	Dataset        resource.Dataset
	DatasetDetails *resource.Resource

	provider ClientProvider

	Tables         []*resource.Resource
	ExternalTables []*resource.Resource
	Views          []*resource.Resource
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
			dsHandle := client.DatasetHandleFrom(res.Dataset())
			err = createOrUpdate(ctx, dsHandle, res)
			return res, err
		}
	}(dataset))

	for _, table := range b.Tables {
		runner.Add(func(res *resource.Resource) func() (interface{}, error) {
			return func() (interface{}, error) {
				handle := client.TableHandleFrom(res.Dataset(), res.Name())
				err = createOrUpdate(ctx, handle, res)
				return res, err
			}
		}(table))
	}

	for _, extTables := range b.ExternalTables {
		runner.Add(func(res *resource.Resource) func() (interface{}, error) {
			return func() (interface{}, error) {
				handle := client.ExternalTableHandleFrom(res.Dataset(), res.Name())
				err = createOrUpdate(ctx, handle, res)
				return res, err
			}
		}(extTables))
	}

	for _, view := range b.Views {
		runner.Add(func(res *resource.Resource) func() (interface{}, error) {
			return func() (interface{}, error) {
				handle := client.ViewHandleFrom(res.Dataset(), res.Name())
				err = createOrUpdate(ctx, handle, res)
				return res, err
			}
		}(view))
	}
	return nil
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
	if err != nil && !errors.IsErrorType(err, errors.ErrAlreadyExists) {
		res.MarkFailure()
		return err
	}

	return res.MarkSuccess()
}

func update(ctx context.Context, handle ResourceHandle, res *resource.Resource) error {
	if err := handle.Update(ctx, res); err != nil {
		res.MarkFailure()
		return err
	}
	return res.MarkSuccess()
}

func (b *Batch) DatasetOrDefault() (*resource.Resource, error) {
	if b.DatasetDetails != nil {
		return b.DatasetDetails, nil
	}

	fakeTnnt := tenant.Tenant{}
	fakeMeta := &resource.Metadata{
		Description: "dataset created by optimus",
		Labels:      map[string]string{"created_by": "optimus"},
	}
	spec := map[string]any{"description": fakeMeta.Description}
	r, err := resource.NewResource(b.Dataset.FullName(), resource.KindDataset, resource.Bigquery, fakeTnnt, fakeMeta, spec)
	if err != nil {
		return nil, err
	}
	resToCreate := resource.FromExisting(r, resource.ReplaceStatus(resource.StatusToCreate))
	return resToCreate, nil
}

func BatchesFrom(resources []*resource.Resource, provider ClientProvider) map[string]Batch {
	var mapping = make(map[string]Batch)

	for _, res := range resources {
		datasetName := res.Dataset().FullName()

		batch, ok := mapping[datasetName]
		if !ok {
			batch = Batch{
				Dataset:  res.Dataset(),
				provider: provider,
			}
		}

		switch res.Kind() {
		case resource.KindDataset:
			batch.DatasetDetails = res
		case resource.KindView:
			batch.Views = append(batch.Views, res)
		case resource.KindExternalTable:
			batch.ExternalTables = append(batch.ExternalTables, res)
		case resource.KindTable:
			batch.Tables = append(batch.Tables, res)
		default:
		}

		mapping[datasetName] = batch
	}
	return mapping
}
