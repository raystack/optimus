package bigquery_test

import (
	"context"
	"errors"
	"testing"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/googleapi"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/store/bigquery"
)

func TestDatasetHandle(t *testing.T) {
	ctx := context.Background()
	bqStore := resource.BigQuery
	tnnt, _ := tenant.NewTenant("proj", "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			ds := new(mockBigQueryDataset)
			dsHandle := bigquery.NewDatasetHandle(ds)

			spec := map[string]any{"description": []string{"a", "b"}}
			res, err := resource.NewResource("proj.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = dsHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: 1 error(s) decoding:\n\n* "+
				"'description' expected type 'string', got unconvertible type '[]string', value: '[a b]': not able to "+
				"decode spec for proj.dataset")
		})
		t.Run("returns error when dataset already present on bigquery", func(t *testing.T) {
			bqErr := &googleapi.Error{Code: 409, Message: "Already Exists project.dataset"}
			ds := new(mockBigQueryDataset)
			ds.On("Create", ctx, mock.Anything).Return(bqErr)
			defer ds.AssertExpectations(t)

			dsHandle := bigquery.NewDatasetHandle(ds)

			spec := map[string]any{"description": "test create"}
			res, err := resource.NewResource("proj.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = dsHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "resource already exists for entity resource_dataset: dataset already "+
				"exists on bigquery: proj.dataset")
		})
		t.Run("returns error when bigquery returns error", func(t *testing.T) {
			ds := new(mockBigQueryDataset)
			ds.On("Create", ctx, mock.Anything).Return(errors.New("some error"))
			defer ds.AssertExpectations(t)

			dsHandle := bigquery.NewDatasetHandle(ds)

			spec := map[string]any{"description": "test create"}
			res, err := resource.NewResource("proj.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = dsHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource_dataset: failed to create "+
				"resource proj.dataset")
		})
		t.Run("successfully creates the resource", func(t *testing.T) {
			ds := new(mockBigQueryDataset)
			ds.On("Create", ctx, mock.Anything).Return(nil)
			defer ds.AssertExpectations(t)

			dsHandle := bigquery.NewDatasetHandle(ds)

			spec := map[string]any{
				"description":      "test create",
				"location":         "asia-southeast2",
				"table_expiration": 2,
			}
			res, err := resource.NewResource("proj.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = dsHandle.Create(ctx, res)
			assert.Nil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			ds := new(mockBigQueryDataset)
			dsHandle := bigquery.NewDatasetHandle(ds)

			spec := map[string]any{"description": []string{"a", "b"}}
			res, err := resource.NewResource("proj.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = dsHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: 1 error(s) decoding:\n\n* "+
				"'description' expected type 'string', got unconvertible type '[]string', value: '[a b]': not able to "+
				"decode spec for proj.dataset")
		})
		t.Run("returns error when dataset not present on bigquery", func(t *testing.T) {
			bqErr := &googleapi.Error{Code: 404}
			ds := new(mockBigQueryDataset)
			ds.On("Update", ctx, mock.Anything, "").Return(nil, bqErr)
			defer ds.AssertExpectations(t)

			dsHandle := bigquery.NewDatasetHandle(ds)

			spec := map[string]any{"description": "test update"}
			res, err := resource.NewResource("proj.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = dsHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found for entity resource_dataset: failed to update dataset in "+
				"bigquery for proj.dataset")
		})
		t.Run("returns error when bigquery returns error", func(t *testing.T) {
			ds := new(mockBigQueryDataset)
			ds.On("Update", ctx, mock.Anything, "").Return(nil, errors.New("some error"))
			defer ds.AssertExpectations(t)

			dsHandle := bigquery.NewDatasetHandle(ds)

			spec := map[string]any{"description": "test update"}
			res, err := resource.NewResource("proj.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = dsHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource_dataset: failed to update resource "+
				"on bigquery for proj.dataset")
		})
		t.Run("successfully updates the resource", func(t *testing.T) {
			meta := &bq.DatasetMetadata{
				Description: "test update",
			}
			ds := new(mockBigQueryDataset)
			ds.On("Update", ctx, mock.Anything, "").Return(meta, nil)
			defer ds.AssertExpectations(t)

			dsHandle := bigquery.NewDatasetHandle(ds)

			spec := map[string]any{
				"description":      "test update",
				"location":         "asia-southeast2",
				"table_expiration": 2,
			}
			res, err := resource.NewResource("proj.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = dsHandle.Update(ctx, res)
			assert.Nil(t, err)
		})
	})
	t.Run("Exists", func(t *testing.T) {
		t.Run("returns false when error in getting metadata", func(t *testing.T) {
			ds := new(mockBigQueryDataset)
			ds.On("Metadata", ctx).Return(nil, errors.New("error in get"))
			defer ds.AssertExpectations(t)

			dsHandle := bigquery.NewDatasetHandle(ds)

			exists := dsHandle.Exists(ctx)
			assert.False(t, exists)
		})
		t.Run("returns true when gets metadata", func(t *testing.T) {
			meta := &bq.DatasetMetadata{
				Description: "test update",
			}
			ds := new(mockBigQueryDataset)
			ds.On("Metadata", ctx).Return(meta, nil)
			defer ds.AssertExpectations(t)

			dsHandle := bigquery.NewDatasetHandle(ds)

			exists := dsHandle.Exists(ctx)
			assert.True(t, exists)
		})
	})
}

type mockBigQueryDataset struct {
	mock.Mock
}

func (bqDS *mockBigQueryDataset) Create(ctx context.Context, metadata *bq.DatasetMetadata) error {
	args := bqDS.Called(ctx, metadata)
	return args.Error(0)
}

func (bqDS *mockBigQueryDataset) Update(ctx context.Context, update bq.DatasetMetadataToUpdate, etag string) (*bq.DatasetMetadata, error) {
	args := bqDS.Called(ctx, update, etag)
	var rs *bq.DatasetMetadata
	if args.Get(0) != nil {
		rs = args.Get(0).(*bq.DatasetMetadata)
	}
	return rs, args.Error(1)
}

func (bqDS *mockBigQueryDataset) Metadata(ctx context.Context) (*bq.DatasetMetadata, error) {
	args := bqDS.Called(ctx)
	var rs *bq.DatasetMetadata
	if args.Get(0) != nil {
		rs = args.Get(0).(*bq.DatasetMetadata)
	}
	return rs, args.Error(1)
}
