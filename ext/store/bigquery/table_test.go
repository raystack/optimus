package bigquery_test

import (
	"context"
	"errors"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/googleapi"

	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/core/tenant"
	"github.com/raystack/optimus/ext/store/bigquery"
)

func TestTableHandle(t *testing.T) {
	ctx := context.Background()
	bqStore := resource.Bigquery
	tnnt, _ := tenant.NewTenant("proj", "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}

	t.Run("Create", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			table := new(mockBigQueryTable)
			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{"description": []string{"a", "b"}}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for proj.dataset.table1")
		})
		t.Run("returns error when cannot get metadata", func(t *testing.T) {
			table := new(mockBigQueryTable)
			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{
				"description":     "test create",
				"expiration_time": "invalid_date",
			}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to get metadata to create for proj.dataset.table1")
		})
		t.Run("returns error when table already present on bigquery", func(t *testing.T) {
			bqErr := &googleapi.Error{Code: 409, Message: "Already Exists project.dataset.table1"}
			table := new(mockBigQueryTable)
			table.On("Create", ctx, mock.Anything).Return(bqErr)
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{"description": "test create"}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "table already exists on bigquery: proj.dataset.table1")
		})
		t.Run("returns error when bigquery returns error", func(t *testing.T) {
			table := new(mockBigQueryTable)
			table.On("Create", ctx, mock.Anything).Return(errors.New("some error"))
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{"description": "test create"}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to create resource proj.dataset.table1")
		})
		t.Run("successfully creates the resource with range partition", func(t *testing.T) {
			table := new(mockBigQueryTable)
			table.On("Create", ctx, mock.Anything).Return(nil)
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{
				"description":     "test create",
				"expiration_time": time.Now().Format(time.RFC3339),
				"schema": []map[string]any{
					{
						"name": "session",
						"type": "STRING",
					},
					{
						"name": "product_name",
						"type": "STRING",
					},
					{
						"name": "product_id",
						"type": "INTEGER",
					},
				},
				"partition": map[string]any{
					"field": "product_id",
					"type":  "range",
					"range": map[string]any{
						"start":    0,
						"end":      100,
						"interval": 2,
					},
				},
				"cluster": map[string]any{
					"using": []string{
						"product_name",
						"session",
					},
				},
			}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Create(ctx, res)
			assert.Nil(t, err)
		})
		t.Run("successfully creates the resource with hour partition", func(t *testing.T) {
			table := new(mockBigQueryTable)
			table.On("Create", ctx, mock.Anything).Return(nil)
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{
				"description":     "test create",
				"expiration_time": time.Now().Format(time.RFC3339),
				"schema": []map[string]any{
					{
						"name": "session",
						"type": "STRING",
					},
				},
				"partition": map[string]any{
					"field": "product_id",
					"type":  "hour",
				},
			}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Create(ctx, res)
			assert.Nil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			table := new(mockBigQueryTable)
			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{"description": []string{"a", "b"}}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "not able to decode spec for proj.dataset.table1")
		})
		t.Run("returns error when creating metadata fails", func(t *testing.T) {
			table := new(mockBigQueryTable)
			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{
				"description":     "test update",
				"expiration_time": "invalid_date",
			}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to get metadata to update for proj.dataset.table1")
		})
		t.Run("returns error when table not present on bigquery", func(t *testing.T) {
			bqErr := &googleapi.Error{Code: 404}
			table := new(mockBigQueryTable)
			table.On("Update", ctx, mock.Anything, "", emptyUpdateOptions).Return(nil, bqErr)
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{"description": "test update"}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to update table in bigquery for proj.dataset.table1")
		})
		t.Run("returns error when bigquery returns error", func(t *testing.T) {
			table := new(mockBigQueryTable)
			table.On("Update", ctx, mock.Anything, "", emptyUpdateOptions).Return(nil, errors.New("some error"))
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{"description": "test update"}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to update table on bigquery for proj.dataset.table1")
		})
		t.Run("successfully updates the resource", func(t *testing.T) {
			meta := &bq.TableMetadata{
				Description: "test update",
			}
			table := new(mockBigQueryTable)
			table.On("Update", ctx, mock.Anything, "", emptyUpdateOptions).Return(meta, nil)
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			spec := map[string]any{
				"description":     "test create",
				"expiration_time": time.Now().Format(time.RFC3339),
				"schema": []map[string]any{
					{
						"name": "session",
						"type": "STRING",
					},
					{
						"name": "product_name",
						"type": "STRING",
					},
					{
						"name": "product_id",
						"type": "INTEGER",
					},
				},
				"partition": map[string]any{
					"field": "product_day",
					"type":  "DAY",
				},
			}
			res, err := resource.NewResource("proj.dataset.table1", bigquery.KindView, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = tHandle.Update(ctx, res)
			assert.Nil(t, err)
		})
	})
	t.Run("GetCopier", func(t *testing.T) {
		t.Run("returns error when source is nil", func(t *testing.T) {
			table := new(mockBigQueryTable)
			tHandle := bigquery.NewTableHandle(table)

			_, err := tHandle.CopierFrom(nil)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "source handle is nil")
		})
		t.Run("returns error when destination is not table", func(t *testing.T) {
			mockTable1 := new(mockBigQueryTable)
			source := bigquery.NewTableHandle(mockTable1)

			table := new(mockBigQueryTable)
			tHandle := bigquery.NewTableHandle(table)

			_, err := tHandle.CopierFrom(source)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "not able to convert to bigquery table")
		})
		t.Run("returns the table copier", func(t *testing.T) {
			table1 := &bq.Table{
				ProjectID: "test",
				DatasetID: "backup",
				TableID:   "backup_table",
			}
			dest := bigquery.NewTableHandle(table1)

			table := new(mockBigQueryTable)
			table.On("CopierFrom", []*bq.Table{table1}).Return(&bq.Copier{})
			tHandle := bigquery.NewTableHandle(table)

			copier, err := tHandle.CopierFrom(dest)
			assert.Nil(t, err)
			assert.NotNil(t, copier)
		})
	})
	t.Run("UpdateExpiry", func(t *testing.T) {
		t.Run("returns error when table not found", func(t *testing.T) {
			bqErr := &googleapi.Error{Code: 404}
			table := new(mockBigQueryTable)
			table.On("Update", ctx, mock.Anything, "", emptyUpdateOptions).Return(nil, bqErr)
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			tableName := "proj.dataset.table1"
			expiry := time.Now().AddDate(0, 3, 0)

			err := tHandle.UpdateExpiry(ctx, tableName, expiry)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to update table in bigquery for proj.dataset.table1")
		})
		t.Run("returns error when error during update", func(t *testing.T) {
			table := new(mockBigQueryTable)
			table.On("Update", ctx, mock.Anything, "", emptyUpdateOptions).Return(nil, errors.New("some error"))
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			tableName := "proj.dataset.table1"
			expiry := time.Now().AddDate(0, 3, 0)

			err := tHandle.UpdateExpiry(ctx, tableName, expiry)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to update table on bigquery for proj.dataset.table1")
		})
		t.Run("updates the expiry successfully", func(t *testing.T) {
			meta := &bq.TableMetadata{
				Description: "test update",
			}
			table := new(mockBigQueryTable)
			table.On("Update", ctx, mock.Anything, "", emptyUpdateOptions).Return(meta, nil)
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			tableName := "proj.dataset.table1"
			expiry := time.Now().AddDate(0, 3, 0)

			err := tHandle.UpdateExpiry(ctx, tableName, expiry)
			assert.Nil(t, err)
		})
	})
	t.Run("Exists", func(t *testing.T) {
		t.Run("returns false when error in getting metadata", func(t *testing.T) {
			table := new(mockBigQueryTable)
			table.On("Metadata", ctx, mock.Anything).Return(nil, errors.New("error in get"))
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			exists := tHandle.Exists(ctx)
			assert.False(t, exists)
		})
		t.Run("returns true when gets metadata", func(t *testing.T) {
			meta := &bq.TableMetadata{
				Description: "test update",
			}
			table := new(mockBigQueryTable)
			table.On("Metadata", ctx, mock.Anything).Return(meta, nil)
			defer table.AssertExpectations(t)

			tHandle := bigquery.NewTableHandle(table)

			exists := tHandle.Exists(ctx)
			assert.True(t, exists)
		})
	})
}

type mockBigQueryTable struct {
	mock.Mock
}

func (m *mockBigQueryTable) Create(ctx context.Context, metadata *bq.TableMetadata) error {
	args := m.Called(ctx, metadata)
	return args.Error(0)
}

func (m *mockBigQueryTable) Update(ctx context.Context, update bq.TableMetadataToUpdate, etag string, option ...bq.TableUpdateOption) (*bq.TableMetadata, error) {
	args := m.Called(ctx, update, etag, option)
	var tm *bq.TableMetadata
	if args.Get(0) != nil {
		tm = args.Get(0).(*bq.TableMetadata)
	}
	return tm, args.Error(1)
}

func (m *mockBigQueryTable) Metadata(ctx context.Context, opts ...bq.TableMetadataOption) (*bq.TableMetadata, error) {
	args := m.Called(ctx, opts)
	var tm *bq.TableMetadata
	if args.Get(0) != nil {
		tm = args.Get(0).(*bq.TableMetadata)
	}
	return tm, args.Error(1)
}

func (m *mockBigQueryTable) CopierFrom(srcs ...*bq.Table) *bq.Copier {
	args := m.Called(srcs)
	return args.Get(0).(*bq.Copier)
}
