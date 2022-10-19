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

func TestExternalTableHandle(t *testing.T) {
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
			et := new(mockBigQueryTable)
			etHandle := bigquery.NewExternalTableHandle(et)

			spec := map[string]any{"description": []string{"a", "b"}}
			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: 1 error(s) decoding:\n\n* "+
				"'description' expected type 'string', got unconvertible type '[]string', value: '[a b]': not able to "+
				"decode spec for proj.dataset.extTable1")
		})
		t.Run("returns error when cannot get metadata", func(t *testing.T) {
			et := new(mockBigQueryTable)
			etHandle := bigquery.NewExternalTableHandle(et)

			spec := map[string]any{
				"description":     "test create",
				"expiration_time": "invalid_date",
			}
			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_external_table: failed to get "+
				"metadata to create for proj.dataset.extTable1")
		})
		t.Run("returns error when external table already present on bigquery", func(t *testing.T) {
			bqErr := &googleapi.Error{Code: 409, Message: "Already Exists project.dataset.extTable1"}
			et := new(mockBigQueryTable)
			et.On("Create", ctx, mock.Anything).Return(bqErr)
			defer et.AssertExpectations(t)

			etHandle := bigquery.NewExternalTableHandle(et)

			spec := map[string]any{"description": "test create"}
			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "resource already exists for entity resource_external_table: external "+
				"table already exists on bigquery: proj.dataset.extTable1")
		})
		t.Run("returns error when external table type is wrong", func(t *testing.T) {
			spec := map[string]any{
				"description": "test create",
				"schema": []map[string]any{
					{
						"name": "product_name",
						"type": "STRING",
					},
				},
				"source": map[string]any{
					"type": "avro",
					"uris": []string{"https://docs.google.com/sheet"},
				},
			}

			et := new(mockBigQueryTable)
			etHandle := bigquery.NewExternalTableHandle(et)

			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_external_table: source format not yet implemented avro")
		})
		t.Run("returns error when bigquery returns error", func(t *testing.T) {
			et := new(mockBigQueryTable)
			et.On("Create", ctx, mock.Anything).Return(errors.New("some error"))
			defer et.AssertExpectations(t)

			etHandle := bigquery.NewExternalTableHandle(et)

			spec := map[string]any{"description": "test create"}
			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Create(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource_external_table: failed to create "+
				"external table proj.dataset.extTable1")
		})
		t.Run("successfully creates the resource", func(t *testing.T) {
			spec := map[string]any{
				"description": "test create",
				"schema": []map[string]any{
					{
						"name": "product",
						"type": "RECORD",
						"schema": []map[string]any{
							{
								"name": "product_name",
								"type": "STRING",
							},
							{
								"name": "product_id",
								"type": "INTEGER",
							},
						},
					},
				},
				"source": map[string]any{
					"type": "google_sheets",
					"uris": []string{"https://docs.google.com/sheet"},
					"config": map[string]any{
						"range":             "kyc",
						"skip_leading_rows": 2,
					},
				},
			}
			argMatcher := mock.MatchedBy(func(req *bq.TableMetadata) bool {
				return req.Description == "test create" &&
					len(req.Schema) == 1 &&
					req.Schema[0].Name == "product" &&
					req.Schema[0].Type == "RECORD" &&
					len(req.Schema[0].Schema) == 2 &&
					string(req.ExternalDataConfig.SourceFormat) == "GOOGLE_SHEETS" &&
					len(req.ExternalDataConfig.SourceURIs) == 1 &&
					req.ExternalDataConfig.Options != nil
			})

			et := new(mockBigQueryTable)
			et.On("Create", ctx, argMatcher).Return(nil)
			defer et.AssertExpectations(t)

			etHandle := bigquery.NewExternalTableHandle(et)

			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Create(ctx, res)
			assert.Nil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("returns error when cannot convert spec", func(t *testing.T) {
			et := new(mockBigQueryTable)
			etHandle := bigquery.NewExternalTableHandle(et)

			spec := map[string]any{"description": []string{"a", "b"}}
			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource: 1 error(s) decoding:\n\n* "+
				"'description' expected type 'string', got unconvertible type '[]string', value: '[a b]': not able to "+
				"decode spec for proj.dataset.extTable1")
		})
		t.Run("returns error when cannot get metadata", func(t *testing.T) {
			et := new(mockBigQueryTable)
			etHandle := bigquery.NewExternalTableHandle(et)

			spec := map[string]any{
				"description":     "test update",
				"expiration_time": "invalid_date",
			}
			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_external_table: failed to get "+
				"metadata to update for proj.dataset.extTable1")
		})
		t.Run("returns error when external table not present on bigquery", func(t *testing.T) {
			bqErr := &googleapi.Error{Code: 404}
			et := new(mockBigQueryTable)
			et.On("Update", ctx, mock.Anything, "").Return(nil, bqErr)
			defer et.AssertExpectations(t)

			etHandle := bigquery.NewExternalTableHandle(et)

			spec := map[string]any{"description": "test update"}
			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found for entity resource_external_table: failed to update "+
				"external_table in bigquery for proj.dataset.extTable1")
		})
		t.Run("returns error when bigquery returns error", func(t *testing.T) {
			et := new(mockBigQueryTable)
			et.On("Update", ctx, mock.Anything, "").Return(nil, errors.New("some error"))
			defer et.AssertExpectations(t)

			etHandle := bigquery.NewExternalTableHandle(et)

			spec := map[string]any{"description": "test update"}
			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Update(ctx, res)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity resource_external_table: failed to update "+
				"external_table on bigquery for proj.dataset.extTable1")
		})
		t.Run("successfully updates the resource", func(t *testing.T) {
			meta := &bq.TableMetadata{
				Description: "test update",
			}
			et := new(mockBigQueryTable)
			et.On("Update", ctx, mock.Anything, "").Return(meta, nil)
			defer et.AssertExpectations(t)

			etHandle := bigquery.NewExternalTableHandle(et)

			spec := map[string]any{"description": "test update"}
			res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			err = etHandle.Update(ctx, res)
			assert.Nil(t, err)
		})
	})
	t.Run("Exists", func(t *testing.T) {
		t.Run("returns false when error in getting metadata", func(t *testing.T) {
			extTable := new(mockBigQueryTable)
			extTable.On("Metadata", ctx).Return(nil, errors.New("error in get"))
			defer extTable.AssertExpectations(t)

			etHandle := bigquery.NewExternalTableHandle(extTable)

			exists := etHandle.Exists(ctx)
			assert.False(t, exists)
		})
		t.Run("returns true when gets metadata", func(t *testing.T) {
			meta := &bq.TableMetadata{
				Description: "test update",
			}
			extTable := new(mockBigQueryTable)
			extTable.On("Metadata", ctx).Return(meta, nil)
			defer extTable.AssertExpectations(t)

			etHandle := bigquery.NewExternalTableHandle(extTable)

			exists := etHandle.Exists(ctx)
			assert.True(t, exists)
		})
	})
}
