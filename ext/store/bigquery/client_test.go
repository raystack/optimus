package bigquery_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/store/bigquery"
)

func TestBqClient(t *testing.T) {
	ctx := context.Background()
	bq := resource.BigQuery
	tnnt, _ := tenant.NewTenant("proj", "ns")
	metadata := resource.Metadata{Description: "meta"}
	spec := map[string]any{"description": "resource"}
	testCredJson := `
{
  "type": "service_account",
  "project_id": "test-bigquery",
  "private_key_id": "4192b",
  "private_key": "-----BEGIN PRIVATE KEY-----\njLpyglDekLC\n-----END PRIVATE KEY-----\n",
  "client_email": "test-service-account@test-bigquery.iam.gserviceaccount.com",
  "client_id": "1234567890",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-service-account%40test-bigquery.iam.gserviceaccount.com"
}
`

	t.Run("newClient", func(t *testing.T) {
		t.Run("returns error when invalid creds", func(t *testing.T) {
			_, err := bigquery.NewClient(ctx, "")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "internal error for entity BigqueryStore: failed to read account")
		})
		t.Run("returns client on creds", func(t *testing.T) {
			client, err := bigquery.NewClient(ctx, testCredJson)
			assert.Nil(t, err)

			assert.NotNil(t, client)
		})
	})
	t.Run("DatasetHandleFrom", func(t *testing.T) {
		t.Run("returns the dataset handle", func(t *testing.T) {
			c, err := bigquery.NewClient(ctx, testCredJson)
			assert.Nil(t, err)

			fullName := "project.dataset"
			dataset, err := resource.NewResource(fullName, resource.KindDataset, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			handle := c.DatasetHandleFrom(dataset.Dataset())
			assert.NotNil(t, handle)
		})
	})
	t.Run("TableHandleFrom", func(t *testing.T) {
		t.Run("returns the table handle", func(t *testing.T) {
			c, err := bigquery.NewClient(ctx, testCredJson)
			assert.Nil(t, err)

			fullName := "project.dataset.table"
			table, err := resource.NewResource(fullName, resource.KindTable, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			handle := c.TableHandleFrom(table.Dataset(), table.Name())
			assert.NotNil(t, handle)
		})
	})
	t.Run("ExternalTableHandleFrom", func(t *testing.T) {
		t.Run("returns the external_table handle", func(t *testing.T) {
			c, err := bigquery.NewClient(ctx, testCredJson)
			assert.Nil(t, err)

			fullName := "project.dataset.external_table"
			extTable, err := resource.NewResource(fullName, resource.KindView, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			handle := c.ExternalTableHandleFrom(extTable)
			assert.NotNil(t, handle)
		})
	})
	t.Run("ViewHandleFrom", func(t *testing.T) {
		t.Run("returns the view handle", func(t *testing.T) {
			c, err := bigquery.NewClient(ctx, testCredJson)
			assert.Nil(t, err)

			fullName := "project.dataset.view"
			view, err := resource.NewResource(fullName, resource.KindView, bq, tnnt, &metadata, spec)
			assert.Nil(t, err)

			handle := c.ViewHandleFrom(view)
			assert.NotNil(t, handle)
		})
	})
	t.Run("Close", func(t *testing.T) {
		t.Run("calls close on bq client", func(t *testing.T) {
			c, err := bigquery.NewClient(ctx, testCredJson)
			assert.Nil(t, err)

			c.Close()
		})
	})
}
