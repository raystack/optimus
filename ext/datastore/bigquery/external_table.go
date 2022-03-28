package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	bqapi "cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"google.golang.org/api/googleapi"

	"github.com/odpf/optimus/models"
)

func createExternalTable(ctx context.Context, spec models.ResourceSpec, client bqiface.Client, upsert bool) error {
	bqResource, ok := spec.Spec.(BQTable)

	if !ok {
		return errors.New("failed to read table spec for bigquery")
	}

	// inherit from base
	bqResource.Metadata.Labels = spec.Labels

	dataset := client.DatasetInProject(bqResource.Project, bqResource.Dataset)
	if err := ensureDataset(ctx, dataset, BQDataset{
		Project:  bqResource.Project,
		Dataset:  bqResource.Dataset,
		Metadata: BQDatasetMetadata{},
	}, false); err != nil {
		return err
	}
	table := dataset.Table(bqResource.Table)
	return ensureExternalTable(ctx, table, bqResource, upsert)
}

func ensureExternalTable(ctx context.Context, tableHandle bqiface.Table, t BQTable, upsert bool) error {
	meta, err := tableHandle.Metadata(ctx)
	if err != nil {
		var metaErr *googleapi.Error
		if !errors.As(err, &metaErr) || metaErr.Code != http.StatusNotFound {
			return err
		}
		meta, err := bqCreateTableMetaAdapter(t)
		if err != nil {
			return err
		}

		if t.Metadata.ExpirationTime != "" {
			expiryTime, err := time.Parse(time.RFC3339, t.Metadata.ExpirationTime)
			if err != nil {
				return fmt.Errorf("unable to parse timestamp %s: %w", t.Metadata.ExpirationTime, err)
			}
			meta.ExpirationTime = expiryTime
		}
		return tableHandle.Create(ctx, meta)
	}
	if !upsert {
		return nil
	}

	// update if already exists
	m := bqapi.TableMetadataToUpdate{
		Description: t.Metadata.Description,
	}
	if t.Metadata.ExpirationTime != "" {
		expiryTime, err := time.Parse(time.RFC3339, t.Metadata.ExpirationTime)
		if err != nil {
			return fmt.Errorf("unable to parse timestamp %s: %w", t.Metadata.ExpirationTime, err)
		}
		m.ExpirationTime = expiryTime
	}
	for k, v := range t.Metadata.Labels {
		m.SetLabel(k, v)
	}
	if _, err := tableHandle.Update(ctx, m, meta.ETag); err != nil {
		return err
	}
	return nil
}
