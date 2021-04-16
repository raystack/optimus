package bigquery

import (
	"context"
	"net/http"
	"strings"
	"time"

	bqapi "cloud.google.com/go/bigquery"

	"google.golang.org/api/googleapi"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

func createStandardView(ctx context.Context, spec models.ResourceSpec, client bqiface.Client, upsert bool) error {
	bqResource, ok := spec.Spec.(BQTable)
	if !ok {
		return errors.New("failed to read table spec for bigquery")
	}

	// view query could be in an external asset
	if query, ok := spec.Assets.GetByName(ViewQueryFile); ok &&
		len(strings.TrimSpace(bqResource.Metadata.ViewQuery)) == 0 {
		bqResource.Metadata.ViewQuery = string(query)
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
	return ensureStandardView(ctx, table, bqResource, upsert)
}

func ensureStandardView(ctx context.Context, tableHandle bqiface.Table, t BQTable, upsert bool) error {
	meta, err := tableHandle.Metadata(ctx)
	if err != nil {
		if metaErr, ok := err.(*googleapi.Error); !ok || metaErr.Code != http.StatusNotFound {
			return err
		}
		meta := &bqapi.TableMetadata{
			ViewQuery:   t.Metadata.ViewQuery,
			Labels:      t.Metadata.Labels,
			Description: t.Metadata.Description,
		}
		if t.Metadata.ExpirationTime != "" {
			expiryTime, err := time.Parse(time.RFC3339, t.Metadata.ExpirationTime)
			if err != nil {
				return errors.Wrapf(err, "unable to parse timestamp %s", t.Metadata.ExpirationTime)
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
		ViewQuery:   t.Metadata.ViewQuery,
	}
	if t.Metadata.ExpirationTime != "" {
		expiryTime, err := time.Parse(time.RFC3339, t.Metadata.ExpirationTime)
		if err != nil {
			return errors.Wrapf(err, "unable to parse timestamp %s", t.Metadata.ExpirationTime)
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
