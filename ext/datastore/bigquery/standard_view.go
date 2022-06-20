package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	bqapi "cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"google.golang.org/api/googleapi"

	"github.com/odpf/optimus/models"
)

func createStandardView(ctx context.Context, spec models.ResourceSpec, client bqiface.Client, upsert bool) error {
	spanCtx, span := startChildSpan(ctx, "CreateStandardView")
	defer span.End()

	bqResource, ok := spec.Spec.(BQTable)
	if !ok {
		return errors.New("failed to read table spec for bigquery")
	}

	// view query could be in an external asset
	if query, ok := spec.Assets.GetByName(ViewQueryFile); ok &&
		strings.TrimSpace(bqResource.Metadata.ViewQuery) == "" {
		bqResource.Metadata.ViewQuery = query
	}

	// inherit from base
	bqResource.Metadata.Labels = spec.Labels

	dataset := client.DatasetInProject(bqResource.Project, bqResource.Dataset)
	if err := ensureDataset(spanCtx, dataset, BQDataset{
		Project:  bqResource.Project,
		Dataset:  bqResource.Dataset,
		Metadata: BQDatasetMetadata{},
	}, false); err != nil {
		return err
	}
	table := dataset.Table(bqResource.Table)
	return ensureStandardView(spanCtx, table, bqResource, upsert)
}

func ensureStandardView(ctx context.Context, tableHandle bqiface.Table, t BQTable, upsert bool) error {
	spanCtx, span := startChildSpan(ctx, "EnsureStandardView")
	defer span.End()

	meta, err := tableHandle.Metadata(spanCtx)
	if err != nil {
		var metaErr *googleapi.Error
		if !errors.As(err, &metaErr) || metaErr.Code != http.StatusNotFound {
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
				return fmt.Errorf("unable to parse timestamp %s: %w", t.Metadata.ExpirationTime, err)
			}
			meta.ExpirationTime = expiryTime
		}
		return tableHandle.Create(spanCtx, meta)
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
			return fmt.Errorf("unable to parse timestamp %s: %w", t.Metadata.ExpirationTime, err)
		}
		m.ExpirationTime = expiryTime
	}
	for k, v := range t.Metadata.Labels {
		m.SetLabel(k, v)
	}
	if _, err := tableHandle.Update(spanCtx, m, meta.ETag); err != nil {
		return err
	}
	return nil
}
