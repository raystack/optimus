package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"google.golang.org/api/googleapi"

	"github.com/odpf/optimus/models"
)

var tableNameParseRegex = regexp.MustCompile(`^([\w-]+)\.(\w+)\.([\w-]+)$`)

const (
	errorReadTableSpec      = "failed to read table spec for bigquery"
	backupTimePostfixFormat = "2006_01_02_15_04_05"
)

const (
	// bigquery datastore specific configurations
	BackupConfigDataset = "dataset"
	BackupConfigPrefix  = "prefix"

	defaultBackupDataset = "optimus_backup"
	defaultBackupPrefix  = "backup"
	defaultBackupTTL     = "720h"
)

func createTable(ctxA context.Context, spec models.ResourceSpec, client bqiface.Client, upsert bool) error {
	spanCtx, span := startChildSpan(ctxA, "CreateTable")
	defer span.End()

	bqResource, ok := spec.Spec.(BQTable)
	if !ok {
		return errors.New(errorReadTableSpec)
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
	return ensureTable(spanCtx, table, bqResource, upsert)
}

// ensureTable make sures table exists with provided config and update if required
func ensureTable(ctx context.Context, tableHandle bqiface.Table, t BQTable, upsert bool) error {
	spanCtx, span := startChildSpan(ctx, "EnsureTable")
	defer span.End()

	meta, err := tableHandle.Metadata(spanCtx)
	if err != nil {
		var metaErr *googleapi.Error
		if !errors.As(err, &metaErr) || metaErr.Code != http.StatusNotFound {
			return err
		}
		m, err := bqCreateTableMetaAdapter(t)
		if err != nil {
			return err
		}
		return tableHandle.Create(spanCtx, m)
	}
	if !upsert {
		return nil
	}

	// update if already exists
	m, err := bqUpdateTableMetaAdapter(t)
	if err != nil {
		return err
	}
	_, err = tableHandle.Update(spanCtx, m, meta.ETag)
	return err
}

// getTable retrieves bq table information
func getTable(ctx context.Context, resourceSpec models.ResourceSpec, client bqiface.Client) (models.ResourceSpec, error) {
	spanCtx, span := startChildSpan(ctx, "GetTable")
	defer span.End()

	var bqResource BQTable
	bqResource, ok := resourceSpec.Spec.(BQTable)
	if !ok {
		return models.ResourceSpec{}, errors.New(errorReadTableSpec)
	}

	dataset := client.DatasetInProject(bqResource.Project, bqResource.Dataset)
	if _, err := dataset.Metadata(spanCtx); err != nil {
		return models.ResourceSpec{}, err
	}

	table := dataset.Table(bqResource.Table)
	tableMeta, err := table.Metadata(spanCtx)
	if err != nil {
		return models.ResourceSpec{}, err
	}

	// generate schema
	tableSchema, err := bqSchemaFrom(tableMeta.Schema)
	if err != nil {
		return models.ResourceSpec{}, err
	}

	// update metadata
	bqResource.Metadata = BQTableMetadata{
		Description: tableMeta.Description,
		Labels:      tableMeta.Labels,
		Schema:      tableSchema,
		Cluster:     bqClusteringFrom(tableMeta.Clustering),
		ViewQuery:   tableMeta.ViewQuery,
		Location:    tableMeta.Location,
	}

	// if table is partitioned
	if tableMeta.TimePartitioning != nil {
		bqResource.Metadata.Partition = bqPartitioningFrom(tableMeta.TimePartitioning)
	} else if tableMeta.RangePartitioning != nil {
		bqResource.Metadata.Partition = &BQPartitionInfo{
			Field: tableMeta.RangePartitioning.Field,
			Range: bqPartitioningRangeFrom(tableMeta.RangePartitioning.Range),
		}
	}

	resourceSpec.Spec = bqResource
	return resourceSpec, nil
}

func deleteTable(ctx context.Context, resourceSpec models.ResourceSpec, client bqiface.Client) error {
	spanCtx, span := startChildSpan(ctx, "DeleteTable")
	defer span.End()

	bqTable, ok := resourceSpec.Spec.(BQTable)
	if !ok {
		return errors.New(errorReadTableSpec)
	}
	dataset := client.DatasetInProject(bqTable.Project, bqTable.Dataset)
	if _, err := dataset.Metadata(spanCtx); err != nil {
		return err
	}

	table := dataset.Table(bqTable.Table)
	return table.Delete(spanCtx)
}

func backupTable(ctx context.Context, request models.BackupResourceRequest, client bqiface.Client) (models.BackupResourceResponse, error) {
	spanCtx, span := startChildSpan(ctx, "BackupTable")
	defer span.End()

	bqResourceSrc, ok := request.Resource.Spec.(BQTable)
	if !ok {
		return models.BackupResourceResponse{}, errors.New(errorReadTableSpec)
	}

	bqResourceDst := prepareBQResourceDst(bqResourceSrc, request.BackupSpec, request.BackupTime)

	tableDst, err := duplicateTable(spanCtx, client, bqResourceSrc, bqResourceDst)
	if err != nil {
		return models.BackupResourceResponse{}, err
	}

	tableDst, err = updateExpiry(spanCtx, tableDst, request)
	if err != nil {
		return models.BackupResourceResponse{}, err
	}

	if err := ensureTable(spanCtx, tableDst, bqResourceDst, false); err != nil {
		return models.BackupResourceResponse{}, err
	}

	resultURN, err := tableSpec{}.GenerateURN(bqResourceDst)
	if err != nil {
		return models.BackupResourceResponse{}, err
	}

	return models.BackupResourceResponse{
		ResultURN:  resultURN,
		ResultSpec: bqResourceDst,
	}, nil
}

func prepareBQResourceDst(bqResourceSrc BQTable, backupSpec models.BackupRequest, backupTime time.Time) BQTable {
	datasetValue, ok := backupSpec.Config[BackupConfigDataset]
	if !ok {
		datasetValue = defaultBackupDataset
		backupSpec.Config[BackupConfigDataset] = defaultBackupDataset
	}

	prefixValue, ok := backupSpec.Config[BackupConfigPrefix]
	if !ok {
		prefixValue = defaultBackupPrefix
		backupSpec.Config[BackupConfigPrefix] = defaultBackupPrefix
	}

	return BQTable{
		Project: bqResourceSrc.Project,
		Dataset: datasetValue,
		Table: fmt.Sprintf("%s_%s_%s_%s", prefixValue, bqResourceSrc.Dataset, bqResourceSrc.Table,
			backupTime.Format(backupTimePostfixFormat)),
	}
}

func duplicateTable(ctx context.Context, client bqiface.Client, bqResourceSrc, bqResourceDst BQTable) (bqiface.Table, error) {
	spanCtx, span := startChildSpan(ctx, "DuplicateTable")
	defer span.End()

	// make sure dataset is present
	datasetDst := client.DatasetInProject(bqResourceDst.Project, bqResourceDst.Dataset)
	if err := ensureDataset(spanCtx, datasetDst, BQDataset{
		Project:  bqResourceSrc.Project,
		Dataset:  bqResourceSrc.Dataset,
		Metadata: BQDatasetMetadata{},
	}, false); err != nil {
		return nil, err
	}

	datasetSrc := client.DatasetInProject(bqResourceSrc.Project, bqResourceSrc.Dataset)
	if _, err := datasetSrc.Metadata(spanCtx); err != nil {
		return nil, err
	}

	// duplicate table
	tableSrc := datasetSrc.Table(bqResourceSrc.Table)
	tableDst := datasetDst.Table(bqResourceDst.Table)

	copier := tableDst.CopierFrom(tableSrc)
	job, err := copier.Run(spanCtx)
	if err != nil {
		return nil, err
	}
	status, err := job.Wait(spanCtx)
	if err != nil {
		return nil, err
	}
	if err := status.Err(); err != nil {
		return nil, err
	}
	return tableDst, nil
}

func updateExpiry(ctx context.Context, tableDst bqiface.Table, req models.BackupResourceRequest) (bqiface.Table, error) {
	spanCtx, span := startChildSpan(ctx, "UpdateExpiry")
	defer span.End()

	meta, err := tableDst.Metadata(spanCtx)
	if err != nil {
		return nil, err
	}

	ttl, ok := req.BackupSpec.Config[models.ConfigTTL]
	if !ok {
		ttl = defaultBackupTTL
		req.BackupSpec.Config[models.ConfigTTL] = defaultBackupTTL
	}

	ttlDuration, err := time.ParseDuration(ttl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bigquery backup TTL %s: %w", ttl, err)
	}

	update := bigquery.TableMetadataToUpdate{
		ExpirationTime: req.BackupTime.Add(ttlDuration),
	}
	if _, err = tableDst.Update(spanCtx, update, meta.ETag); err != nil {
		return nil, err
	}
	return tableDst, nil
}
