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
	"github.com/odpf/optimus/models"
	"google.golang.org/api/googleapi"
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

func createTable(ctx context.Context, spec models.ResourceSpec, client bqiface.Client, upsert bool) error {
	bqResource, ok := spec.Spec.(BQTable)
	if !ok {
		return errors.New(errorReadTableSpec)
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
	return ensureTable(ctx, table, bqResource, upsert)
}

// ensureTable make sures table exists with provided config and update if required
func ensureTable(ctx context.Context, tableHandle bqiface.Table, t BQTable, upsert bool) error {
	meta, err := tableHandle.Metadata(ctx)
	if err != nil {
		if metaErr, ok := err.(*googleapi.Error); !ok || metaErr.Code != http.StatusNotFound {
			return err
		}
		m, err := bqCreateTableMetaAdapter(t)
		if err != nil {
			return err
		}
		return tableHandle.Create(ctx, m)
	}
	if !upsert {
		return nil
	}

	// update if already exists
	m, err := bqUpdateTableMetaAdapter(t)
	if err != nil {
		return err
	}
	_, err = tableHandle.Update(ctx, m, meta.ETag)
	return err
}

// getTable retrieves bq table information
func getTable(ctx context.Context, resourceSpec models.ResourceSpec, client bqiface.Client) (models.ResourceSpec, error) {
	var bqResource BQTable
	bqResource, ok := resourceSpec.Spec.(BQTable)
	if !ok {
		return models.ResourceSpec{}, errors.New(errorReadTableSpec)
	}

	dataset := client.DatasetInProject(bqResource.Project, bqResource.Dataset)
	if _, err := dataset.Metadata(ctx); err != nil {
		return models.ResourceSpec{}, err
	}

	table := dataset.Table(bqResource.Table)
	tableMeta, err := table.Metadata(ctx)
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
	bqTable, ok := resourceSpec.Spec.(BQTable)
	if !ok {
		return errors.New(errorReadTableSpec)
	}
	dataset := client.DatasetInProject(bqTable.Project, bqTable.Dataset)
	if _, err := dataset.Metadata(ctx); err != nil {
		return err
	}

	table := dataset.Table(bqTable.Table)
	return table.Delete(ctx)
}

func backupTable(ctx context.Context, request models.BackupResourceRequest, client bqiface.Client) (models.BackupResourceResponse, error) {
	bqResourceSrc, ok := request.Resource.Spec.(BQTable)
	if !ok {
		return models.BackupResourceResponse{}, errors.New(errorReadTableSpec)
	}

	bqResourceDst := prepareBQResourceDst(bqResourceSrc, request.BackupSpec, request.BackupTime)

	tableDst, err := duplicateTable(ctx, client, bqResourceSrc, bqResourceDst)
	if err != nil {
		return models.BackupResourceResponse{}, err
	}

	tableDst, err = updateExpiry(ctx, tableDst, request)
	if err != nil {
		return models.BackupResourceResponse{}, err
	}

	if err := ensureTable(ctx, tableDst, bqResourceDst, false); err != nil {
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
	// make sure dataset is present
	datasetDst := client.DatasetInProject(bqResourceDst.Project, bqResourceDst.Dataset)
	if err := ensureDataset(ctx, datasetDst, BQDataset{
		Project:  bqResourceSrc.Project,
		Dataset:  bqResourceSrc.Dataset,
		Metadata: BQDatasetMetadata{},
	}, false); err != nil {
		return nil, err
	}

	datasetSrc := client.DatasetInProject(bqResourceSrc.Project, bqResourceSrc.Dataset)
	if _, err := datasetSrc.Metadata(ctx); err != nil {
		return nil, err
	}

	// duplicate table
	tableSrc := datasetSrc.Table(bqResourceSrc.Table)
	tableDst := datasetDst.Table(bqResourceDst.Table)

	copier := tableDst.CopierFrom(tableSrc)
	job, err := copier.Run(ctx)
	if err != nil {
		return nil, err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := status.Err(); err != nil {
		return nil, err
	}
	return tableDst, nil
}

func updateExpiry(ctx context.Context, tableDst bqiface.Table, req models.BackupResourceRequest) (bqiface.Table, error) {
	meta, err := tableDst.Metadata(ctx)
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
	if _, err = tableDst.Update(ctx, update, meta.ETag); err != nil {
		return nil, err
	}
	return tableDst, nil
}
