package bigquery

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	backupTimePostfixFormat = "2006_01_02_15_04_05"

	configDataset        = "dataset"
	defaultBackupDataset = "optimus_backup"

	configPrefix        = "prefix"
	defaultBackupPrefix = "backup"

	configTTL        = "ttl"
	defaultBackupTTL = "720h"
)

func BackupResources(ctx context.Context, backup *resource.Backup, resources []*resource.Resource, client Client) (*resource.BackupResult, error) {
	var ignored []resource.IgnoredResource

	var tablesToBackup []*resource.Resource
	for _, r := range resources {
		if r.Kind() != resource.KindTable {
			ignored = append(ignored, resource.IgnoredResource{
				Name:   r.FullName(),
				Reason: "backup not supported for " + r.Kind().String(),
			})
			continue
		}

		tablesToBackup = append(tablesToBackup, r)
	}

	var backupNames []string
	if len(tablesToBackup) > 0 {
		destinationDataset, err := DestinationDataset(tablesToBackup[0].Dataset(), backup)
		if err != nil {
			return nil, err
		}
		err = CreateIfDatasetDoesNotExist(ctx, client, destinationDataset)
		if err != nil {
			return nil, err
		}
	}
	for _, r := range tablesToBackup {
		tableName, err := BackupTable(ctx, backup, r, client)
		if err != nil {
			return nil, err
		}
		backupNames = append(backupNames, tableName)
	}

	return &resource.BackupResult{
		ResourceNames:    backupNames,
		IgnoredResources: ignored,
	}, nil
}

func CreateIfDatasetDoesNotExist(ctx context.Context, client Client, dataset resource.Dataset) error {
	datasetHandle := client.DatasetHandleFrom(dataset)
	if datasetHandle.Exists(ctx) {
		return nil
	}

	backupMetadata := &resource.Metadata{
		Description: "backup dataset created by optimus",
		Labels:      map[string]string{"created_by": "optimus"},
	}
	spec := map[string]any{"description": backupMetadata.Description}
	r, err := resource.NewResource(dataset.FullName(), resource.KindDataset, resource.Bigquery, tenant.Tenant{}, backupMetadata, spec)
	if err != nil {
		return err
	}

	err = datasetHandle.Create(ctx, r)
	if err != nil && !errors.IsErrorType(err, errors.ErrAlreadyExists) {
		return err
	}
	return nil
}

func BackupTable(ctx context.Context, backup *resource.Backup, source *resource.Resource, client Client) (string, error) {
	datasetDST, err := DestinationDataset(source.Dataset(), backup)
	if err != nil {
		return "", err
	}

	nameDST, err := DestinationName(source, backup)
	if err != nil {
		return "", err
	}

	backupExpiry, err := DestinationExpiry(backup)
	if err != nil {
		return "", err
	}

	sourceHandle := client.TableHandleFrom(source.Dataset(), source.Name())
	destinationHandle := client.TableHandleFrom(datasetDST, nameDST)

	err = CopyTable(ctx, sourceHandle, destinationHandle)
	if err != nil {
		return "", err
	}

	destinationFullName := datasetDST.FullName() + "." + nameDST.String()
	err = destinationHandle.UpdateExpiry(ctx, destinationFullName, backupExpiry)
	if err != nil {
		return "", err
	}

	return destinationFullName, nil
}

func CopyTable(ctx context.Context, source, destination TableResourceHandle) error {
	copier, err := source.GetCopier(destination)
	if err != nil {
		return err
	}

	copyJob, err := copier.Run(ctx)
	if err != nil {
		return err
	}

	return copyJob.Wait(ctx)
}

func DestinationDataset(sourceDataset resource.Dataset, backup *resource.Backup) (resource.Dataset, error) {
	datasetName := backup.GetConfigOrDefaultFor(configDataset, defaultBackupDataset)

	return resource.DataSetFrom(sourceDataset.Store, sourceDataset.Database, datasetName)
}

func DestinationName(source *resource.Resource, backup *resource.Backup) (resource.Name, error) {
	// TODO: After getting resources check status, reject if not exist or create_failure
	prefixValue := backup.GetConfigOrDefaultFor(configPrefix, defaultBackupPrefix)

	backupTime := backup.CreatedAt()
	nameStr := fmt.Sprintf("%s_%s_%s_%s", prefixValue, source.Dataset().Schema, source.Name().String(),
		backupTime.Format(backupTimePostfixFormat))

	return resource.NameFrom(nameStr)
}

func DestinationExpiry(backup *resource.Backup) (time.Time, error) {
	ttl := backup.GetConfigOrDefaultFor(configTTL, defaultBackupTTL)

	ttlDuration, err := time.ParseDuration(ttl)
	if err != nil {
		return time.Time{}, errors.InvalidArgument(store, "failed to parse bigquery backup TTL "+ttl)
	}

	backupTime := backup.CreatedAt()
	return backupTime.Add(ttlDuration), nil
}
