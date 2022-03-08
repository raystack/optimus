package bigquery

import (
	"context"
	"errors"
	"fmt"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/odpf/optimus/models"
)

const (
	// Required secret
	SecretName = "DATASTORE_BIGQUERY"
)

var (
	This = &BigQuery{
		ClientFac: &defaultBQClientFactory{},
	}

	errSecretNotFoundStr = "secret %s required to migrate datastore not found for %s"
)

type ClientFactory interface {
	New(ctx context.Context, svcAccount string) (bqiface.Client, error)
}

type BigQuery struct {
	ClientFac ClientFactory
}

func (b BigQuery) Name() string {
	return "bigquery"
}

func (b BigQuery) Description() string {
	return "GCP BigQuery"
}

func (b BigQuery) Types() map[models.ResourceType]models.DatastoreTypeController {
	return map[models.ResourceType]models.DatastoreTypeController{
		models.ResourceTypeTable:         &tableSpec{},
		models.ResourceTypeView:          &standardViewSpec{},
		models.ResourceTypeDataset:       &datasetSpec{},
		models.ResourceTypeExternalTable: &externalTableSpec{},
	}
}

func (b *BigQuery) CreateResource(ctx context.Context, request models.CreateResourceRequest) error {
	svcAcc, ok := request.Project.Secret.GetByName(SecretName)
	if !ok || len(svcAcc) == 0 {
		return errors.New(fmt.Sprintf(errSecretNotFoundStr, SecretName, b.Name()))
	}

	client, err := b.ClientFac.New(ctx, svcAcc)
	if err != nil {
		return err
	}

	switch request.Resource.Type {
	case models.ResourceTypeTable:
		return createTable(ctx, request.Resource, client, false)
	case models.ResourceTypeView:
		return createStandardView(ctx, request.Resource, client, false)
	case models.ResourceTypeDataset:
		return createDataset(ctx, request.Resource, client, false)
	case models.ResourceTypeExternalTable:
		return createExternalTable(ctx, request.Resource, client, false)
	}
	return fmt.Errorf("unsupported resource type %s", request.Resource.Type)
}

func (b *BigQuery) UpdateResource(ctx context.Context, request models.UpdateResourceRequest) error {
	svcAcc, ok := request.Project.Secret.GetByName(SecretName)
	if !ok || len(svcAcc) == 0 {
		return errors.New(fmt.Sprintf(errSecretNotFoundStr, SecretName, b.Name()))
	}

	client, err := b.ClientFac.New(ctx, svcAcc)
	if err != nil {
		return err
	}

	switch request.Resource.Type {
	case models.ResourceTypeTable:
		return createTable(ctx, request.Resource, client, true)
	case models.ResourceTypeView:
		return createStandardView(ctx, request.Resource, client, true)
	case models.ResourceTypeDataset:
		return createDataset(ctx, request.Resource, client, true)
	case models.ResourceTypeExternalTable:
		return createExternalTable(ctx, request.Resource, client, true)
	}
	return fmt.Errorf("unsupported resource type %s", request.Resource.Type)
}

func (b *BigQuery) ReadResource(ctx context.Context, request models.ReadResourceRequest) (models.ReadResourceResponse, error) {
	svcAcc, ok := request.Project.Secret.GetByName(SecretName)
	if !ok || len(svcAcc) == 0 {
		return models.ReadResourceResponse{}, errors.New(fmt.Sprintf(errSecretNotFoundStr, SecretName, b.Name()))
	}

	client, err := b.ClientFac.New(ctx, svcAcc)
	if err != nil {
		return models.ReadResourceResponse{}, err
	}

	switch request.Resource.Type {
	case models.ResourceTypeTable:
		info, err := getTable(ctx, request.Resource, client)
		if err != nil {
			return models.ReadResourceResponse{}, err
		}
		return models.ReadResourceResponse{
			Resource: info,
		}, nil
	case models.ResourceTypeView:
		info, err := getTable(ctx, request.Resource, client)
		if err != nil {
			return models.ReadResourceResponse{}, err
		}
		return models.ReadResourceResponse{
			Resource: info,
		}, nil
	case models.ResourceTypeDataset:
		info, err := getDataset(ctx, request.Resource, client)
		if err != nil {
			return models.ReadResourceResponse{}, err
		}
		return models.ReadResourceResponse{
			Resource: info,
		}, nil
	}
	return models.ReadResourceResponse{}, fmt.Errorf("unsupported resource type %s", request.Resource.Type)
}

func (b *BigQuery) DeleteResource(ctx context.Context, request models.DeleteResourceRequest) error {
	svcAcc, ok := request.Project.Secret.GetByName(SecretName)
	if !ok || len(svcAcc) == 0 {
		return errors.New(fmt.Sprintf(errSecretNotFoundStr, SecretName, b.Name()))
	}

	client, err := b.ClientFac.New(ctx, svcAcc)
	if err != nil {
		return err
	}

	switch request.Resource.Type {
	case models.ResourceTypeTable:
		return deleteTable(ctx, request.Resource, client)
	case models.ResourceTypeView:
		return deleteTable(ctx, request.Resource, client)
	case models.ResourceTypeDataset:
		return deleteDataset(ctx, request.Resource, client)
	}
	return fmt.Errorf("unsupported resource type %s", request.Resource.Type)
}

func (b *BigQuery) BackupResource(ctx context.Context, request models.BackupResourceRequest) (models.BackupResourceResponse, error) {
	if request.Resource.Type != models.ResourceTypeTable {
		return models.BackupResourceResponse{}, models.ErrUnsupportedResource
	}

	if request.BackupSpec.DryRun {
		return models.BackupResourceResponse{}, nil
	}

	svcAcc, ok := request.BackupSpec.Project.Secret.GetByName(SecretName)
	if !ok || len(svcAcc) == 0 {
		return models.BackupResourceResponse{}, fmt.Errorf(errSecretNotFoundStr, SecretName, b.Name())
	}

	client, err := b.ClientFac.New(ctx, svcAcc)
	if err != nil {
		return models.BackupResourceResponse{}, err
	}

	return backupTable(ctx, request, client)
}

func init() {
	if err := models.DatastoreRegistry.Add(This); err != nil {
		panic(err)
	}
}
