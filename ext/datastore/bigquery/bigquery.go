package bigquery

import (
	"context"
	"fmt"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"

	"github.com/odpf/optimus/models"
)

const (
	// SecretName for creation and manipulation of a project resources in bigquery
	SecretName = "DATASTORE_BIGQUERY" //nolint:gosec
)

var (
	This = &BigQuery{
		ClientFac: &defaultBQClientFactory{},
	}

	errSecretNotFoundStr = "secret %s required to migrate datastore not found for %s" //nolint:gosec
)

type ClientFactory interface {
	New(ctx context.Context, svcAccount string) (bqiface.Client, error)
}

type BigQuery struct {
	ClientFac ClientFactory
}

func (BigQuery) Name() string {
	return "bigquery"
}

func (BigQuery) Description() string {
	return "GCP BigQuery"
}

func (BigQuery) Types() map[models.ResourceType]models.DatastoreTypeController {
	return map[models.ResourceType]models.DatastoreTypeController{
		models.ResourceTypeTable:         &tableSpec{},
		models.ResourceTypeView:          &standardViewSpec{},
		models.ResourceTypeDataset:       &datasetSpec{},
		models.ResourceTypeExternalTable: &externalTableSpec{},
	}
}

func (b *BigQuery) CreateResource(ctx context.Context, request models.CreateResourceRequest) error {
	spanCtx, span := startChildSpan(ctx, "CreateResource")
	defer span.End()

	svcAcc, ok := request.Project.Secret.GetByName(SecretName)
	if !ok || svcAcc == "" {
		return fmt.Errorf(errSecretNotFoundStr, SecretName, b.Name())
	}

	client, err := b.ClientFac.New(spanCtx, svcAcc)
	if err != nil {
		return err
	}

	switch request.Resource.Type {
	case models.ResourceTypeTable:
		return createTable(spanCtx, request.Resource, client, false)
	case models.ResourceTypeView:
		return createStandardView(spanCtx, request.Resource, client, false)
	case models.ResourceTypeDataset:
		return createDataset(spanCtx, request.Resource, client, false)
	case models.ResourceTypeExternalTable:
		return createExternalTable(spanCtx, request.Resource, client, false)
	}
	return fmt.Errorf("unsupported resource type %s", request.Resource.Type)
}

func (b *BigQuery) UpdateResource(ctx context.Context, request models.UpdateResourceRequest) error {
	spanCtx, span := startChildSpan(ctx, "UpdateResource")
	defer span.End()

	svcAcc, ok := request.Project.Secret.GetByName(SecretName)
	if !ok || svcAcc == "" {
		return fmt.Errorf(errSecretNotFoundStr, SecretName, b.Name())
	}

	client, err := b.ClientFac.New(spanCtx, svcAcc)
	if err != nil {
		return err
	}

	switch request.Resource.Type {
	case models.ResourceTypeTable:
		return createTable(spanCtx, request.Resource, client, true)
	case models.ResourceTypeView:
		return createStandardView(spanCtx, request.Resource, client, true)
	case models.ResourceTypeDataset:
		return createDataset(spanCtx, request.Resource, client, true)
	case models.ResourceTypeExternalTable:
		return createExternalTable(spanCtx, request.Resource, client, true)
	}
	return fmt.Errorf("unsupported resource type %s", request.Resource.Type)
}

func (b *BigQuery) ReadResource(ctx context.Context, request models.ReadResourceRequest) (models.ReadResourceResponse, error) {
	spanCtx, span := startChildSpan(ctx, "ReadResource")
	defer span.End()

	svcAcc, ok := request.Project.Secret.GetByName(SecretName)
	if !ok || svcAcc == "" {
		return models.ReadResourceResponse{}, fmt.Errorf(errSecretNotFoundStr, SecretName, b.Name())
	}

	client, err := b.ClientFac.New(spanCtx, svcAcc)
	if err != nil {
		return models.ReadResourceResponse{}, err
	}

	switch request.Resource.Type {
	case models.ResourceTypeTable:
		info, err := getTable(spanCtx, request.Resource, client)
		if err != nil {
			return models.ReadResourceResponse{}, err
		}
		return models.ReadResourceResponse{
			Resource: info,
		}, nil
	case models.ResourceTypeView:
		info, err := getTable(spanCtx, request.Resource, client)
		if err != nil {
			return models.ReadResourceResponse{}, err
		}
		return models.ReadResourceResponse{
			Resource: info,
		}, nil
	case models.ResourceTypeDataset:
		info, err := getDataset(spanCtx, request.Resource, client)
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
	spanCtx, span := startChildSpan(ctx, "DeleteResource")
	defer span.End()

	svcAcc, ok := request.Project.Secret.GetByName(SecretName)
	if !ok || svcAcc == "" {
		return fmt.Errorf(errSecretNotFoundStr, SecretName, b.Name())
	}

	client, err := b.ClientFac.New(spanCtx, svcAcc)
	if err != nil {
		return err
	}

	switch request.Resource.Type {
	case models.ResourceTypeTable:
		return deleteTable(spanCtx, request.Resource, client)
	case models.ResourceTypeView:
		return deleteTable(spanCtx, request.Resource, client)
	case models.ResourceTypeDataset:
		return deleteDataset(spanCtx, request.Resource, client)
	}
	return fmt.Errorf("unsupported resource type %s", request.Resource.Type)
}

func (b *BigQuery) BackupResource(ctx context.Context, request models.BackupResourceRequest) (models.BackupResourceResponse, error) {
	spanCtx, span := startChildSpan(ctx, "BackupResource")
	defer span.End()

	if request.Resource.Type != models.ResourceTypeTable {
		return models.BackupResourceResponse{}, models.ErrUnsupportedResource
	}

	if request.BackupSpec.DryRun {
		return models.BackupResourceResponse{}, nil
	}

	svcAcc, ok := request.BackupSpec.Project.Secret.GetByName(SecretName)
	if !ok || svcAcc == "" {
		return models.BackupResourceResponse{}, fmt.Errorf(errSecretNotFoundStr, SecretName, b.Name())
	}

	client, err := b.ClientFac.New(spanCtx, svcAcc)
	if err != nil {
		return models.BackupResourceResponse{}, err
	}

	return backupTable(spanCtx, request, client)
}

func init() { //nolint:gochecknoinits
	if err := models.DatastoreRegistry.Add(This); err != nil {
		panic(err)
	}
}
