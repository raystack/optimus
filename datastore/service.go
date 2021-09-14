package datastore

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/kushsharma/parallel"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

const (
	ConcurrentTicketPerSec = 5
	ConcurrentLimit        = 20
)

type ResourceSpecRepoFactory interface {
	New(namespace models.NamespaceSpec, storer models.Datastorer) store.ResourceSpecRepository
}

type ProjectResourceSpecRepoFactory interface {
	New(spec models.ProjectSpec, storer models.Datastorer) store.ProjectResourceSpecRepository
}

type Service struct {
	resourceRepoFactory ResourceSpecRepoFactory
	dsRepo              models.DatastoreRepo
}

func (srv Service) GetAll(namespace models.NamespaceSpec, datastoreName string) ([]models.ResourceSpec, error) {
	ds, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return nil, err
	}
	return srv.resourceRepoFactory.New(namespace, ds).GetAll()
}

func (srv Service) CreateResource(ctx context.Context, namespace models.NamespaceSpec, resourceSpecs []models.ResourceSpec, obs progress.Observer) error {
	runner := parallel.NewRunner(parallel.WithLimit(ConcurrentLimit), parallel.WithTicket(ConcurrentTicketPerSec))
	for _, resourceSpec := range resourceSpecs {
		currentSpec := resourceSpec
		repo := srv.resourceRepoFactory.New(namespace, currentSpec.Datastore)
		runner.Add(func() (interface{}, error) {
			if err := repo.Save(currentSpec); err != nil {
				return nil, err
			}

			err := currentSpec.Datastore.CreateResource(ctx, models.CreateResourceRequest{
				Resource: currentSpec,
				Project:  namespace.ProjectSpec,
			})
			srv.notifyProgress(obs, &EventResourceCreated{
				Spec: currentSpec,
				Err:  err,
			})
			return nil, err
		})
	}

	var errorSet error
	for _, result := range runner.Run() {
		if result.Err != nil {
			errorSet = multierror.Append(errorSet, result.Err)
		}
	}
	return errorSet
}

func (srv Service) UpdateResource(ctx context.Context, namespace models.NamespaceSpec, resourceSpecs []models.ResourceSpec, obs progress.Observer) error {
	runner := parallel.NewRunner(parallel.WithLimit(ConcurrentLimit), parallel.WithTicket(ConcurrentTicketPerSec))
	for _, resourceSpec := range resourceSpecs {
		currentSpec := resourceSpec
		repo := srv.resourceRepoFactory.New(namespace, currentSpec.Datastore)
		runner.Add(func() (interface{}, error) {
			if err := repo.Save(currentSpec); err != nil {
				return nil, err
			}

			err := currentSpec.Datastore.UpdateResource(ctx, models.UpdateResourceRequest{
				Resource: currentSpec,
				Project:  namespace.ProjectSpec,
			})
			srv.notifyProgress(obs, &EventResourceUpdated{
				Spec: currentSpec,
				Err:  err,
			})
			return nil, err
		})
	}

	var errorSet error
	for _, result := range runner.Run() {
		if result.Err != nil {
			errorSet = multierror.Append(errorSet, result.Err)
		}
	}
	return errorSet
}

func (srv Service) ReadResource(ctx context.Context, namespace models.NamespaceSpec, datastoreName, name string) (models.ResourceSpec, error) {
	ds, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return models.ResourceSpec{}, err
	}
	repo := srv.resourceRepoFactory.New(namespace, ds)
	dbSpec, err := repo.GetByName(name)
	if err != nil {
		return models.ResourceSpec{}, err
	}

	infoResponse, err := dbSpec.Datastore.ReadResource(ctx, models.ReadResourceRequest{
		Resource: dbSpec,
		Project:  namespace.ProjectSpec,
	})
	if err != nil {
		return models.ResourceSpec{}, err
	}
	return infoResponse.Resource, nil
}

func (srv Service) DeleteResource(ctx context.Context, namespace models.NamespaceSpec, datastoreName, name string) error {
	ds, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return err
	}
	repo := srv.resourceRepoFactory.New(namespace, ds)
	resourceSpec, err := repo.GetByName(name)
	if err != nil {
		return err
	}

	// migrate the deleted resource
	if err := resourceSpec.Datastore.DeleteResource(ctx, models.DeleteResourceRequest{
		Resource: resourceSpec,
		Project:  namespace.ProjectSpec,
	}); err != nil {
		return err
	}

	return repo.Delete(name)
}

func (srv Service) BackupResourceDryRun(ctx context.Context, projectSpec models.ProjectSpec, namespaceSpec models.NamespaceSpec, jobSpecs []models.JobSpec) ([]string, error) {
	var resourcesToBackup []string
	for _, jobSpec := range jobSpecs {
		destination, err := jobSpec.Task.Unit.DependencyMod.GenerateDestination(context.TODO(), models.GenerateDestinationRequest{
			Config: models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
			Assets: models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
		})
		if err != nil {
			return nil, err
		}

		ds, err := srv.dsRepo.GetByName(destination.Type.String())
		if err != nil {
			return nil, err
		}

		repo := srv.resourceRepoFactory.New(namespaceSpec, ds)
		resourceSpec, err := repo.GetByURN(destination.URN())
		if err != nil {
			if err == store.ErrResourceNotFound {
				continue
			}
			return nil, err
		}

		backupReq := models.BackupResourceRequest{
			Resource: resourceSpec,
			Project:  projectSpec,
			DryRun:   true,
		}
		if err := ds.BackupResource(ctx, backupReq); err != nil {
			if err == models.ErrUnsupportedResource {
				continue
			}
			return nil, err
		}
		resourcesToBackup = append(resourcesToBackup, destination.Destination)
	}
	return resourcesToBackup, nil
}

func (srv *Service) notifyProgress(po progress.Observer, event progress.Event) {
	if po == nil {
		return
	}
	po.Notify(event)
}

func NewService(resourceRepoFactory ResourceSpecRepoFactory, dsRepo models.DatastoreRepo) *Service {
	return &Service{
		resourceRepoFactory: resourceRepoFactory,
		dsRepo:              dsRepo,
	}
}

type (
	// EventResourceCreated represents the resource being created in datastore
	EventResourceCreated struct {
		Spec models.ResourceSpec
		Err  error
	}

	// EventResourceUpdated represents the resource being updated in datastore
	EventResourceUpdated struct {
		Spec models.ResourceSpec
		Err  error
	}
)

func (e *EventResourceUpdated) String() string {
	if e.Err != nil {
		return fmt.Sprintf("updating: %s, failed with error: %s", e.Spec.Name, e.Err.Error())
	}
	return fmt.Sprintf("updated: %s", e.Spec.Name)
}

func (e *EventResourceCreated) String() string {
	if e.Err != nil {
		return fmt.Sprintf("creating: %s, failed with error: %s", e.Spec.Name, e.Err.Error())
	}
	return fmt.Sprintf("created: %s", e.Spec.Name)
}
