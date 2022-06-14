package datastore

import (
	"context"
	"errors"
	"fmt"
	"reflect"

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

type Service struct {
	resourceRepoFactory ResourceSpecRepoFactory
	dsRepo              models.DatastoreRepo
}

func (srv Service) GetAll(ctx context.Context, namespace models.NamespaceSpec, datastoreName string) ([]models.ResourceSpec, error) {
	ds, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return nil, err
	}
	return srv.resourceRepoFactory.New(namespace, ds).GetAll(ctx)
}

func (srv Service) CreateResource(ctx context.Context, namespace models.NamespaceSpec, resourceSpecs []models.ResourceSpec, obs progress.Observer) error {
	createResource := func(rs models.ResourceSpec) error {
		request := models.CreateResourceRequest{
			Resource: rs,
			Project:  namespace.ProjectSpec,
		}
		err := rs.Datastore.CreateResource(ctx, request)
		srv.notifyProgress(obs, &EventResourceCreated{
			Spec: rs,
			Err:  err,
		})
		return err
	}
	return srv.saveResource(ctx, namespace, resourceSpecs, obs, createResource)
}

func (srv Service) UpdateResource(ctx context.Context, namespace models.NamespaceSpec, resourceSpecs []models.ResourceSpec, obs progress.Observer) error {
	updateDatastore := func(rs models.ResourceSpec) error {
		request := models.UpdateResourceRequest{
			Resource: rs,
			Project:  namespace.ProjectSpec,
		}
		err := rs.Datastore.UpdateResource(ctx, request)
		srv.notifyProgress(obs, &EventResourceUpdated{
			Spec: rs,
			Err:  err,
		})
		return err
	}
	return srv.saveResource(ctx, namespace, resourceSpecs, obs, updateDatastore)
}

func (srv Service) ReadResource(ctx context.Context, namespace models.NamespaceSpec, datastoreName, name string) (models.ResourceSpec, error) {
	ds, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return models.ResourceSpec{}, err
	}
	repo := srv.resourceRepoFactory.New(namespace, ds)
	dbSpec, err := repo.GetByName(ctx, name)
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
	resourceSpec, err := repo.GetByName(ctx, name)
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

	return repo.Delete(ctx, name)
}

func (srv Service) saveResource(
	ctx context.Context,
	namespace models.NamespaceSpec,
	resourceSpecs []models.ResourceSpec,
	obs progress.Observer,
	storeDatastore func(models.ResourceSpec) error,
) error {
	runner := parallel.NewRunner(parallel.WithLimit(ConcurrentLimit), parallel.WithTicket(ConcurrentTicketPerSec))
	for _, incomingSpec := range resourceSpecs {
		repo := srv.resourceRepoFactory.New(namespace, incomingSpec.Datastore)
		runner.Add(func() (interface{}, error) {
			proceed, err := srv.isProceedToSave(ctx, repo, incomingSpec)
			if err != nil {
				return nil, err
			}

			if !proceed {
				srv.notifyProgress(obs, &EventResourceSkipped{
					Spec:   incomingSpec,
					Reason: "incoming resource is the same as existing",
				})
				return nil, nil // nolint:nilnil
			}

			if err := repo.Save(ctx, incomingSpec); err != nil {
				return nil, err
			}
			return nil, storeDatastore(incomingSpec)
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

func (Service) isProceedToSave(ctx context.Context, repo store.ResourceSpecRepository, incomingSpec models.ResourceSpec) (bool, error) {
	var proceed bool
	if existingSpec, err := repo.GetByName(ctx, incomingSpec.Name); err != nil {
		if !errors.Is(err, store.ErrResourceNotFound) {
			return proceed, err
		}
		proceed = true
	} else {
		incomingSpec.ID = existingSpec.ID
		incomingSpec.URN = existingSpec.URN
		proceed = !reflect.DeepEqual(existingSpec, incomingSpec)
	}
	return proceed, nil
}

func (*Service) notifyProgress(po progress.Observer, event progress.Event) {
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

	// EventResourceSkipped represents the resource being skipped in datastore
	EventResourceSkipped struct {
		Spec   models.ResourceSpec
		Reason string
	}
)

func (e *EventResourceSkipped) String() string {
	return fmt.Sprintf("resource [%s] is skipped because %s", e.Spec.Name, e.Reason)
}

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
