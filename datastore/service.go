package datastore

import (
	"context"
	"fmt"

	"github.com/odpf/optimus/core/progress"

	"github.com/hashicorp/go-multierror"
	"github.com/kushsharma/parallel"

	"github.com/odpf/optimus/store"

	"github.com/odpf/optimus/models"
)

type ResourceSpecRepoFactory interface {
	New(spec models.ProjectSpec, storer models.Datastorer) store.ResourceSpecRepository
}

type Service struct {
	resourceRepoFactory ResourceSpecRepoFactory
	dsRepo              models.DatastoreRepo
}

func (srv Service) GetAll(proj models.ProjectSpec, datastoreName string) ([]models.ResourceSpec, error) {
	ds, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return nil, err
	}
	return srv.resourceRepoFactory.New(proj, ds).GetAll()
}

func (srv Service) CreateResource(ctx context.Context, proj models.ProjectSpec,
	resourceSpecs []models.ResourceSpec, obs progress.Observer) error {
	runner := parallel.NewRunner()
	for _, resourceSpec := range resourceSpecs {
		currentSpec := resourceSpec
		repo := srv.resourceRepoFactory.New(proj, currentSpec.Datastore)
		runner.Add(func() (interface{}, error) {
			if err := repo.Save(currentSpec); err != nil {
				return nil, err
			}

			err := currentSpec.Datastore.CreateResource(ctx, models.CreateResourceRequest{
				Resource: currentSpec,
				Project:  proj,
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

func (srv Service) UpdateResource(ctx context.Context, proj models.ProjectSpec,
	resourceSpecs []models.ResourceSpec, obs progress.Observer) error {
	runner := parallel.NewRunner()
	for _, resourceSpec := range resourceSpecs {
		currentSpec := resourceSpec
		repo := srv.resourceRepoFactory.New(proj, currentSpec.Datastore)
		runner.Add(func() (interface{}, error) {
			if err := repo.Save(currentSpec); err != nil {
				return nil, err
			}

			err := currentSpec.Datastore.UpdateResource(ctx, models.UpdateResourceRequest{
				Resource: currentSpec,
				Project:  proj,
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

func (srv Service) ReadResource(ctx context.Context, proj models.ProjectSpec, datastoreName, name string) (models.ResourceSpec, error) {
	ds, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return models.ResourceSpec{}, err
	}
	repo := srv.resourceRepoFactory.New(proj, ds)
	dbSpec, err := repo.GetByName(name)
	if err != nil {
		return models.ResourceSpec{}, err
	}

	infoResponse, err := dbSpec.Datastore.ReadResource(ctx, models.ReadResourceRequest{
		Resource: dbSpec,
		Project:  proj,
	})
	if err != nil {
		return models.ResourceSpec{}, err
	}
	return infoResponse.Resource, nil
}

func (srv Service) DeleteResource(ctx context.Context, proj models.ProjectSpec, datastoreName, name string) error {
	ds, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return err
	}
	repo := srv.resourceRepoFactory.New(proj, ds)
	resourceSpec, err := repo.GetByName(name)
	if err != nil {
		return err
	}

	// migrate the deleted resource
	if err := resourceSpec.Datastore.DeleteResource(ctx, models.DeleteResourceRequest{
		Resource: resourceSpec,
		Project:  proj,
	}); err != nil {
		return err
	}

	return repo.Delete(name)
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
		return fmt.Sprintf("updating: %s, failed with error): %s", e.Spec.Name, e.Err.Error())
	}
	return fmt.Sprintf("updated: %s", e.Spec.Name)
}

func (e *EventResourceCreated) String() string {
	if e.Err != nil {
		return fmt.Sprintf("creating: %s, failed with error): %s", e.Spec.Name, e.Err.Error())
	}
	return fmt.Sprintf("created: %s", e.Spec.Name)
}
