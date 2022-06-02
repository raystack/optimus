package datastore

import (
	"context"
	"crypto/sha256"
	"errors"
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
	runner := parallel.NewRunner(parallel.WithLimit(ConcurrentLimit), parallel.WithTicket(ConcurrentTicketPerSec))
	for _, resourceSpec := range resourceSpecs {
		incomingSpec := resourceSpec
		repo := srv.resourceRepoFactory.New(namespace, incomingSpec.Datastore)
		runner.Add(func() (interface{}, error) {
			proceed, err := srv.isProceedToSave(ctx, repo, incomingSpec)
			if err != nil {
				return nil, err
			}

			if !proceed {
				srv.notifyProgress(obs, &EventResourceSkipped{
					Spec:    incomingSpec,
					Process: "create",
					Reason:  "incoming resource is the same as existing",
				})
				return nil, nil // nolint:nilnil
			}

			if err := repo.Save(ctx, incomingSpec); err != nil {
				return nil, err
			}
			err = incomingSpec.Datastore.CreateResource(ctx, models.CreateResourceRequest{
				Resource: incomingSpec,
				Project:  namespace.ProjectSpec,
			})
			srv.notifyProgress(obs, &EventResourceCreated{
				Spec: incomingSpec,
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
		incomingSpec := resourceSpec
		repo := srv.resourceRepoFactory.New(namespace, incomingSpec.Datastore)
		runner.Add(func() (interface{}, error) {
			proceed, err := srv.isProceedToSave(ctx, repo, incomingSpec)
			if err != nil {
				return nil, err
			}

			if !proceed {
				srv.notifyProgress(obs, &EventResourceSkipped{
					Spec:    incomingSpec,
					Process: "update",
					Reason:  "incoming resource is the same as existing",
				})
				return nil, nil // nolint:nilnil
			}

			if err := repo.Save(ctx, incomingSpec); err != nil {
				return nil, err
			}
			err = incomingSpec.Datastore.UpdateResource(ctx, models.UpdateResourceRequest{
				Resource: incomingSpec,
				Project:  namespace.ProjectSpec,
			})
			srv.notifyProgress(obs, &EventResourceUpdated{
				Spec: incomingSpec,
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

func (srv Service) isProceedToSave(ctx context.Context, repo store.ResourceSpecRepository, incomingSpec models.ResourceSpec) (bool, error) {
	var proceed bool
	if existingSpec, err := repo.GetByName(ctx, incomingSpec.Name); err != nil {
		if !errors.Is(err, store.ErrResourceNotFound) {
			return proceed, err
		}
		proceed = true
	} else {
		incomingSpec.ID = existingSpec.ID
		incomingSpec.URN = existingSpec.URN
		proceed = !srv.isSameHash(existingSpec, incomingSpec)
	}
	return proceed, nil
}

func (srv Service) isSameHash(rsc1, rsc2 models.ResourceSpec) bool {
	hash1, err := srv.calculateHash(rsc1)
	if err != nil {
		return false
	}
	hash2, err := srv.calculateHash(rsc2)
	if err != nil {
		return false
	}
	return hash1 == hash2
}

func (Service) calculateHash(rsc models.ResourceSpec) (string, error) {
	h := sha256.New()
	rep := fmt.Sprintf("%+v", rsc)
	_, err := h.Write([]byte(rep))
	return fmt.Sprintf("%x", h.Sum(nil)), err
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
		Spec    models.ResourceSpec
		Process string
		Reason  string
	}
)

func (e *EventResourceSkipped) String() string {
	return fmt.Sprintf("process [%s] on resource [%s] is skipped because %s", e.Process, e.Spec.Name, e.Reason)
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
