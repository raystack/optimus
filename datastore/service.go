package datastore

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/kushsharma/parallel"
	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/core/sender"
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
	l                   log.Logger
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

func (srv Service) CreateResource(ctx context.Context, namespace models.NamespaceSpec, resourceSpecs []models.ResourceSpec) error {
	createResource := func(rs models.ResourceSpec) error {
		request := models.CreateResourceRequest{
			Resource: rs,
			Project:  namespace.ProjectSpec,
		}
		if err := rs.Datastore.CreateResource(ctx, request); err != nil {
			errMsg := fmt.Sprintf("creating: %s, failed with error: %s", rs.Name, err.Error())
			srv.l.Error(errMsg)
			return err
		}
		successMsg := fmt.Sprintf("created: %s", rs.Name)
		srv.l.Info(successMsg)
		return nil
	}
	return srv.saveResource(ctx, namespace, resourceSpecs, nil, nil, createResource)
}

func (srv Service) UpdateResource(ctx context.Context, namespace models.NamespaceSpec, resourceSpecs []models.ResourceSpec, logSender sender.LogStatus, progressSender sender.ProgressCount) error {
	updateDatastore := func(rs models.ResourceSpec) error {
		request := models.UpdateResourceRequest{
			Resource: rs,
			Project:  namespace.ProjectSpec,
		}
		err := rs.Datastore.UpdateResource(ctx, request)
		if err != nil {
			errMsg := fmt.Sprintf("updating: %s, failed with error: %s", rs.Name, err.Error())
			srv.l.Error(errMsg)
			sender.SendErrorMessage(logSender, errMsg)
			return err
		}

		successMsg := fmt.Sprintf("updated: %s", rs.Name)
		srv.l.Info(successMsg)
		sender.SendSuccessMessage(logSender, successMsg)
		return nil
	}
	return srv.saveResource(ctx, namespace, resourceSpecs, logSender, progressSender, updateDatastore)
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
	logSender sender.LogStatus,
	progressSender sender.ProgressCount,
	storeDatastore func(models.ResourceSpec) error,
) error {
	runner := parallel.NewRunner(parallel.WithLimit(ConcurrentLimit), parallel.WithTicket(ConcurrentTicketPerSec))
	for _, incomingSpec := range resourceSpecs {
		runner.Add(func(spec models.ResourceSpec, ls sender.LogStatus, ps sender.ProgressCount) func() (interface{}, error) {
			return func() (interface{}, error) {
				defer sender.ProgressInc(ps)

				repo := srv.resourceRepoFactory.New(namespace, spec.Datastore)
				existingSpec, err := repo.GetByName(ctx, spec.Name)
				if err != nil && !errors.Is(err, store.ErrResourceNotFound) {
					return nil, err
				}

				if existingSpec.Equal(spec) {
					warnMsg := fmt.Sprintf("resource [%s] is skipped because %s", incomingSpec.Name, "incoming resource is the same as existing")
					sender.SendWarningMessage(ls, warnMsg)
					return nil, nil // nolint:nilnil
				}
				if err := repo.Save(ctx, spec); err != nil {
					return nil, err
				}
				return nil, storeDatastore(spec)
			}
		}(incomingSpec, logSender, progressSender))
	}

	var errorSet error
	for _, result := range runner.Run() {
		if result.Err != nil {
			errorSet = multierror.Append(errorSet, result.Err)
		}
	}
	return errorSet
}

func NewService(logger log.Logger, resourceRepoFactory ResourceSpecRepoFactory, dsRepo models.DatastoreRepo) *Service {
	return &Service{
		l:                   logger,
		resourceRepoFactory: resourceRepoFactory,
		dsRepo:              dsRepo,
	}
}
