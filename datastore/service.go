package datastore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/utils"

	"github.com/hashicorp/go-multierror"
	"github.com/kushsharma/parallel"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

const (
	ConcurrentTicketPerSec = 5
	ConcurrentLimit        = 20

	//backupListWindow window interval to fetch recent backups
	backupListWindow = -3 * 30 * 24 * time.Hour
)

type ResourceSpecRepoFactory interface {
	New(namespace models.NamespaceSpec, storer models.Datastorer) store.ResourceSpecRepository
}

type ProjectResourceSpecRepoFactory interface {
	New(spec models.ProjectSpec, storer models.Datastorer) store.ProjectResourceSpecRepository
}

type BackupRepoFactory interface {
	New(spec models.ProjectSpec, storer models.Datastorer) store.BackupRepository
}

type NamespaceRepoFactory interface {
	New(spec models.ProjectSpec) store.NamespaceRepository
}

type Service struct {
	resourceRepoFactory        ResourceSpecRepoFactory
	projectResourceRepoFactory ProjectResourceSpecRepoFactory
	dsRepo                     models.DatastoreRepo
	backupRepoFactory          BackupRepoFactory
	uuidProvider               utils.UUIDProvider
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
		currentSpec := resourceSpec
		repo := srv.resourceRepoFactory.New(namespace, currentSpec.Datastore)
		runner.Add(func() (interface{}, error) {
			if err := repo.Save(ctx, currentSpec); err != nil {
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
			if err := repo.Save(ctx, currentSpec); err != nil {
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

func generateResourceDestination(ctx context.Context, jobSpec models.JobSpec) (*models.GenerateDestinationResponse, error) {
	return jobSpec.Task.Unit.DependencyMod.GenerateDestination(ctx, models.GenerateDestinationRequest{
		Config: models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
		Assets: models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
	})
}

func (srv Service) BackupResourceDryRun(ctx context.Context, backupRequest models.BackupRequest, jobSpecs []models.JobSpec) (models.BackupPlan, error) {
	var resourcesToBackup []string
	var resourcesToIgnore []string
	for _, jobSpec := range jobSpecs {
		destination, err := generateResourceDestination(ctx, jobSpec)
		if err != nil {
			return models.BackupPlan{}, err
		}

		datastorer, err := srv.dsRepo.GetByName(destination.Type.String())
		if err != nil {
			return models.BackupPlan{}, err
		}

		projectResourceRepo := srv.projectResourceRepoFactory.New(backupRequest.Project, datastorer)
		resourceSpec, namespaceSpec, err := projectResourceRepo.GetByURN(ctx, destination.URN())
		if err != nil {
			if err == store.ErrResourceNotFound {
				continue
			}
			return models.BackupPlan{}, err
		}

		if resourceSpec.Name != backupRequest.ResourceName {
			isAuthorized := false
			for _, allowedNamespace := range backupRequest.AllowedDownstreamNamespaces {
				if allowedNamespace == models.AllNamespace || allowedNamespace == namespaceSpec.Name {
					isAuthorized = true
					break
				}
			}
			if !isAuthorized {
				resourcesToIgnore = append(resourcesToIgnore, destination.Destination)
				continue
			}
		}

		//do backup in storer
		_, err = datastorer.BackupResource(ctx, models.BackupResourceRequest{
			Resource:   resourceSpec,
			BackupSpec: backupRequest,
		})
		if err != nil {
			if err == models.ErrUnsupportedResource {
				continue
			}
			return models.BackupPlan{}, err
		}

		resourcesToBackup = append(resourcesToBackup, destination.Destination)
	}
	return models.BackupPlan{
		Resources:        resourcesToBackup,
		IgnoredResources: resourcesToIgnore,
	}, nil
}

func (srv Service) BackupResource(ctx context.Context, backupRequest models.BackupRequest, jobSpecs []models.JobSpec) (models.BackupResult, error) {
	backupSpec, err := srv.prepareBackupSpec(backupRequest)
	if err != nil {
		return models.BackupResult{}, err
	}
	backupRequest.ID = backupSpec.ID
	backupTime := time.Now()

	var resources []string
	var resourcesToIgnore []string
	for _, jobSpec := range jobSpecs {
		destination, err := generateResourceDestination(ctx, jobSpec)
		if err != nil {
			return models.BackupResult{}, err
		}

		datastorer, err := srv.dsRepo.GetByName(destination.Type.String())
		if err != nil {
			return models.BackupResult{}, err
		}

		projectResourceRepo := srv.projectResourceRepoFactory.New(backupRequest.Project, datastorer)
		resourceSpec, namespaceSpec, err := projectResourceRepo.GetByURN(ctx, destination.URN())
		if err != nil {
			if err == store.ErrResourceNotFound {
				continue
			}
			return models.BackupResult{}, err
		}

		if resourceSpec.Name != backupRequest.ResourceName {
			isAuthorized := false
			for _, allowedDownstream := range backupRequest.AllowedDownstreamNamespaces {
				if allowedDownstream == models.AllNamespace || allowedDownstream == namespaceSpec.Name {
					isAuthorized = true
					break
				}
			}
			if !isAuthorized {
				resourcesToIgnore = append(resourcesToIgnore, destination.Destination)
				continue
			}
		}

		//do backup in storer
		backupResp, err := datastorer.BackupResource(ctx, models.BackupResourceRequest{
			Resource:   resourceSpec,
			BackupSpec: backupRequest,
			BackupTime: backupTime,
		})
		if err != nil {
			if err == models.ErrUnsupportedResource {
				continue
			}
			return models.BackupResult{}, err
		}
		// form slices of result urn to return
		resources = append(resources, backupResp.ResultURN)
		// enrich backup spec with result detail to be saved
		backupSpec.Result[destination.Destination] = models.BackupDetail{
			URN:  backupResp.ResultURN,
			Spec: backupResp.ResultSpec,
		}
		// enrich backup spec with resource detail to be saved
		if resourceSpec.Name == backupRequest.ResourceName {
			backupSpec.Resource = resourceSpec
		}
	}

	//save the backup
	backupRepo := srv.backupRepoFactory.New(backupRequest.Project, backupSpec.Resource.Datastore)
	if err := backupRepo.Save(ctx, backupSpec); err != nil {
		return models.BackupResult{}, err
	}

	return models.BackupResult{
		Resources:        resources,
		IgnoredResources: resourcesToIgnore,
	}, nil
}

func (srv Service) ListBackupResources(ctx context.Context, projectSpec models.ProjectSpec, datastoreName string) ([]models.BackupSpec, error) {
	datastorer, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return []models.BackupSpec{}, err
	}

	backupRepo := srv.backupRepoFactory.New(projectSpec, datastorer)
	backupSpecs, err := backupRepo.GetAll(ctx)
	if err != nil {
		if err == store.ErrResourceNotFound {
			return []models.BackupSpec{}, nil
		}
		return []models.BackupSpec{}, err
	}

	var recentBackups []models.BackupSpec
	for _, backup := range backupSpecs {
		if backup.CreatedAt.After(time.Now().UTC().Add(backupListWindow)) {
			recentBackups = append(recentBackups, backup)
		}
	}
	return recentBackups, nil
}

func (srv Service) GetBackupResourceDetail(ctx context.Context, projectSpec models.ProjectSpec, datastoreName string,
	id uuid.UUID) (models.BackupSpec, error) {
	datastorer, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return models.BackupSpec{}, err
	}

	backupRepo := srv.backupRepoFactory.New(projectSpec, datastorer)
	return backupRepo.GetByID(ctx, id)
}

func (srv Service) prepareBackupSpec(backupRequest models.BackupRequest) (models.BackupSpec, error) {
	backupID, err := srv.uuidProvider.NewUUID()
	if err != nil {
		return models.BackupSpec{}, err
	}
	backupRequest.Config[models.ConfigIgnoreDownstream] = strconv.FormatBool(backupRequest.IgnoreDownstream)
	return models.BackupSpec{
		ID:          backupID,
		Description: backupRequest.Description,
		Config:      backupRequest.Config,
		Result:      make(map[string]interface{}),
	}, nil
}

func (srv *Service) notifyProgress(po progress.Observer, event progress.Event) {
	if po == nil {
		return
	}
	po.Notify(event)
}

func NewService(resourceRepoFactory ResourceSpecRepoFactory, projectResourceRepoFactory ProjectResourceSpecRepoFactory,
	dsRepo models.DatastoreRepo, uuidProvider utils.UUIDProvider, backupRepoFactory BackupRepoFactory) *Service {
	return &Service{
		resourceRepoFactory:        resourceRepoFactory,
		projectResourceRepoFactory: projectResourceRepoFactory,
		dsRepo:                     dsRepo,
		backupRepoFactory:          backupRepoFactory,
		uuidProvider:               uuidProvider,
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
