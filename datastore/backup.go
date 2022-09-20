package datastore

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/service"
)

// backupListWindow window interval to fetch recent backups
const backupListWindow = -3 * 30 * 24 * time.Hour

type ProjectResourceSpecRepoFactory interface {
	New(spec models.ProjectSpec, storer models.Datastorer) store.ProjectResourceSpecRepository
}

type BackupService struct {
	projectResourceRepoFactory ProjectResourceSpecRepoFactory
	dsRepo                     models.DatastoreRepo
	backupRepo                 store.BackupRepository
	uuidProvider               utils.UUIDProvider
	pluginService              service.PluginService
}

func (srv BackupService) BackupResourceDryRun(ctx context.Context, backupRequest models.BackupRequest, jobSpecs []models.JobSpec) (models.BackupPlan, error) {
	var resourcesToBackup []string
	var resourcesToIgnore []string
	for _, jobSpec := range jobSpecs {
		destination, err := srv.pluginService.GenerateDestination(ctx, jobSpec, backupRequest.Namespace)
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
			if errors.Is(err, store.ErrResourceNotFound) {
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

		// do backup in storer
		_, err = datastorer.BackupResource(ctx, models.BackupResourceRequest{
			Resource:   resourceSpec,
			BackupSpec: backupRequest,
		})
		if err != nil {
			if errors.Is(err, models.ErrUnsupportedResource) {
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

func (srv BackupService) BackupResource(ctx context.Context, backupRequest models.BackupRequest, jobSpecs []models.JobSpec) (models.BackupResult, error) {
	backupSpec, err := srv.prepareBackupSpec(backupRequest)
	if err != nil {
		return models.BackupResult{}, err
	}
	backupRequest.ID = backupSpec.ID
	backupTime := time.Now()

	var resources []string
	var resourcesToIgnore []string
	for _, jobSpec := range jobSpecs {
		destination, err := srv.pluginService.GenerateDestination(ctx, jobSpec, backupRequest.Namespace)
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
			if errors.Is(err, store.ErrResourceNotFound) {
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

		// do backup in storer
		backupResp, err := datastorer.BackupResource(ctx, models.BackupResourceRequest{
			Resource:   resourceSpec,
			BackupSpec: backupRequest,
			BackupTime: backupTime,
		})
		if err != nil {
			if errors.Is(err, models.ErrUnsupportedResource) {
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

	// save the backup
	if err := srv.backupRepo.Save(ctx, backupSpec); err != nil {
		return models.BackupResult{}, err
	}

	return models.BackupResult{
		Resources:        resources,
		IgnoredResources: resourcesToIgnore,
	}, nil
}

func (srv BackupService) ListResourceBackups(ctx context.Context, projectSpec models.ProjectSpec, datastoreName string) ([]models.BackupSpec, error) {
	datastorer, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return []models.BackupSpec{}, err
	}

	backupSpecs, err := srv.backupRepo.GetAll(ctx, projectSpec, datastorer)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
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

func (srv BackupService) GetResourceBackup(ctx context.Context, _ models.ProjectSpec, datastoreName string,
	id uuid.UUID) (models.BackupSpec, error) {
	datastorer, err := srv.dsRepo.GetByName(datastoreName)
	if err != nil {
		return models.BackupSpec{}, err
	}

	return srv.backupRepo.GetByID(ctx, id, datastorer)
}

func (srv BackupService) prepareBackupSpec(backupRequest models.BackupRequest) (models.BackupSpec, error) {
	backupID, err := srv.uuidProvider.NewUUID()
	if err != nil {
		return models.BackupSpec{}, err
	}
	backupRequest.Config = addIgnoreDownstreamConfig(backupRequest.Config, backupRequest.AllowedDownstreamNamespaces)
	return models.BackupSpec{
		ID:          backupID,
		Description: backupRequest.Description,
		Config:      backupRequest.Config,
		Result:      make(map[string]interface{}),
	}, nil
}

func addIgnoreDownstreamConfig(config map[string]string, allowedDownstreamNamespaces []string) map[string]string {
	if len(config) == 0 {
		config = make(map[string]string)
	}

	ignoreDownstream := true
	if len(allowedDownstreamNamespaces) > 0 {
		ignoreDownstream = false
	}

	config[models.ConfigIgnoreDownstream] = strconv.FormatBool(ignoreDownstream)
	return config
}

func NewBackupService(projectResourceRepoFactory ProjectResourceSpecRepoFactory, dsRepo models.DatastoreRepo, uuidProvider utils.UUIDProvider, backupRepo store.BackupRepository, pluginService service.PluginService) *BackupService {
	return &BackupService{
		projectResourceRepoFactory: projectResourceRepoFactory,
		dsRepo:                     dsRepo,
		backupRepo:                 backupRepo,
		uuidProvider:               uuidProvider,
		pluginService:              pluginService,
	}
}
