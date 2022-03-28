package server

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/odpf/salt/log"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	"gorm.io/gorm"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/ext/scheduler/airflow2"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/postgres"
)

// projectJobSpecRepoFactory stores raw specifications
type projectJobSpecRepoFactory struct {
	db *gorm.DB
}

func (fac *projectJobSpecRepoFactory) New(project models.ProjectSpec) store.ProjectJobSpecRepository {
	return postgres.NewProjectJobSpecRepository(fac.db, project, postgres.NewAdapter(models.PluginRegistry))
}

type replaySpecRepoRepository struct {
	db             *gorm.DB
	jobSpecRepoFac jobSpecRepoFactory
}

func (fac *replaySpecRepoRepository) New() store.ReplaySpecRepository {
	return postgres.NewReplayRepository(fac.db, postgres.NewAdapter(models.PluginRegistry))
}

type replayWorkerFact struct {
	replaySpecRepoFac job.ReplaySpecRepoFactory
	scheduler         models.SchedulerUnit
	logger            log.Logger
}

func (fac *replayWorkerFact) New() job.ReplayWorker {
	return job.NewReplayWorker(fac.logger, fac.replaySpecRepoFac, fac.scheduler)
}

// jobSpecRepoFactory stores raw specifications
type jobSpecRepoFactory struct {
	db                    *gorm.DB
	projectJobSpecRepoFac projectJobSpecRepoFactory
}

func (fac *jobSpecRepoFactory) New(namespace models.NamespaceSpec) job.SpecRepository {
	return postgres.NewJobSpecRepository(
		fac.db,
		namespace,
		fac.projectJobSpecRepoFac.New(namespace.ProjectSpec),
		postgres.NewAdapter(models.PluginRegistry),
	)
}

type projectRepoFactory struct {
	db   *gorm.DB
	hash models.ApplicationKey
}

func (fac *projectRepoFactory) New() store.ProjectRepository {
	return postgres.NewProjectRepository(fac.db, fac.hash)
}

type namespaceRepoFactory struct {
	db   *gorm.DB
	hash models.ApplicationKey
}

func (fac *namespaceRepoFactory) New(projectSpec models.ProjectSpec) store.NamespaceRepository {
	return postgres.NewNamespaceRepository(fac.db, projectSpec, fac.hash)
}

type jobRunRepoFactory struct {
	db *gorm.DB
}

func (fac *jobRunRepoFactory) New() store.JobRunRepository {
	return postgres.NewJobRunRepository(fac.db, postgres.NewAdapter(models.PluginRegistry))
}

type instanceRepoFactory struct {
	db *gorm.DB
}

func (fac *instanceRepoFactory) New() store.InstanceRepository {
	return postgres.NewInstanceRepository(fac.db, postgres.NewAdapter(models.PluginRegistry))
}

// projectResourceSpecRepoFactory stores raw resource specifications at a project level
type projectResourceSpecRepoFactory struct {
	db *gorm.DB
}

func (fac *projectResourceSpecRepoFactory) New(proj models.ProjectSpec, ds models.Datastorer) store.ProjectResourceSpecRepository {
	return postgres.NewProjectResourceSpecRepository(fac.db, proj, ds)
}

// resourceSpecRepoFactory stores raw resource specifications
type resourceSpecRepoFactory struct {
	db                         *gorm.DB
	projectResourceSpecRepoFac projectResourceSpecRepoFactory
}

func (fac *resourceSpecRepoFactory) New(namespace models.NamespaceSpec, ds models.Datastorer) store.ResourceSpecRepository {
	return postgres.NewResourceSpecRepository(fac.db, namespace, ds, fac.projectResourceSpecRepoFac.New(namespace.ProjectSpec, ds))
}

// backupRepoFactory stores backup specifications
type backupRepoFactory struct {
	db *gorm.DB
}

func (fac *backupRepoFactory) New(projectSpec models.ProjectSpec, storer models.Datastorer) store.BackupRepository {
	return postgres.NewBackupRepository(fac.db, projectSpec, storer)
}

type airflowBucketFactory struct{}

func (o *airflowBucketFactory) New(ctx context.Context, projectSpec models.ProjectSpec) (airflow2.Bucket, error) {
	storagePath, ok := projectSpec.Config[models.ProjectStoragePathKey]
	if !ok {
		return nil, fmt.Errorf("%s config not configured for project %s", models.ProjectStoragePathKey, projectSpec.Name)
	}
	parsedURL, err := url.Parse(storagePath)
	if err != nil {
		return nil, err
	}

	switch parsedURL.Scheme {
	case "gs":
		storageSecret, ok := projectSpec.Secret.GetByName(models.ProjectSecretStorageKey)
		if !ok {
			return nil, fmt.Errorf("%s secret not configured for project %s", models.ProjectSecretStorageKey, projectSpec.Name)
		}
		creds, err := google.CredentialsFromJSON(ctx, []byte(storageSecret), "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			return nil, err
		}
		client, err := gcp.NewHTTPClient(
			gcp.DefaultTransport(),
			gcp.CredentialsTokenSource(creds))
		if err != nil {
			return nil, err
		}

		gcsBucket, err := gcsblob.OpenBucket(ctx, client, parsedURL.Host, nil)
		if err != nil {
			return nil, err
		}
		// create a *blob.Bucket
		if parsedURL.Path == "" {
			return gcsBucket, nil
		}
		prefix := fmt.Sprintf("%s/", strings.Trim(parsedURL.Path, "/\\"))
		return blob.PrefixedBucket(gcsBucket, prefix), nil
	case "file":
		return fileblob.OpenBucket(parsedURL.Path, &fileblob.Options{
			CreateDir: true,
			Metadata:  fileblob.MetadataDontWrite,
		})
	case "mem":
		return memblob.OpenBucket(nil), nil
	}
	return nil, fmt.Errorf("unsupported storage config %s", storagePath)
}

type pipelineLogObserver struct {
	log log.Logger
}

func (obs *pipelineLogObserver) Notify(evt progress.Event) {
	obs.log.Info("observing pipeline log", "progress event", evt.String(), "reporter", "pipeline")
}

func jobSpecAssetDump(engine models.TemplateEngine) func(jobSpec models.JobSpec, scheduledAt time.Time) (models.JobAssets, error) {
	return func(jobSpec models.JobSpec, scheduledAt time.Time) (models.JobAssets, error) {
		aMap, err := compiler.DumpAssets(jobSpec, scheduledAt, engine, false)
		if err != nil {
			return models.JobAssets{}, err
		}
		return models.JobAssets{}.FromMap(aMap), nil
	}
}
