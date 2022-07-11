package server

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/odpf/salt/log"
	"go.opentelemetry.io/otel"
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

type replayWorkerFact struct {
	replaySpecRepoFac store.ReplaySpecRepository
	scheduler         models.SchedulerUnit
	logger            log.Logger
}

func (fac *replayWorkerFact) New() job.ReplayWorker {
	return job.NewReplayWorker(fac.logger, fac.replaySpecRepoFac, fac.scheduler)
}

// namespaceJobSpecRepoFactory stores raw specifications
type namespaceJobSpecRepoFactory struct {
	db                    *gorm.DB
	projectJobSpecRepoFac projectJobSpecRepoFactory
}

func (fac *namespaceJobSpecRepoFactory) New(namespace models.NamespaceSpec) store.NamespaceJobSpecRepository {
	return postgres.NewNamespaceJobSpecRepository(
		fac.db,
		namespace,
		fac.projectJobSpecRepoFac.New(namespace.ProjectSpec),
		postgres.NewAdapter(models.PluginRegistry),
	)
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

type airflowBucketFactory struct{}

func (*airflowBucketFactory) New(ctx context.Context, projectSpec models.ProjectSpec) (airflow2.Bucket, error) {
	spanCtx, span := otel.Tracer("server/bucketFactory").Start(ctx, "NewBucketFactory")
	defer span.End()

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
		span.AddEvent("Init bucket for GCS")
		storageSecret, ok := projectSpec.Secret.GetByName(models.ProjectSecretStorageKey)
		if !ok {
			return nil, fmt.Errorf("%s secret not configured for project %s", models.ProjectSecretStorageKey, projectSpec.Name)
		}
		creds, err := google.CredentialsFromJSON(spanCtx, []byte(storageSecret), "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			return nil, err
		}
		client, err := gcp.NewHTTPClient(
			gcp.DefaultTransport(),
			gcp.CredentialsTokenSource(creds))
		if err != nil {
			return nil, err
		}

		gcsBucket, err := gcsblob.OpenBucket(spanCtx, client, parsedURL.Host, nil)
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
		span.AddEvent("Init bucket for File")
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
