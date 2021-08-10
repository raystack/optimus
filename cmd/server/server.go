package server

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpctags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/hashicorp/go-multierror"
	"github.com/jinzhu/gorm"
	v1 "github.com/odpf/optimus/api/handler/v1"
	v1handler "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/gossip"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/datastore"
	_ "github.com/odpf/optimus/ext/datastore"
	"github.com/odpf/optimus/ext/executor/noop"
	"github.com/odpf/optimus/ext/notify/slack"
	"github.com/odpf/optimus/ext/scheduler/airflow"
	"github.com/odpf/optimus/ext/scheduler/airflow2"
	"github.com/odpf/optimus/ext/scheduler/airflow2/compiler"
	"github.com/odpf/optimus/ext/scheduler/prime"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/meta"
	"github.com/odpf/optimus/models"
	_ "github.com/odpf/optimus/plugin"
	"github.com/odpf/optimus/run"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/postgres"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	slackapi "github.com/slack-go/slack"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/gcp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	//listen for sigterm
	termChan           = make(chan os.Signal, 1)
	shutdownWait       = 30 * time.Second
	GRPCMaxRecvMsgSize = 45 << 20 // 45MB
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
	jsonLog           log.Logger
}

func (fac *replayWorkerFact) New() job.ReplayWorker {
	return job.NewReplayWorker(fac.jsonLog, fac.replaySpecRepoFac, fac.scheduler)
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

type projectSecretRepoFactory struct {
	db   *gorm.DB
	hash models.ApplicationKey
}

func (fac *projectSecretRepoFactory) New(spec models.ProjectSpec) store.ProjectSecretRepository {
	return postgres.NewSecretRepository(fac.db, spec, fac.hash)
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

type airflowBucketFactory struct{}

func (o *airflowBucketFactory) New(ctx context.Context, projectSpec models.ProjectSpec) (airflow2.Bucket, error) {
	storagePath, ok := projectSpec.Config[models.ProjectStoragePathKey]
	if !ok {
		return nil, errors.Errorf("%s config not configured for project %s", models.ProjectStoragePathKey, projectSpec.Name)
	}

	parsedURL, err := url.Parse(storagePath)
	if err != nil {
		return nil, err
	}

	switch parsedURL.Scheme {
	case "gs":
		storageSecret, ok := projectSpec.Secret.GetByName(models.ProjectSecretStorageKey)
		if !ok {
			return nil, errors.Errorf("%s secret not configured for project %s", models.ProjectSecretStorageKey, projectSpec.Name)
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
	return nil, errors.Errorf("unsupported storage config %s", storagePath)
}

type metadataServiceFactory struct {
	writer *meta.Writer
}

func (factory *metadataServiceFactory) New() models.MetadataService {
	return meta.NewService(
		factory.writer,
		&meta.JobAdapter{},
	)
}

type pipelineLogObserver struct {
	log log.Logger
}

func (obs *pipelineLogObserver) Notify(evt progress.Event) {
	obs.log.Info("observing pipeline log", "progress event", evt.String(), "reporter", "pipeline")
}

func jobSpecAssetDump() func(jobSpec models.JobSpec, scheduledAt time.Time) (models.JobAssets, error) {
	engine := run.NewGoEngine()
	return func(jobSpec models.JobSpec, scheduledAt time.Time) (models.JobAssets, error) {
		aMap, err := run.DumpAssets(jobSpec, scheduledAt, engine, false)
		if err != nil {
			return models.JobAssets{}, err
		}
		return models.JobAssets{}.FromMap(aMap), nil
	}
}

func checkRequiredConfigs(conf config.Provider) error {
	errRequiredMissing := errors.New("required config missing")
	if conf.GetServe().IngressHost == "" {
		return errors.Wrap(errRequiredMissing, "serve.ingress_host")
	}
	if conf.GetServe().ReplayNumWorkers < 1 {
		return errors.New(fmt.Sprintf("%s should be greater than 0", config.KeyServeReplayNumWorkers))
	}
	if conf.GetServe().DB.DSN == "" {
		return errors.Wrap(errRequiredMissing, "serve.db.dsn")
	}
	if parsed, err := url.Parse(conf.GetServe().DB.DSN); err != nil {
		return errors.Wrap(err, "failed to parse serve.db.dsn")
	} else {
		if parsed.Scheme != "postgres" {
			return errors.New("unsupported database scheme, use 'postgres'")
		}
	}
	return nil
}

func Initialize(l log.Logger, conf config.Provider) error {
	if err := checkRequiredConfigs(conf); err != nil {
		return err
	}
	l.Info("starting optimus", "version", config.Version)
	progressObs := &pipelineLogObserver{
		log: l,
	}

	// setup db
	if err := postgres.Migrate(conf.GetServe().DB.DSN); err != nil {
		return errors.Wrap(err, "postgres.Migrate")
	}
	dbConn, err := postgres.Connect(conf.GetServe().DB.DSN, conf.GetServe().DB.MaxIdleConnection, conf.GetServe().DB.MaxOpenConnection)
	if err != nil {
		return errors.Wrap(err, "postgres.Connect")
	}

	jobCompiler := compiler.NewCompiler(conf.GetServe().IngressHost)
	// init default scheduler
	switch conf.GetScheduler().Name {
	case "airflow":
		models.BatchScheduler = airflow.NewScheduler(
			&airflowBucketFactory{},
			&http.Client{},
			jobCompiler,
		)
	case "airflow2":
		models.BatchScheduler = airflow2.NewScheduler(
			&airflowBucketFactory{},
			&http.Client{},
			jobCompiler,
		)
	default:
		return errors.Errorf("unsupported scheduler: %s", conf.GetScheduler().Name)
	}
	jobrunRepoFac := &jobRunRepoFactory{
		db: dbConn,
	}
	models.ManualScheduler = prime.NewScheduler(
		jobrunRepoFac,
		func() time.Time {
			return time.Now().UTC()
		},
	)

	// used to encrypt secrets
	appHash, err := models.NewApplicationSecret(conf.GetServe().AppKey)
	if err != nil {
		return errors.Wrap(err, "NewApplicationSecret")
	}

	// registered project store repository factory, it's a wrapper over a storage
	// interface
	projectRepoFac := &projectRepoFactory{
		db:   dbConn,
		hash: appHash,
	}
	if !conf.GetScheduler().SkipInit {
		registeredProjects, err := projectRepoFac.New().GetAll()
		if err != nil {
			return errors.Wrap(err, "projectRepoFactory.GetAll()")
		}
		// bootstrap scheduler for registered projects
		for _, proj := range registeredProjects {
			bootstrapCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			l.Info("bootstrapping project", "project name", proj.Name)
			if err := models.BatchScheduler.Bootstrap(bootstrapCtx, proj); err != nil {
				// Major ERROR, but we can't make this fatal
				// other projects might be working fine
				l.Error("no bootstrapping project", "error", err)
			}
			l.Info("bootstrapped project", "project name", proj.Name)
			cancel()
		}
	}

	projectSecretRepoFac := &projectSecretRepoFactory{
		db:   dbConn,
		hash: appHash,
	}
	namespaceSpecRepoFac := &namespaceRepoFactory{
		db:   dbConn,
		hash: appHash,
	}
	projectJobSpecRepoFac := projectJobSpecRepoFactory{
		db: dbConn,
	}

	// registered job store repository factory
	jobSpecRepoFac := jobSpecRepoFactory{
		db:                    dbConn,
		projectJobSpecRepoFac: projectJobSpecRepoFac,
	}
	dependencyResolver := job.NewDependencyResolver()
	priorityResolver := job.NewPriorityResolver()

	// Logrus entry is used, allowing pre-definition of certain fields by the user.
	logrusEntry := logrus.NewEntry(logrus.New())
	// Shared options for the logger, with a custom gRPC code to log level function.
	opts := []grpc_logrus.Option{
		grpc_logrus.WithLevels(grpc_logrus.DefaultCodeToLevel),
	}
	// Make sure that log statements internal to gRPC library are logged using the logrus Logger as well.
	grpc_logrus.ReplaceGrpcLogger(logrusEntry)

	grpcAddr := fmt.Sprintf("%s:%d", conf.GetServe().Host, conf.GetServe().Port)
	grpcOpts := []grpc.ServerOption{
		grpc_middleware.WithUnaryServerChain(
			grpctags.UnaryServerInterceptor(grpctags.WithFieldExtractor(grpctags.CodeGenRequestFieldExtractor)),
			grpc_logrus.UnaryServerInterceptor(logrusEntry, opts...),
		),
		grpc.MaxRecvMsgSize(GRPCMaxRecvMsgSize),
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	reflection.Register(grpcServer)

	// prepare factory writer for metadata
	var metaSvcFactory meta.MetaSvcFactory
	kafkaWriter := NewKafkaWriter(conf.GetServe().Metadata.KafkaJobTopic, strings.Split(conf.GetServe().Metadata.KafkaBrokers, ","), conf.GetServe().Metadata.KafkaBatchSize)
	l.Info("kafka metadata writer config received", "topic", conf.GetServe().Metadata.KafkaJobTopic, "brokers", conf.GetServe().Metadata.KafkaBrokers)
	if kafkaWriter != nil {
		l.Info("job metadata publishing is enabled", "topic", conf.GetServe().Metadata.KafkaJobTopic, "brokers", conf.GetServe().Metadata.KafkaBrokers)
		metaWriter := meta.NewWriter(kafkaWriter, conf.GetServe().Metadata.WriterBatchSize)
		defer kafkaWriter.Close()
		metaSvcFactory = &metadataServiceFactory{
			writer: metaWriter,
		}
	} else {
		l.Info("job metadata publishing is disabled")
	}

	projectResourceSpecRepoFac := projectResourceSpecRepoFactory{
		db: dbConn,
	}
	resourceSpecRepoFac := resourceSpecRepoFactory{
		db:                         dbConn,
		projectResourceSpecRepoFac: projectResourceSpecRepoFac,
	}

	replaySpecRepoFac := &replaySpecRepoRepository{
		db:             dbConn,
		jobSpecRepoFac: jobSpecRepoFac,
	}
	replayWorkerFactory := &replayWorkerFact{
		replaySpecRepoFac: replaySpecRepoFac,
		scheduler:         models.BatchScheduler,
	}
	replayValidator := job.NewReplayValidator(models.BatchScheduler)
	replaySyncer := job.NewReplaySyncer(
		l,
		replaySpecRepoFac,
		projectRepoFac,
		models.BatchScheduler,
		func() time.Time {
			return time.Now().UTC()
		},
	)
	replayManager := job.NewManager(l, replayWorkerFactory, replaySpecRepoFac, utils.NewUUIDProvider(), job.ReplayManagerConfig{
		NumWorkers:    conf.GetServe().ReplayNumWorkers,
		WorkerTimeout: conf.GetServe().ReplayWorkerTimeoutSecs,
		RunTimeout:    conf.GetServe().ReplayRunTimeoutSecs,
	}, models.BatchScheduler, replayValidator, replaySyncer)

	notificationContext, cancelNotifiers := context.WithCancel(context.Background())
	defer cancelNotifiers()
	eventService := job.NewEventService(l, map[string]models.Notifier{
		"slack": slack.NewNotifier(notificationContext, slackapi.APIURL,
			slack.DefaultEventBatchInterval,
			func(err error) {
				l.Error("slack error accumulator", "error", err)
			},
		),
	})

	// runtime service instance over grpc
	pb.RegisterRuntimeServiceServer(grpcServer, v1handler.NewRuntimeServiceServer(
		l,
		config.Version,
		job.NewService(
			&jobSpecRepoFac,
			models.BatchScheduler,
			models.ManualScheduler,
			jobSpecAssetDump(),
			dependencyResolver,
			priorityResolver,
			metaSvcFactory,
			&projectJobSpecRepoFac,
			replayManager,
		),
		eventService,
		datastore.NewService(&resourceSpecRepoFac, models.DatastoreRegistry),
		projectRepoFac,
		namespaceSpecRepoFac,
		projectSecretRepoFac,
		v1.NewAdapter(models.PluginRegistry, models.DatastoreRegistry),
		progressObs,
		run.NewService(
			jobrunRepoFac,
			func() time.Time {
				return time.Now().UTC()
			},
			run.NewGoEngine(),
		),
		models.BatchScheduler,
	))

	timeoutGrpcDialCtx, grpcDialCancel := context.WithTimeout(context.Background(), time.Second*5)
	defer grpcDialCancel()

	// prepare http proxy
	gwmux := runtime.NewServeMux(
		runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler),
	)
	// gRPC dialup options to proxy http connections
	grpcConn, err := grpc.DialContext(timeoutGrpcDialCtx, grpcAddr, []grpc.DialOption{
		grpc.WithInsecure(),
	}...)
	if err != nil {
		return errors.Wrap(err, "grpc.DialContext")
	}
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	defer runtimeCancel()
	if err := pb.RegisterRuntimeServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return errors.Wrap(err, "RegisterRuntimeServiceHandler")
	}

	// base router
	baseMux := http.NewServeMux()
	baseMux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "pong")
	})
	baseMux.Handle("/api/", http.StripPrefix("/api", gwmux))

	srv := &http.Server{
		Handler:      grpcHandlerFunc(grpcServer, baseMux),
		Addr:         grpcAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// run our server in a goroutine so that it doesn't block to wait for termination requests
	go func() {
		l.Info("starting listening at", "address", grpcAddr)
		if err := srv.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				l.Fatal("server error", "error", err)
			}
		}
	}()

	clusterCtx, clusterCancel := context.WithCancel(context.Background())
	clusterServer := gossip.NewServer(l)
	clusterPlanner := prime.NewPlanner(
		l,
		clusterServer, jobrunRepoFac, &instanceRepoFactory{
			db: dbConn,
		},
		utils.NewUUIDProvider(), noop.NewExecutor(), func() time.Time {
			return time.Now().UTC()
		},
	)
	if conf.GetScheduler().NodeID != "" {
		// start optimus cluster
		if err := clusterServer.Init(clusterCtx, conf.GetScheduler()); err != nil {
			clusterCancel()
			return err
		}

		if err := clusterPlanner.Init(clusterCtx); err != nil {
			clusterCancel()
			return err
		}
	}

	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	signal.Notify(termChan, os.Interrupt)
	signal.Notify(termChan, os.Kill)
	signal.Notify(termChan, syscall.SIGTERM)

	// Block until we receive our signal.
	<-termChan
	l.Info("termination request received")
	var terminalError error

	if err = replayManager.Close(); err != nil {
		terminalError = multierror.Append(terminalError, errors.Wrap(err, "replayManager.Close"))
	}

	// Create a deadline to wait for server
	ctxProxy, cancelProxy := context.WithTimeout(context.Background(), shutdownWait)
	defer cancelProxy()

	// Doesn't block if no connections, but will otherwise wait
	// until the timeout deadline.
	if err := srv.Shutdown(ctxProxy); err != nil {
		terminalError = multierror.Append(terminalError, errors.Wrap(err, "srv.Shutdown"))
	}
	grpcServer.GracefulStop()

	// gracefully shutdown event service, e.g. slack notifiers flush in memory batches
	cancelNotifiers()
	if err := eventService.Close(); err != nil && len(err.Error()) != 0 {
		terminalError = multierror.Append(terminalError, errors.Wrap(err, "eventService.Close"))
	}

	// shutdown cluster
	clusterCancel()
	clusterPlanner.Close()
	clusterServer.Shutdown()

	l.Info("bye")
	return terminalError
}

// grpcHandlerFunc routes http1 calls to baseMux and http2 with grpc header to grpcServer.
// Using a single port for proxying both http1 & 2 protocols will degrade http performance
// but for our use-case the convenience per performance tradeoff is better suited
// if in the future, this does become a bottleneck(which I highly doubt), we can break the service
// into two ports, default port for grpc and default+1 for grpc-gateway proxy.
// We can also use something like a connection multiplexer
// https://github.com/soheilhy/cmux to achieve the same.
func grpcHandlerFunc(grpcServer *grpc.Server, otherHandler http.Handler) http.Handler {
	return h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
		} else {
			otherHandler.ServeHTTP(w, r)
		}
	}), &http2.Server{})
}

// NewKafkaWriter creates a new kafka client that will be used for meta publishing
func NewKafkaWriter(topic string, brokers []string, batchSize int) *kafka.Writer {
	// check if metadata publisher is disabled
	if len(brokers) == 0 || (len(brokers) == 1 && (brokers[0] == "-" || brokers[0] == "")) {
		return nil
	}

	return kafka.NewWriter(kafka.WriterConfig{
		Topic:     topic,
		Brokers:   brokers,
		BatchSize: batchSize,
	})
}
