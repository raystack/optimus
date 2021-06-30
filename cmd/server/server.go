package server

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/odpf/optimus/ext/notify/slack"

	"github.com/odpf/optimus/utils"

	"github.com/odpf/optimus/ext/scheduler/airflow"

	"github.com/odpf/optimus/config"

	"github.com/odpf/optimus/datastore"
	"github.com/odpf/optimus/meta"
	"github.com/segmentio/kafka-go"

	"google.golang.org/api/option"

	"cloud.google.com/go/storage"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpctags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	slackapi "github.com/slack-go/slack"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	v1 "github.com/odpf/optimus/api/handler/v1"
	v1handler "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/core/logger"
	"github.com/odpf/optimus/core/progress"
	_ "github.com/odpf/optimus/ext/datastore"
	"github.com/odpf/optimus/ext/scheduler/airflow2"
	"github.com/odpf/optimus/instance"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	_ "github.com/odpf/optimus/plugin"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/gcs"
	"github.com/odpf/optimus/store/postgres"
)

var (
	//listen for sigterm
	termChan = make(chan os.Signal, 1)

	shutdownWait = 30 * time.Second

	GRPCMaxRecvMsgSize = 45 << 20 // 45MB
)

// projectJobSpecRepoFactory stores raw specifications
type projectJobSpecRepoFactory struct {
	db *gorm.DB
}

func (fac *projectJobSpecRepoFactory) New(project models.ProjectSpec) store.ProjectJobSpecRepository {
	return postgres.NewProjectJobSpecRepository(fac.db, project, postgres.NewAdapter(models.TaskRegistry, models.HookRegistry))
}

type replaySpecRepoRepository struct {
	db             *gorm.DB
	jobSpecRepoFac jobSpecRepoFactory
}

func (fac *replaySpecRepoRepository) New(job models.JobSpec) store.ReplaySpecRepository {
	return postgres.NewReplayRepository(fac.db, job)
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
		postgres.NewAdapter(models.TaskRegistry, models.HookRegistry),
	)
}

// jobRepoFactory stores compiled specifications that will be consumed by a
// scheduler
type jobRepoFactory struct {
	schd models.SchedulerUnit
}

func (fac *jobRepoFactory) New(ctx context.Context, proj models.ProjectSpec) (store.JobRepository, error) {
	storagePath, ok := proj.Config[models.ProjectStoragePathKey]
	if !ok {
		return nil, errors.Errorf("%s not configured for project %s", models.ProjectStoragePathKey, proj.Name)
	}
	storageSecret, ok := proj.Secret.GetByName(models.ProjectSecretStorageKey)
	if !ok {
		return nil, errors.Errorf("%s secret not configured for project %s", models.ProjectSecretStorageKey, proj.Name)
	}

	p, err := url.Parse(storagePath)
	if err != nil {
		return nil, err
	}
	switch p.Scheme {
	case "gs":
		storageClient, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(storageSecret)))
		if err != nil {
			return nil, errors.Wrap(err, "error creating google storage client")
		}
		return gcs.NewJobRepository(p.Hostname(), filepath.Join(p.Path, fac.schd.GetJobsDir()), fac.schd.GetJobsExtension(), storageClient), nil
	}
	return nil, errors.Errorf("unsupported storage config %s in %s of project %s", storagePath, models.ProjectStoragePathKey, proj.Name)
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

type instanceRepoFactory struct {
	db *gorm.DB
}

func (fac *instanceRepoFactory) New(spec models.JobSpec) store.InstanceSpecRepository {
	return postgres.NewInstanceRepository(fac.db, spec, postgres.NewAdapter(models.TaskRegistry, models.HookRegistry))
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

type objectWriterFactory struct {
}

func (o *objectWriterFactory) New(ctx context.Context, writerPath, writerSecret string) (store.ObjectWriter, error) {
	p, err := url.Parse(writerPath)
	if err != nil {
		return nil, err
	}

	switch p.Scheme {
	case "gs":
		gcsClient, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(writerSecret)))
		if err != nil {
			return nil, errors.Wrap(err, "error creating google storage client")
		}
		return &gcs.GcsObjectWriter{
			Client: gcsClient,
		}, nil
	}
	return nil, errors.Errorf("unsupported storage config %s", writerPath)
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
	log logrus.FieldLogger
}

func (obs *pipelineLogObserver) Notify(evt progress.Event) {
	obs.log.Info(evt)
}

func jobSpecAssetDump() func(jobSpec models.JobSpec, scheduledAt time.Time) (models.JobAssets, error) {
	engine := instance.NewGoEngine()
	return func(jobSpec models.JobSpec, scheduledAt time.Time) (models.JobAssets, error) {
		aMap, err := instance.DumpAssets(jobSpec, scheduledAt, engine, false)
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

func Initialize(conf config.Provider) error {
	if err := checkRequiredConfigs(conf); err != nil {
		return err
	}

	log := logrus.New()
	if loglevel, err := logrus.ParseLevel(conf.GetLog().Level); err == nil {
		log.Level = loglevel
	}
	log.SetOutput(os.Stdout)
	logger.Init(conf.GetLog().Level)

	mainLog := log.WithField("reporter", "main")
	mainLog.Infof("starting optimus %s", config.Version)

	progressObs := &pipelineLogObserver{
		log: log.WithField("reporter", "pipeline"),
	}

	// setup db
	if err := postgres.Migrate(conf.GetServe().DB.DSN); err != nil {
		return errors.Wrap(err, "postgres.Migrate")
	}
	dbConn, err := postgres.Connect(conf.GetServe().DB.DSN, conf.GetServe().DB.MaxIdleConnection, conf.GetServe().DB.MaxOpenConnection)
	if err != nil {
		return errors.Wrap(err, "postgres.Connect")
	}

	// init default scheduler
	switch conf.GetScheduler().Name {
	case "airflow":
		models.Scheduler = airflow.NewScheduler(
			&objectWriterFactory{},
			&http.Client{},
		)
	case "airflow2":
		models.Scheduler = airflow2.NewScheduler(
			&objectWriterFactory{},
			&http.Client{},
		)
	default:
		return errors.Errorf("unsupported scheduler: %s", conf.GetScheduler().Name)
	}

	// used to encrypt secrets
	appHash, err := models.NewApplicationSecret(conf.GetServe().AppKey)
	if err != nil {
		return errors.Wrap(err, "NewApplicationSecret")
	}

	// registered project store repository factory, its a wrapper over a storage
	// interface
	projectRepoFac := &projectRepoFactory{
		db:   dbConn,
		hash: appHash,
	}
	registeredProjects, err := projectRepoFac.New().GetAll()
	if err != nil {
		return errors.Wrap(err, "projectRepoFactory.GetAll()")
	}
	// bootstrap scheduler for registered projects
	for _, proj := range registeredProjects {
		func() {
			bootstrapCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			logger.I("bootstrapping project ", proj.Name)
			if err := models.Scheduler.Bootstrap(bootstrapCtx, proj); err != nil {
				// Major ERROR, but we can't make this fatal
				// other projects might be working fine though
				logger.E(err)
			}
			logger.I("bootstrapped project ", proj.Name)
		}()
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
	jobCompiler := job.NewCompiler(models.Scheduler.GetTemplate(), conf.GetServe().IngressHost)
	dependencyResolver := job.NewDependencyResolver()
	priorityResolver := job.NewPriorityResolver()

	// Logrus entry is used, allowing pre-definition of certain fields by the user.
	logrusEntry := logrus.NewEntry(log)
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
	mainLog.WithFields(logrus.Fields{
		"topic":   conf.GetServe().Metadata.KafkaJobTopic,
		"brokers": conf.GetServe().Metadata.KafkaBrokers,
	}).Debug("kafka metadata writer config received")
	if kafkaWriter != nil {
		mainLog.Infof("job metadata publishing is enabled with brokers %s to topic %s", conf.GetServe().Metadata.KafkaBrokers, conf.GetServe().Metadata.KafkaJobTopic)
		metaWriter := meta.NewWriter(kafkaWriter, conf.GetServe().Metadata.WriterBatchSize)
		defer kafkaWriter.Close()
		metaSvcFactory = &metadataServiceFactory{
			writer: metaWriter,
		}
	} else {
		mainLog.Info("job metadata publishing is disabled")
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
	replayWorker := job.NewReplayWorker(replaySpecRepoFac, models.Scheduler)
	replayManager := job.NewManager(replayWorker, replaySpecRepoFac, utils.NewUUIDProvider(), job.ReplayManagerConfig{
		NumWorkers:    conf.GetServe().ReplayNumWorkers,
		WorkerTimeout: conf.GetServe().ReplayWorkerTimeoutSecs,
	})

	notificationContext, cancelNotifiers := context.WithCancel(context.Background())
	defer cancelNotifiers()
	eventService := job.NewEventService(map[string]models.Notifier{
		"slack": slack.NewNotifier(notificationContext, slackapi.APIURL,
			slack.DefaultEventBatchInterval,
			func(err error) {
				logger.E(err)
			},
		),
	})

	// runtime service instance over grpc
	pb.RegisterRuntimeServiceServer(grpcServer, v1handler.NewRuntimeServiceServer(
		config.Version,
		job.NewService(
			&jobSpecRepoFac,
			&jobRepoFactory{
				schd: models.Scheduler,
			},
			jobCompiler,
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
		v1.NewAdapter(models.TaskRegistry, models.HookRegistry, models.DatastoreRegistry),
		progressObs,
		instance.NewService(
			&instanceRepoFactory{
				db: dbConn,
			},
			func() time.Time {
				return time.Now().UTC()
			},
			instance.NewGoEngine(),
		),
		models.Scheduler,
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
	baseMux.HandleFunc("/ping", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "pong")
	}))
	baseMux.Handle("/", gwmux)

	srv := &http.Server{
		Handler:      grpcHandlerFunc(grpcServer, baseMux),
		Addr:         grpcAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// run our server in a goroutine so that it doesn't block to wait for termination requests
	go func() {
		mainLog.Infoln("starting listening at ", grpcAddr)
		if err := srv.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				mainLog.Fatalf("server error: %v\n", err)
			}
		}
	}()

	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	signal.Notify(termChan, os.Interrupt)
	signal.Notify(termChan, os.Kill)
	signal.Notify(termChan, syscall.SIGTERM)

	// Block until we receive our signal.
	<-termChan
	mainLog.Info("termination request received")
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

	mainLog.Info("bye")

	return terminalError
}

// grpcHandlerFunc routes http1 calls to baseMux and http2 with grpc header to grpcServer.
// Using a single port for proxying both http1 & 2 protocols will degrade http performance
// but for our usecase the convenience per performance tradeoff is better suited
// if in future, this does become a bottleneck(which I highly doubt), we can break the service
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
