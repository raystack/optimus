package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/odpf/optimus/config"

	"github.com/hashicorp/go-hclog"

	"github.com/odpf/optimus/plugin"

	hplugin "github.com/hashicorp/go-plugin"

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
	"github.com/odpf/optimus/resources"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/gcs"
	"github.com/odpf/optimus/store/postgres"
)

var (
	//listen for sigterm
	termChan = make(chan os.Signal, 1)

	shutdownWait = 30 * time.Second

	GRPCMaxRecvMsgSize = 25 << 20 // 25MB
)

// Config for the service
var Config = struct {
	ServerPort          string
	ServerHost          string
	LogLevel            string
	DBHost              string
	DBUser              string
	DBPassword          string
	DBName              string
	DBSSLMode           string
	MaxIdleDBConn       string
	MaxOpenDBConn       string
	IngressHost         string
	AppKey              string
	KafkaJobTopic       string
	KafkaBrokers        string
	KafkaBatchSize      string
	MetaWriterBatchSize string
}{
	ServerPort:          "9100",
	ServerHost:          "0.0.0.0",
	LogLevel:            "DEBUG",
	MaxIdleDBConn:       "5",
	MaxOpenDBConn:       "10",
	DBSSLMode:           "disable",
	DBPassword:          "-",
	KafkaJobTopic:       "resource_optimus_job_log",
	KafkaBatchSize:      "50",
	MetaWriterBatchSize: "50",
	KafkaBrokers:        "-",
}

func lookupEnvOrString(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// cfg defines an input parameter to the service
type cfg struct {
	Env, Cmd, Desc string
}

// cfgRules define how input parameters map to local
// configuration variables
var cfgRules = map[*string]cfg{
	&Config.ServerPort: {
		Env:  "SERVER_PORT",
		Cmd:  "server-port",
		Desc: "port to listen on",
	},
	&Config.ServerHost: {
		Env:  "SERVER_HOST",
		Cmd:  "server-host",
		Desc: "the network interface to listen on",
	},
	&Config.LogLevel: {
		Env:  "LOG_LEVEL",
		Cmd:  "log-level",
		Desc: "log level - DEBUG, INFO, WARNING, ERROR, FATAL",
	},
	&Config.DBHost: {
		Env:  "DB_HOST",
		Cmd:  "db-host",
		Desc: "database host to connect to database",
	},
	&Config.DBUser: {
		Env:  "DB_USER",
		Cmd:  "db-user",
		Desc: "database user to connect to database",
	},
	&Config.DBPassword: {
		Env:  "DB_PASSWORD",
		Cmd:  "db-password",
		Desc: "database password to connect to database",
	},
	&Config.DBName: {
		Env:  "DB_NAME",
		Cmd:  "db-name",
		Desc: "database name to connect to database",
	},
	&Config.DBSSLMode: {
		Env:  "DB_SSL_MODE",
		Cmd:  "db-ssl-mode",
		Desc: "database sslmode to connect to database (require, disable)",
	},
	&Config.MaxIdleDBConn: {
		Env:  "MAX_IDLE_DB_CONN",
		Cmd:  "max-idle-db-conn",
		Desc: "maximum allowed idle DB connections",
	},
	&Config.IngressHost: {
		Env:  "INGRESS_HOST",
		Cmd:  "ingress-host",
		Desc: "service ingress host for jobs to communicate back to optimus",
	},
	&Config.AppKey: {
		Env:  "APP_KEY",
		Cmd:  "app-key",
		Desc: "random 32 character hash used for encrypting secrets",
	},
	&Config.KafkaJobTopic: {
		Env:  "KAFKA_JOB_TOPIC",
		Cmd:  "kafka-job-topic",
		Desc: "kafka topic where metadata of optimus Job needs to be published",
	},
	&Config.KafkaBrokers: {
		Env:  "KAFKA_BROKERS",
		Cmd:  "kafka-brokers",
		Desc: "comma separated kafka brokers to use for publishing metadata, leave empty to disable metadata publisher",
	},
	&Config.KafkaBatchSize: {
		Env:  "KAFKA_BATCH_SIZE",
		Cmd:  "kafka-batch-size",
		Desc: "limit on how many messages will be buffered before being sent to a kafka partition.",
	},
	&Config.MetaWriterBatchSize: {
		Env:  "META_WRITER_BATCH_SIZE",
		Cmd:  "meta-writer-batch-size",
		Desc: "limit on how many messages will be buffered before being sent to a writer.",
	},
}

func validateConfig() error {
	var errs []string
	for v, cfg := range cfgRules {
		if strings.TrimSpace(*v) == "" {
			errs = append(
				errs,
				fmt.Sprintf(
					"missing required parameter: -%s (can also be set using %s environment variable)",
					cfg.Cmd,
					cfg.Env,
				),
			)
		}
		if *v == "-" { // "- is used for empty arguments"
			*v = ""
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "\n"))
	}
	return nil
}

// projectJobSpecRepoFactory stores raw specifications
type projectJobSpecRepoFactory struct {
	db *gorm.DB
}

func (fac *projectJobSpecRepoFactory) New(project models.ProjectSpec) store.ProjectJobSpecRepository {
	return postgres.NewProjectJobRepository(fac.db, project, postgres.NewAdapter(models.TaskRegistry, models.HookRegistry))
}

// jobSpecRepoFactory stores raw specifications
type jobSpecRepoFactory struct {
	db                    *gorm.DB
	projectJobSpecRepoFac projectJobSpecRepoFactory
}

func (fac *jobSpecRepoFactory) New(namespace models.NamespaceSpec) store.JobSpecRepository {
	return postgres.NewJobRepository(
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

func init() {
	for v, cfg := range cfgRules {
		flag.StringVar(v, cfg.Cmd, lookupEnvOrString(cfg.Env, *v), cfg.Desc)
	}
	flag.Parse()
}

func main() {
	log := logrus.New()
	log.SetOutput(os.Stdout)
	logger.Init(Config.LogLevel)

	mainLog := log.WithField("reporter", "main")
	mainLog.Infof("starting optimus %s", config.Version)

	err := validateConfig()
	if err != nil {
		mainLog.Fatalf("configuration error:\n%v", err)
	}

	// Create an hclog.Logger
	pluginLogLevel := hclog.Info
	if Config.LogLevel == "DEBUG" {
		pluginLogLevel = hclog.Debug
	}
	pluginLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "optimus",
		Output: os.Stdout,
		Level:  pluginLogLevel,
	})
	plugin.Initialize(pluginLogger)
	// Make sure we clean up any managed plugins at the end of this
	defer hplugin.CleanupClients()

	progressObs := &pipelineLogObserver{
		log: log.WithField("reporter", "pipeline"),
	}

	// setup db
	maxIdleConnection, _ := strconv.Atoi(Config.MaxIdleDBConn)
	maxOpenConnection, _ := strconv.Atoi(Config.MaxOpenDBConn)
	databaseURL := fmt.Sprintf("postgres://%s:%s@%s:5432/%s?sslmode=%s", Config.DBUser, url.QueryEscape(Config.DBPassword), Config.DBHost, Config.DBName, Config.DBSSLMode)
	if err := postgres.Migrate(databaseURL); err != nil {
		panic(err)
	}
	dbConn, err := postgres.Connect(databaseURL, maxIdleConnection, maxOpenConnection)
	if err != nil {
		panic(err)
	}

	// init default scheduler, should be configurable by user configs later
	models.Scheduler = airflow2.NewScheduler(
		resources.FileSystem,
		&objectWriterFactory{},
		&http.Client{},
	)

	appHash, err := models.NewApplicationSecret(Config.AppKey)
	if err != nil {
		panic(err)
	}

	// registered project store repository factory, its a wrapper over a storage
	// interface
	projectRepoFac := &projectRepoFactory{
		db:   dbConn,
		hash: appHash,
	}
	registeredProjects, err := projectRepoFac.New().GetAll()
	if err != nil {
		panic(err)
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
	jobCompiler := job.NewCompiler(resources.FileSystem, models.Scheduler.GetTemplatePath(), Config.IngressHost)
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

	serverPort, err := strconv.Atoi(Config.ServerPort)
	if err != nil {
		panic("invalid server port")
	}
	grpcAddr := fmt.Sprintf("%s:%d", Config.ServerHost, serverPort)
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
	kafkaBatchSize, err := strconv.Atoi(Config.KafkaBatchSize)
	if err != nil {
		mainLog.Fatalf("error reading kafka batch size: %v", err)
	}
	writerBatchSize, err := strconv.Atoi(Config.MetaWriterBatchSize)
	if err != nil {
		mainLog.Fatalf("error reading writer batch size: %v", err)
	}
	kafkaWriter := NewKafkaWriter(Config.KafkaJobTopic, strings.Split(Config.KafkaBrokers, ","), kafkaBatchSize)
	if kafkaWriter != nil {
		mainLog.Infof("job metadata publishing is enabled with brokers %s to topic %s", Config.KafkaBrokers, Config.KafkaJobTopic)
		metaWriter := meta.NewWriter(kafkaWriter, writerBatchSize)
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
		),
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
		panic(fmt.Errorf("Fail to dial: %v", err))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := pb.RegisterRuntimeServiceHandler(ctx, gwmux, grpcConn); err != nil {
		panic(err)
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

	// run our server in a goroutine so that it doesn't block.
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

	// Create a deadline to wait for server
	ctxProxy, cancelProxy := context.WithTimeout(context.Background(), shutdownWait)
	defer cancelProxy()

	// Doesn't block if no connections, but will otherwise wait
	// until the timeout deadline.
	if err := srv.Shutdown(ctxProxy); err != nil {
		mainLog.Warn(err)
	}
	grpcServer.GracefulStop()

	mainLog.Info("bye")
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
