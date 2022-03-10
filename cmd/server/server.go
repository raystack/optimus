package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpctags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/hashicorp/go-multierror"
	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/gossip"
	"github.com/odpf/optimus/datastore"
	_ "github.com/odpf/optimus/ext/datastore"
	"github.com/odpf/optimus/ext/executor/noop"
	"github.com/odpf/optimus/ext/notify/slack"
	"github.com/odpf/optimus/ext/scheduler/airflow"
	"github.com/odpf/optimus/ext/scheduler/airflow2"
	"github.com/odpf/optimus/ext/scheduler/airflow2/compiler"
	"github.com/odpf/optimus/ext/scheduler/prime"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	_ "github.com/odpf/optimus/plugin"
	"github.com/odpf/optimus/run"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store/postgres"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	"github.com/sirupsen/logrus"
	slackapi "github.com/slack-go/slack"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gorm.io/gorm"
)

// termChan listen for sigterm
var termChan = make(chan os.Signal, 1)

const (
	shutdownWait       = 30 * time.Second
	GRPCMaxRecvMsgSize = 64 << 20 // 64MB
	GRPCMaxSendMsgSize = 64 << 20 // 64MB
)

const (
	DialTimeout      = time.Second * 5
	BootstrapTimeout = time.Second * 10
)

func checkRequiredConfigs(conf config.ServerConfig) error {
	errRequiredMissing := errors.New("required config missing")
	if conf.IngressHost == "" {
		return fmt.Errorf("serve.ingress_host: %w", errRequiredMissing)
	}
	if conf.ReplayNumWorkers < 1 {
		return fmt.Errorf("%s should be greater than 0", config.KeyServeReplayNumWorkers)
	}
	if conf.DB.DSN == "" {
		return fmt.Errorf("serve.db.dsn: %w", errRequiredMissing)
	}
	if parsed, err := url.Parse(conf.DB.DSN); err != nil {
		return fmt.Errorf("failed to parse serve.db.dsn: %w", err)
	} else {
		if parsed.Scheme != "postgres" {
			return errors.New("unsupported database scheme, use 'postgres'")
		}
	}
	return nil
}

func Initialize(l log.Logger, conf config.Optimus) error {
	if err := checkRequiredConfigs(conf.Server); err != nil {
		return err
	}
	l.Info("starting optimus", "version", config.Version)
	progressObs := &pipelineLogObserver{
		log: l,
	}

	// used to encrypt secrets
	appHash, err := models.NewApplicationSecret(conf.Server.AppKey)
	if err != nil {
		return fmt.Errorf("NewApplicationSecret: %w", err)
	}

	dbConn, err := setupDB(l, conf)
	if err != nil {
		return err
	}

	jobrunRepoFac := &jobRunRepoFactory{
		db: dbConn,
	}

	// registered project store repository factory, it's a wrapper over a storage
	// interface
	projectRepoFac := &projectRepoFactory{
		db:   dbConn,
		hash: appHash,
	}

	projectSecretRepo := postgres.NewSecretRepository(dbConn, appHash)
	namespaceSpecRepoFac := &namespaceRepoFactory{
		db:   dbConn,
		hash: appHash,
	}
	projectJobSpecRepoFac := &projectJobSpecRepoFactory{
		db: dbConn,
	}

	err = initSchedulers(l, conf, jobrunRepoFac, projectRepoFac)
	if err != nil {
		return err
	}

	// services
	projectService := service.NewProjectService(projectRepoFac)
	namespaceService := service.NewNamespaceService(projectService, namespaceSpecRepoFac)
	secretService := service.NewSecretService(projectService, namespaceService, projectSecretRepo)

	// registered job store repository factory
	jobSpecRepoFac := jobSpecRepoFactory{
		db:                    dbConn,
		projectJobSpecRepoFac: *projectJobSpecRepoFac,
	}
	dependencyResolver := job.NewDependencyResolver(projectJobSpecRepoFac)
	priorityResolver := job.NewPriorityResolver()

	grpcAddr := fmt.Sprintf("%s:%d", conf.Server.Host, conf.Server.Port)
	grpcServer, err := setupGRPCServer(l)
	if err != nil {
		return err
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
		logger:            l,
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
		NumWorkers:    conf.Server.ReplayNumWorkers,
		WorkerTimeout: conf.Server.ReplayWorkerTimeout,
		RunTimeout:    conf.Server.ReplayRunTimeout,
	}, models.BatchScheduler, replayValidator, replaySyncer)
	backupRepoFac := backupRepoFactory{
		db: dbConn,
	}

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
			projectJobSpecRepoFac,
			replayManager,
		),
		eventService,
		datastore.NewService(&resourceSpecRepoFac, &projectResourceSpecRepoFac, models.DatastoreRegistry, utils.NewUUIDProvider(), &backupRepoFac),
		projectService,
		namespaceService,
		secretService,
		v1handler.NewAdapter(models.PluginRegistry, models.DatastoreRegistry),
		progressObs,
		run.NewService(
			jobrunRepoFac,
			secretService,
			func() time.Time {
				return time.Now().UTC()
			},
			run.NewGoEngine(),
		),
		models.BatchScheduler,
	))
	grpc_prometheus.Register(grpcServer)
	grpc_prometheus.EnableHandlingTimeHistogram(grpc_prometheus.WithHistogramBuckets(prometheus.DefBuckets))

	timeoutGrpcDialCtx, grpcDialCancel := context.WithTimeout(context.Background(), DialTimeout)
	defer grpcDialCancel()

	// prepare http proxy
	gwmux := runtime.NewServeMux(
		runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler),
	)
	// gRPC dialup options to proxy http connections
	grpcConn, err := grpc.DialContext(timeoutGrpcDialCtx, grpcAddr, []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(GRPCMaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(GRPCMaxSendMsgSize),
		),
	}...)
	if err != nil {
		return fmt.Errorf("grpc.DialContext: %w", err)
	}
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	defer runtimeCancel()
	if err := pb.RegisterRuntimeServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return fmt.Errorf("RegisterRuntimeServiceHandler: %w", err)
	}

	// base router
	baseMux := http.NewServeMux()
	baseMux.HandleFunc("/ping", otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "pong")
	}), "Ping").ServeHTTP)
	baseMux.Handle("/api/", otelhttp.NewHandler(http.StripPrefix("/api", gwmux), "api"))

	//nolint: gomnd
	srv := &http.Server{
		Handler:      grpcHandlerFunc(grpcServer, baseMux),
		Addr:         grpcAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 60 * time.Second,
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
	if conf.Scheduler.NodeID != "" {
		// start optimus cluster
		if err := clusterServer.Init(clusterCtx, conf.Scheduler); err != nil {
			clusterCancel()
			return err
		}

		clusterPlanner.Init(clusterCtx)
	}

	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	signal.Notify(termChan, os.Interrupt, syscall.SIGTERM)

	// Block until we receive our signal.
	<-termChan
	l.Info("termination request received")
	var terminalError error

	if err = replayManager.Close(); err != nil {
		terminalError = multierror.Append(terminalError, fmt.Errorf("replayManager.Close: %w", err))
	}

	// Create a deadline to wait for server
	ctxProxy, cancelProxy := context.WithTimeout(context.Background(), shutdownWait)
	defer cancelProxy()

	// Doesn't block if no connections, but will otherwise wait
	// until the timeout deadline.
	if err := srv.Shutdown(ctxProxy); err != nil {
		terminalError = multierror.Append(terminalError, fmt.Errorf("srv.Shutdown: %w", err))
	}
	grpcServer.GracefulStop()

	// gracefully shutdown event service, e.g. slack notifiers flush in memory batches
	cancelNotifiers()
	if err := eventService.Close(); err != nil {
		terminalError = multierror.Append(terminalError, fmt.Errorf("eventService.Close: %w", err))
	}

	// shutdown cluster
	clusterCancel()
	clusterPlanner.Close()
	clusterServer.Shutdown()

	sqlConn, err := dbConn.DB()
	if err != nil {
		terminalError = multierror.Append(terminalError, fmt.Errorf("dbConn.DB: %w", err))
	}
	if err := sqlConn.Close(); err != nil {
		terminalError = multierror.Append(terminalError, fmt.Errorf("sqlConn.Close: %w", err))
	}

	l.Info("bye")
	return terminalError
}

func setupGRPCServer(l log.Logger) (*grpc.Server, error) {
	// Logrus entry is used, allowing pre-definition of certain fields by the user.
	grpcLogLevel, err := logrus.ParseLevel(l.Level())
	if err != nil {
		return nil, err
	}
	grpcLogrus := logrus.New()
	grpcLogrus.SetLevel(grpcLogLevel)
	grpcLogrusEntry := logrus.NewEntry(grpcLogrus)
	// Shared options for the logger, with a custom gRPC code to log level function.
	opts := []grpc_logrus.Option{
		grpc_logrus.WithLevels(grpc_logrus.DefaultCodeToLevel),
	}
	// Make sure that log statements internal to gRPC library are logged using the logrus logger as well.
	grpc_logrus.ReplaceGrpcLogger(grpcLogrusEntry)

	grpcOpts := []grpc.ServerOption{
		grpc_middleware.WithUnaryServerChain(
			grpctags.UnaryServerInterceptor(grpctags.WithFieldExtractor(grpctags.CodeGenRequestFieldExtractor)),
			grpc_logrus.UnaryServerInterceptor(grpcLogrusEntry, opts...),
			otelgrpc.UnaryServerInterceptor(),
			grpc_prometheus.UnaryServerInterceptor,
		),
		grpc_middleware.WithStreamServerChain(
			otelgrpc.StreamServerInterceptor(),
			grpc_prometheus.StreamServerInterceptor,
		),
		grpc.MaxRecvMsgSize(GRPCMaxRecvMsgSize),
		grpc.MaxSendMsgSize(GRPCMaxSendMsgSize),
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	reflection.Register(grpcServer)
	return grpcServer, nil
}

func initSchedulers(l log.Logger, conf config.Optimus, jobrunRepoFac *jobRunRepoFactory, projectRepoFac *projectRepoFactory) error {
	jobCompiler := compiler.NewCompiler(conf.Server.IngressHost)
	// init default scheduler
	switch conf.Scheduler.Name {
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
		return fmt.Errorf("unsupported scheduler: %s", conf.Scheduler.Name)
	}

	models.ManualScheduler = prime.NewScheduler( // careful global variable
		jobrunRepoFac,
		func() time.Time {
			return time.Now().UTC()
		},
	)

	if !conf.Scheduler.SkipInit {
		registeredProjects, err := projectRepoFac.New().GetAll(context.Background())
		if err != nil {
			return fmt.Errorf("projectRepoFactory.GetAll(): %w", err)
		}
		// bootstrap scheduler for registered projects
		for _, proj := range registeredProjects {
			bootstrapCtx, cancel := context.WithTimeout(context.Background(), BootstrapTimeout)
			l.Info("bootstrapping project", "project name", proj.Name)
			if err := models.BatchScheduler.Bootstrap(bootstrapCtx, proj); err != nil { // careful global variable
				// Major ERROR, but we can't make this fatal
				// other projects might be working fine
				l.Error("no bootstrapping project", "error", err)
			}
			l.Info("bootstrapped project", "project name", proj.Name)
			cancel()
		}
	}
	return nil
}

func setupDB(l log.Logger, conf config.Optimus) (*gorm.DB, error) {
	// setup db
	if err := postgres.Migrate(conf.Server.DB.DSN); err != nil {
		return nil, fmt.Errorf("postgres.Migrate: %w", err)
	}
	dbConn, err := postgres.Connect(conf.Server.DB.DSN, conf.Server.DB.MaxIdleConnection,
		conf.Server.DB.MaxOpenConnection, l.Writer())
	if err != nil {
		return nil, fmt.Errorf("postgres.Connect: %w", err)
	}
	return dbConn, nil
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
