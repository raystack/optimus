package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	slackapi "github.com/slack-go/slack"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/datastore"
	"github.com/odpf/optimus/ext/notify/slack"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/run"
	"github.com/odpf/optimus/service"
	"github.com/odpf/optimus/store/postgres"
	"github.com/odpf/optimus/utils"
)

type setupFn func() error

type OptimusServer struct {
	conf   config.Optimus
	logger log.Logger

	appKey models.ApplicationKey
	dbConn *gorm.DB

	serverAddr string
	grpcServer *grpc.Server
	httpServer *http.Server

	shutdown     bool
	shutdownLock sync.Mutex
	cleanupFn    []func()
}

func New(l log.Logger, conf config.Optimus) (*OptimusServer, error) {
	if err := checkRequiredConfigs(conf.Server); err != nil {
		return nil, err
	}

	l.Info("Starting optimus", "version", config.Version)
	addr := fmt.Sprintf("%s:%d", conf.Server.Host, conf.Server.Port)
	server := &OptimusServer{
		conf:       conf,
		logger:     l,
		serverAddr: addr,
		shutdown:   false,
	}

	fns := []setupFn{
		server.setupAppKey,
		server.setupDB,
		server.setupGRPCServer,
		server.setupRuntimeServer,
		server.setupMonitoring,
		server.setupHTTPProxy,
		server.startListening,
	}

	for _, fn := range fns {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	return server, nil
}

func (s *OptimusServer) setupAppKey() error {
	var err error
	s.appKey, err = models.NewApplicationSecret(s.conf.Server.AppKey)
	if err != nil {
		return fmt.Errorf("NewApplicationSecret: %w", err)
	}
	return nil
}

func (s *OptimusServer) setupDB() error {
	var err error
	s.dbConn, err = setupDB(s.logger, s.conf)
	return err
}

func (s *OptimusServer) setupGRPCServer() error {
	var err error
	s.grpcServer, err = setupGRPCServer(s.logger)
	return err
}

func (s *OptimusServer) setupMonitoring() error {
	grpc_prometheus.Register(s.grpcServer)
	grpc_prometheus.EnableHandlingTimeHistogram(grpc_prometheus.WithHistogramBuckets(prometheus.DefBuckets))
	return nil
}

func (s *OptimusServer) setupHTTPProxy() error {
	srv, cleanup, err := prepareHTTPProxy(s.serverAddr, s.grpcServer)
	s.httpServer = srv
	s.cleanupFn = append(s.cleanupFn, cleanup)
	return err
}

func (s *OptimusServer) startListening() error {
	// run our server in a goroutine so that it doesn't block to wait for termination requests
	go func() {
		s.logger.Info("starting listening at", "address", s.serverAddr)
		if err := s.httpServer.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				s.logger.Fatal("server error", "error", err)
			}
		}
	}()
	return nil
}

func (s *OptimusServer) Shutdown() error {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()
	if s.shutdown {
		return nil // already shutting down
	}
	s.shutdown = true

	// Create a deadline to wait for server
	ctxProxy, cancelProxy := context.WithTimeout(context.Background(), shutdownWait)
	defer cancelProxy()

	if err := s.httpServer.Shutdown(ctxProxy); err != nil {
		s.logger.Error("Error in proxy shutdown", err)
	}
	s.grpcServer.GracefulStop()

	for _, fn := range s.cleanupFn {
		fn() // Todo: log all the errors from cleanup before exit
	}

	sqlConn, err := s.dbConn.DB()
	if err != nil {
		s.logger.Error("Error while getting sqlConn", err)
	} else if err := sqlConn.Close(); err != nil {
		s.logger.Error("Error in sqlConn.Close", err)
	}

	s.logger.Info("Server shutdown complete")

	return nil
}

func (s *OptimusServer) setupRuntimeServer() error {
	progressObs := &pipelineLogObserver{
		log: s.logger,
	}

	jobrunRepoFac := &jobRunRepoFactory{
		db: s.dbConn,
	}

	projectRepoFac := &projectRepoFactory{
		db:   s.dbConn,
		hash: s.appKey,
	}

	projectSecretRepo := postgres.NewSecretRepository(s.dbConn, s.appKey)
	namespaceSpecRepoFac := &namespaceRepoFactory{
		db:   s.dbConn,
		hash: s.appKey,
	}
	projectJobSpecRepoFac := &projectJobSpecRepoFactory{
		db: s.dbConn,
	}

	scheduler, err := initScheduler(s.logger, s.conf, projectRepoFac)
	if err != nil {
		return err
	}
	models.BatchScheduler = scheduler // TODO: remove global

	// services
	projectService := service.NewProjectService(projectRepoFac)
	namespaceService := service.NewNamespaceService(projectService, namespaceSpecRepoFac)
	secretService := service.NewSecretService(projectService, namespaceService, projectSecretRepo)

	// registered job store repository factory
	jobSpecRepoFac := jobSpecRepoFactory{
		db:                    s.dbConn,
		projectJobSpecRepoFac: *projectJobSpecRepoFac,
	}
	dependencyResolver := job.NewDependencyResolver(projectJobSpecRepoFac)
	priorityResolver := job.NewPriorityResolver()

	projectResourceSpecRepoFac := projectResourceSpecRepoFactory{
		db: s.dbConn,
	}
	resourceSpecRepoFac := resourceSpecRepoFactory{
		db:                         s.dbConn,
		projectResourceSpecRepoFac: projectResourceSpecRepoFac,
	}

	replaySpecRepoFac := &replaySpecRepoRepository{
		db:             s.dbConn,
		jobSpecRepoFac: jobSpecRepoFac,
	}
	replayWorkerFactory := &replayWorkerFact{
		replaySpecRepoFac: replaySpecRepoFac,
		scheduler:         scheduler,
		logger:            s.logger,
	}
	replayValidator := job.NewReplayValidator(scheduler)
	replaySyncer := job.NewReplaySyncer(
		s.logger,
		replaySpecRepoFac,
		projectRepoFac,
		scheduler,
		func() time.Time {
			return time.Now().UTC()
		},
	)

	replayManager := job.NewManager(s.logger, replayWorkerFactory, replaySpecRepoFac, utils.NewUUIDProvider(), job.ReplayManagerConfig{
		NumWorkers:    s.conf.Server.ReplayNumWorkers,
		WorkerTimeout: s.conf.Server.ReplayWorkerTimeout,
		RunTimeout:    s.conf.Server.ReplayRunTimeout,
	}, scheduler, replayValidator, replaySyncer)

	backupRepoFac := backupRepoFactory{
		db: s.dbConn,
	}

	notificationContext, cancelNotifiers := context.WithCancel(context.Background())
	s.cleanupFn = append(s.cleanupFn, cancelNotifiers)
	eventService := job.NewEventService(s.logger, map[string]models.Notifier{
		"slack": slack.NewNotifier(notificationContext, slackapi.APIURL,
			slack.DefaultEventBatchInterval,
			func(err error) {
				s.logger.Error("slack error accumulator", "error", err)
			},
		),
	})

	// runtime service instance over grpc
	manualScheduler := models.ManualScheduler
	jobService := job.NewService(
		&jobSpecRepoFac,
		scheduler,
		manualScheduler,
		jobSpecAssetDump(),
		dependencyResolver,
		priorityResolver,
		projectJobSpecRepoFac,
		replayManager,
	)
	jobRunService := run.NewService(
		jobrunRepoFac,
		secretService,
		func() time.Time {
			return time.Now().UTC()
		},
		run.NewGoEngine(),
	)
	pb.RegisterRuntimeServiceServer(s.grpcServer, v1handler.NewRuntimeServiceServer(
		s.logger,
		config.Version,
		jobService,
		eventService,
		datastore.NewService(&resourceSpecRepoFac, &projectResourceSpecRepoFac, models.DatastoreRegistry, utils.NewUUIDProvider(), &backupRepoFac),
		projectService,
		namespaceService,
		secretService,
		v1handler.NewAdapter(models.PluginRegistry, models.DatastoreRegistry),
		progressObs,
		jobRunService,
		scheduler,
	))

	cleanupCluster, err := initPrimeCluster(s.logger, s.conf, jobrunRepoFac, s.dbConn)
	if err != nil {
		return err
	}

	s.cleanupFn = append(s.cleanupFn, func() {
		replayManager.Close() // err is nil
	})
	s.cleanupFn = append(s.cleanupFn, cleanupCluster)
	s.cleanupFn = append(s.cleanupFn, func() {
		err = eventService.Close()
		if err != nil {
			s.logger.Error("Error while closing event service: %s", err)
		}
	})

	return nil
}
