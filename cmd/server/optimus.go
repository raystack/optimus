package server

import (
	"context"
	"fmt"
	"net/http"
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

	cleanupFn []func()
}

func New(l log.Logger, conf config.Optimus) (*OptimusServer, error) {
	if err := checkRequiredConfigs(conf.Server); err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%s:%d", conf.Server.Host, conf.Server.Port)
	server := &OptimusServer{
		conf:       conf,
		logger:     l,
		serverAddr: addr,
	}

	fns := []setupFn{
		server.setupAppKey,
		server.setupDB,
		server.setupGRPCServer,
		server.setupHandlers,
		server.setupMonitoring,
		server.setupHTTPProxy,
		server.startListening,
	}

	for _, fn := range fns {
		if err := fn(); err != nil {
			return server, err
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
	if err := postgres.Migrate(s.conf.Server.DB.DSN); err != nil {
		return fmt.Errorf("postgres.Migrate: %w", err)
	}
	// TODO: Connect should accept DBConfig
	s.dbConn, err = postgres.Connect(s.conf.Server.DB.DSN, s.conf.Server.DB.MaxIdleConnection,
		s.conf.Server.DB.MaxOpenConnection, s.logger.Writer())
	if err != nil {
		return fmt.Errorf("postgres.Connect: %w", err)
	}

	return nil
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
		s.logger.Info("Listening at", "address", s.serverAddr)
		if err := s.httpServer.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				s.logger.Fatal("server error", "error", err)
			}
		}
	}()
	return nil
}

func (s *OptimusServer) Shutdown() {
	if s.httpServer != nil {
		// Create a deadline to wait for server
		ctxProxy, cancelProxy := context.WithTimeout(context.Background(), shutdownWait)
		defer cancelProxy()

		if err := s.httpServer.Shutdown(ctxProxy); err != nil {
			s.logger.Error("Error in proxy shutdown", err)
		}
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

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
}

func (s *OptimusServer) setupHandlers() error {
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

	jobrunRepoFac := &jobRunRepoFactory{
		db: s.dbConn,
	}
	// job run service
	jobRunService := run.NewService(
		jobrunRepoFac,
		secretService,
		func() time.Time {
			return time.Now().UTC()
		},
		models.BatchScheduler,
		run.NewGoEngine(),
	)

	progressObs := &pipelineLogObserver{
		log: s.logger,
	}

	projectResourceSpecRepoFac := projectResourceSpecRepoFactory{
		db: s.dbConn,
	}
	resourceSpecRepoFac := resourceSpecRepoFactory{
		db:                         s.dbConn,
		projectResourceSpecRepoFac: projectResourceSpecRepoFac,
	}
	backupRepoFac := backupRepoFactory{
		db: s.dbConn,
	}
	dataStoreService := datastore.NewService(&resourceSpecRepoFac, &projectResourceSpecRepoFac, models.DatastoreRegistry, utils.NewUUIDProvider(), &backupRepoFac)
	// adapter service
	adapterService := v1handler.NewAdapter(models.PluginRegistry, models.DatastoreRegistry)

	// secret service
	pb.RegisterSecretServiceServer(s.grpcServer, v1handler.NewSecretServiceServer(s.logger, secretService))
	// resource service
	pb.RegisterResourceServiceServer(s.grpcServer, v1handler.NewResourceServiceServer(s.logger,
		dataStoreService,
		namespaceService,
		adapterService,
		progressObs))
	// replay service
	pb.RegisterReplayServiceServer(s.grpcServer, v1handler.NewReplayServiceServer(s.logger,
		jobService,
		namespaceService,
		adapterService,
		projectService))
	// project service
	pb.RegisterProjectServiceServer(s.grpcServer, v1handler.NewProjectServiceServer(s.logger,
		adapterService,
		projectService))
	// namespace service
	pb.RegisterNamespaceServiceServer(s.grpcServer, v1handler.NewNamespaceServiceServer(s.logger,
		adapterService,
		namespaceService))
	// job Spec service
	pb.RegisterJobSpecificationServiceServer(s.grpcServer, v1handler.NewJobSpecServiceServer(s.logger,
		jobService,
		adapterService,
		namespaceService,
		progressObs))
	// job run service
	pb.RegisterJobRunServiceServer(s.grpcServer, v1handler.NewJobRunServiceServer(s.logger,
		jobService,
		projectService,
		namespaceService,
		adapterService,
		jobRunService,
		models.BatchScheduler))
	// backup service
	pb.RegisterBackupServiceServer(s.grpcServer, v1handler.NewBackupServiceServer(s.logger,
		jobService,
		dataStoreService,
		namespaceService,
		projectService))
	// runtime service instance over grpc
	pb.RegisterRuntimeServiceServer(s.grpcServer, v1handler.NewRuntimeServiceServer(
		s.logger,
		config.Version,
		jobService,
		eventService,
		namespaceService,
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
