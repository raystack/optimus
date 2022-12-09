package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/hashicorp/go-hclog"
	hPlugin "github.com/hashicorp/go-plugin"
	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	slackapi "github.com/slack-go/slack"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	jobRunCompiler "github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/config"
	jHandler "github.com/odpf/optimus/core/job/handler/v1beta1"
	jResolver "github.com/odpf/optimus/core/job/resolver"
	jService "github.com/odpf/optimus/core/job/service"
	rModel "github.com/odpf/optimus/core/resource"
	rHandler "github.com/odpf/optimus/core/resource/handler/v1beta1"
	rService "github.com/odpf/optimus/core/resource/service"
	schedulerHandler "github.com/odpf/optimus/core/scheduler/handler/v1beta1"
	schedulerResolver "github.com/odpf/optimus/core/scheduler/resolver"
	schedulerService "github.com/odpf/optimus/core/scheduler/service"
	tHandler "github.com/odpf/optimus/core/tenant/handler/v1beta1"
	tService "github.com/odpf/optimus/core/tenant/service"
	"github.com/odpf/optimus/ext/notify/pagerduty"
	"github.com/odpf/optimus/ext/notify/slack"
	bqStore "github.com/odpf/optimus/ext/store/bigquery"
	"github.com/odpf/optimus/internal/compiler"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/store/postgres"
	jRepo "github.com/odpf/optimus/internal/store/postgres/job"
	"github.com/odpf/optimus/internal/store/postgres/resource"
	schedulerRepo "github.com/odpf/optimus/internal/store/postgres/scheduler"
	"github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/internal/telemetry"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const keyLength = 32

type setupFn func() error

type OptimusServer struct {
	conf   config.ServerConfig
	logger log.Logger

	appKey models.ApplicationKey
	dbConn *gorm.DB
	key    *[keyLength]byte

	serverAddr string
	grpcServer *grpc.Server
	httpServer *http.Server

	cleanupFn []func()
}

func New(conf config.ServerConfig) (*OptimusServer, error) {
	addr := fmt.Sprintf("%s:%d", conf.Serve.Host, conf.Serve.Port)
	server := &OptimusServer{
		conf:       conf,
		serverAddr: addr,
		logger:     NewLogger(conf.Log.Level.String()),
	}

	if err := checkRequiredConfigs(conf.Serve); err != nil {
		return server, err
	}

	setupFns := []setupFn{
		server.setupPlugins,
		server.setupTelemetry,
		server.setupAppKey,
		server.setupDB,
		server.setupGRPCServer,
		server.setupHandlers,
		server.setupMonitoring,
		server.setupHTTPProxy,
	}

	for _, fn := range setupFns {
		if err := fn(); err != nil {
			return server, err
		}
	}

	server.logger.Info("Starting Optimus", "version", config.BuildVersion)
	server.startListening()

	return server, nil
}

func (s *OptimusServer) setupPlugins() error {
	pluginLogLevel := hclog.Info
	if s.conf.Log.Level == config.LogLevelDebug {
		pluginLogLevel = hclog.Debug
	}

	pluginLoggerOpt := &hclog.LoggerOptions{
		Name:   "optimus",
		Output: os.Stdout,
		Level:  pluginLogLevel,
	}
	pluginLogger := hclog.New(pluginLoggerOpt)
	s.cleanupFn = append(s.cleanupFn, hPlugin.CleanupClients)

	var pluginArgs []string
	if s.conf.Telemetry.JaegerAddr != "" {
		pluginArgs = append(pluginArgs, "-t", s.conf.Telemetry.JaegerAddr)
	}
	// discover and load plugins.
	return plugin.Initialize(pluginLogger, pluginArgs...)
}

func (s *OptimusServer) setupTelemetry() error {
	teleShutdown, err := telemetry.Init(s.logger, s.conf.Telemetry)
	if err != nil {
		return err
	}

	s.cleanupFn = append(s.cleanupFn, teleShutdown)
	return nil
}

func (s *OptimusServer) setupAppKey() error {
	var err error
	s.appKey, err = models.NewApplicationSecret(s.conf.Serve.AppKey)
	if err != nil {
		return fmt.Errorf("NewApplicationSecret: %w", err)
	}

	s.key, err = applicationKeyFromString(s.conf.Serve.AppKey)
	if err != nil {
		return err
	}

	return nil
}

func applicationKeyFromString(appKey string) (*[keyLength]byte, error) {
	if len(appKey) < keyLength {
		return nil, errors.InvalidArgument("application_key", "application key should be 32 chars in length")
	}

	var key [keyLength]byte
	_, err := io.ReadFull(bytes.NewBufferString(appKey), key[:])
	return &key, err
}

func (s *OptimusServer) setupDB() error {
	migration, err := postgres.NewMigration(s.logger, config.BuildVersion, s.conf.Serve.DB.DSN)
	if err != nil {
		return fmt.Errorf("error initializing migration: %w", err)
	}
	ctx := context.Background()
	if err := migration.Up(ctx); err != nil {
		return fmt.Errorf("error executing migration up: %w", err)
	}

	s.dbConn, err = postgres.Connect(s.conf.Serve.DB, s.logger.Writer())
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

func (s *OptimusServer) startListening() {
	// run our server in a goroutine so that it doesn't block to wait for termination requests
	go func() {
		s.logger.Info("Listening at", "address", s.serverAddr)
		if err := s.httpServer.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				s.logger.Fatal("server error", "error", err)
			}
		}
	}()
}

func (s *OptimusServer) Shutdown() {
	s.logger.Warn("Shutting down server")
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

	if s.dbConn != nil {
		sqlConn, err := s.dbConn.DB()
		if err != nil {
			s.logger.Error("Error while getting sqlConn", err)
		} else if err := sqlConn.Close(); err != nil {
			s.logger.Error("Error in sqlConn.Close", err)
		}
	}

	s.logger.Info("Server shutdown complete")
}

func (s *OptimusServer) setupHandlers() error {
	// Tenant Bounded Context Setup
	tProjectRepo := tenant.NewProjectRepository(s.dbConn)
	tNamespaceRepo := tenant.NewNamespaceRepository(s.dbConn)
	tSecretRepo := tenant.NewSecretRepository(s.dbConn)

	tProjectService := tService.NewProjectService(tProjectRepo)
	tNamespaceService := tService.NewNamespaceService(tNamespaceRepo)
	tSecretService := tService.NewSecretService(s.key, tSecretRepo)
	tenantService := tService.NewTenantService(tProjectService, tNamespaceService, tSecretService)

	// Resource Bounded Context
	resourceRepository := resource.NewRepository(s.dbConn)
	backupRepository := resource.NewBackupRepository(s.dbConn)
	resourceManager := rService.NewResourceManager(resourceRepository, s.logger)
	resourceService := rService.NewResourceService(s.logger, resourceRepository, resourceManager, tenantService)
	backupService := rService.NewBackupService(backupRepository, resourceRepository, resourceManager)

	// Register datastore
	bqClientProvider := bqStore.NewClientProvider()
	bigqueryStore := bqStore.NewBigqueryDataStore(tenantService, bqClientProvider)
	resourceManager.RegisterDatastore(rModel.Bigquery, bigqueryStore)

	// Scheduler bounded context
	jobRunRepo := schedulerRepo.NewJobRunRepository(s.dbConn)
	operatorRunRepository := schedulerRepo.NewOperatorRunRepository(s.dbConn)
	jobProviderRepo := schedulerRepo.NewJobProviderRepository(s.dbConn)

	notificationContext, cancelNotifiers := context.WithCancel(context.Background())
	s.cleanupFn = append(s.cleanupFn, cancelNotifiers)

	notifierChanels := map[string]schedulerService.Notifier{
		"slack": slack.NewNotifier(notificationContext, slackapi.APIURL,
			slack.DefaultEventBatchInterval,
			func(err error) {
				s.logger.Error("slack error accumulator", "error", err)
			},
		),
		"pagerduty": pagerduty.NewNotifier(
			notificationContext,
			pagerduty.DefaultEventBatchInterval,
			func(err error) {
				s.logger.Error("pagerduty error accumulator", "error", err)
			},
			new(pagerduty.PagerDutyServiceImpl),
		),
	}

	newPriorityResolver := schedulerResolver.NewPriorityResolver()
	newEngine := compiler.NewEngine()
	assetCompiler := schedulerService.NewJobAssetsCompiler(newEngine, models.PluginRegistry)
	jobInputCompiler := schedulerService.NewJobInputCompiler(tenantService, newEngine, assetCompiler)
	notificationService := schedulerService.NewNotifyService(s.logger, jobProviderRepo, tenantService, notifierChanels)
	newScheduler, err := NewScheduler(s.conf, models.PluginRegistry, tProjectService, tSecretService)
	if err != nil {
		return err
	}
	newJobRunService := schedulerService.NewJobRunService(s.logger, jobProviderRepo, jobRunRepo, operatorRunRepository, newScheduler, newPriorityResolver, jobInputCompiler)

	engine := jobRunCompiler.NewGoEngine()

	// Job Bounded Context Setup
	jJobRepo := jRepo.NewJobRepository(s.dbConn)
	jPluginService := jService.NewJobPluginService(tSecretService, models.PluginRegistry, engine, s.logger)
	jExternalUpstreamResolver, _ := jResolver.NewExternalUpstreamResolver(s.conf.ResourceManagers)
	jInternalUpstreamResolver := jResolver.NewInternalUpstreamResolver(jJobRepo)
	jUpstreamResolver := jResolver.NewUpstreamResolver(jJobRepo, jExternalUpstreamResolver, jInternalUpstreamResolver)
	jJobService := jService.NewJobService(jJobRepo, jPluginService, jUpstreamResolver, tenantService, s.logger)

	scheduler, err := initScheduler(s.conf)
	if err != nil {
		return err
	}
	models.BatchScheduler = scheduler // TODO: remove global

	// Tenant Handlers
	pb.RegisterSecretServiceServer(s.grpcServer, tHandler.NewSecretsHandler(s.logger, tSecretService))
	pb.RegisterProjectServiceServer(s.grpcServer, tHandler.NewProjectHandler(s.logger, tProjectService))
	pb.RegisterNamespaceServiceServer(s.grpcServer, tHandler.NewNamespaceHandler(s.logger, tNamespaceService))

	// Resource Handler
	pb.RegisterResourceServiceServer(s.grpcServer, rHandler.NewResourceHandler(s.logger, resourceService))

	pb.RegisterJobRunServiceServer(s.grpcServer, schedulerHandler.NewJobRunHandler(s.logger, newJobRunService, notificationService))

	// backup service
	pb.RegisterBackupServiceServer(s.grpcServer, rHandler.NewBackupHandler(s.logger, backupService))
	// runtime service instance over grpc
	pb.RegisterRuntimeServiceServer(s.grpcServer, v1handler.NewRuntimeServiceServer(s.logger, config.BuildVersion))

	// Core Job Handler
	pb.RegisterJobSpecificationServiceServer(s.grpcServer, jHandler.NewJobHandler(jJobService, s.logger))

	s.cleanupFn = append(s.cleanupFn, func() {
		err = notificationService.Close()
		if err != nil {
			s.logger.Error("Error while closing event service: %s", err)
		}
	})

	return nil
}
