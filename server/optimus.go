package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/hashicorp/go-hclog"
	hPlugin "github.com/hashicorp/go-plugin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/odpf/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	slackapi "github.com/slack-go/slack"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"

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
	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/internal/store/postgres"
	jRepo "github.com/odpf/optimus/internal/store/postgres/job"
	"github.com/odpf/optimus/internal/store/postgres/resource"
	schedulerRepo "github.com/odpf/optimus/internal/store/postgres/scheduler"
	"github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/internal/telemetry"
	"github.com/odpf/optimus/plugin"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	oHandler "github.com/odpf/optimus/server/handler/v1beta1"
)

const keyLength = 32

type setupFn func() error

type OptimusServer struct {
	conf   *config.ServerConfig
	logger log.Logger

	dbPool *pgxpool.Pool
	key    *[keyLength]byte

	serverAddr string
	mux        cmux.CMux
	grpcServer *grpc.Server
	httpServer *http.Server

	pluginRepo *models.PluginRepository
	cleanupFn  []func()
}

func New(conf *config.ServerConfig) (*OptimusServer, error) {
	addr := fmt.Sprintf(":%d", conf.Serve.Port)
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
		server.setupMux,
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
	var err error
	s.pluginRepo, err = plugin.Initialize(pluginLogger, pluginArgs...)
	return err
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
	err := postgres.Migrate(s.conf.Serve.DB.DSN)
	if err != nil {
		return fmt.Errorf("error initializing migration: %w", err)
	}

	s.dbPool, err = postgres.Open(s.conf.Serve.DB)
	if err != nil {
		return fmt.Errorf("postgres.Open: %w", err)
	}

	return nil
}

func (s *OptimusServer) setupMux() error {
	ln, err := net.Listen("tcp", s.serverAddr)
	if err != nil {
		return err
	}

	s.mux = cmux.New(ln)
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
	srv, cleanup, err := prepareHTTPProxy(s.serverAddr)
	s.httpServer = srv
	s.cleanupFn = append(s.cleanupFn, cleanup)
	return err
}

func (s *OptimusServer) startListening() {
	// run our server in a goroutine so that it doesn't block to wait for termination requests
	// We first match a request first for grpc
	grpcLn := s.mux.MatchWithWriters(
		cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	// if it does not match grpc, we send the request to http
	httpL := s.mux.Match(cmux.Any())

	go func() {
		// Start listening on grpc server
		if err := s.grpcServer.Serve(grpcLn); err != nil {
			s.logger.Info("shutting down grpc server")
		}
	}()

	go func() {
		// Start listening on http server
		err := s.httpServer.Serve(httpL)
		if err != nil {
			s.logger.Info("Shutting http proxy")
		}
	}()

	go func() {
		// Start mux server
		s.logger.Info("Listening at", "address", s.serverAddr)
		if err := s.mux.Serve(); err != nil {
			s.logger.Info("server error", "error", err)
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

	if s.dbPool != nil {
		s.dbPool.Close()
	}

	s.logger.Info("Server shutdown complete")
}

func (s *OptimusServer) setupHandlers() error {
	// Tenant Bounded Context Setup
	tProjectRepo := tenant.NewProjectRepository(s.dbPool)
	tNamespaceRepo := tenant.NewNamespaceRepository(s.dbPool)
	tSecretRepo := tenant.NewSecretRepository(s.dbPool)

	tProjectService := tService.NewProjectService(tProjectRepo)
	tNamespaceService := tService.NewNamespaceService(tNamespaceRepo)
	tSecretService := tService.NewSecretService(s.key, tSecretRepo)
	tenantService := tService.NewTenantService(tProjectService, tNamespaceService, tSecretService)

	// Resource Bounded Context
	resourceRepository := resource.NewRepository(s.dbPool)
	backupRepository := resource.NewBackupRepository(s.dbPool)
	resourceManager := rService.NewResourceManager(resourceRepository, s.logger)
	resourceService := rService.NewResourceService(s.logger, resourceRepository, resourceManager, tenantService)
	backupService := rService.NewBackupService(backupRepository, resourceRepository, resourceManager)

	// Register datastore
	bqClientProvider := bqStore.NewClientProvider()
	bigqueryStore := bqStore.NewBigqueryDataStore(tenantService, bqClientProvider)
	resourceManager.RegisterDatastore(rModel.Bigquery, bigqueryStore)

	// Scheduler bounded context
	jobRunRepo := schedulerRepo.NewJobRunRepository(s.dbPool)
	operatorRunRepository := schedulerRepo.NewOperatorRunRepository(s.dbPool)
	jobProviderRepo := schedulerRepo.NewJobProviderRepository(s.dbPool)

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

	newEngine := compiler.NewEngine()

	newPriorityResolver := schedulerResolver.NewPriorityResolver()
	assetCompiler := schedulerService.NewJobAssetsCompiler(newEngine, s.pluginRepo)
	jobInputCompiler := schedulerService.NewJobInputCompiler(tenantService, newEngine, assetCompiler)
	notificationService := schedulerService.NewNotifyService(s.logger, jobProviderRepo, tenantService, notifierChanels)
	newScheduler, err := NewScheduler(s.logger, s.conf, s.pluginRepo, tProjectService, tSecretService)
	if err != nil {
		return err
	}
	newJobRunService := schedulerService.NewJobRunService(s.logger, jobProviderRepo, jobRunRepo, operatorRunRepository, newScheduler, newPriorityResolver, jobInputCompiler)

	// Job Bounded Context Setup
	jJobRepo := jRepo.NewJobRepository(s.dbPool)
	jPluginService := jService.NewJobPluginService(tSecretService, s.pluginRepo, newEngine, s.logger)
	jExternalUpstreamResolver, _ := jResolver.NewExternalUpstreamResolver(s.conf.ResourceManagers)
	jInternalUpstreamResolver := jResolver.NewInternalUpstreamResolver(jJobRepo)
	jUpstreamResolver := jResolver.NewUpstreamResolver(jJobRepo, jExternalUpstreamResolver, jInternalUpstreamResolver)
	jJobService := jService.NewJobService(jJobRepo, jPluginService, jUpstreamResolver, tenantService, s.logger)

	// Tenant Handlers
	pb.RegisterSecretServiceServer(s.grpcServer, tHandler.NewSecretsHandler(s.logger, tSecretService))
	pb.RegisterProjectServiceServer(s.grpcServer, tHandler.NewProjectHandler(s.logger, tProjectService))
	pb.RegisterNamespaceServiceServer(s.grpcServer, tHandler.NewNamespaceHandler(s.logger, tNamespaceService))

	// Resource Handler
	pb.RegisterResourceServiceServer(s.grpcServer, rHandler.NewResourceHandler(s.logger, resourceService))

	pb.RegisterJobRunServiceServer(s.grpcServer, schedulerHandler.NewJobRunHandler(s.logger, newJobRunService, notificationService))

	// backup service
	pb.RegisterBackupServiceServer(s.grpcServer, rHandler.NewBackupHandler(s.logger, backupService))

	// version service
	pb.RegisterRuntimeServiceServer(s.grpcServer, oHandler.NewVersionHandler(s.logger, config.BuildVersion))

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
