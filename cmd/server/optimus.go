package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/odpf/salt/log"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

var (
	ErrMissingConfig       = errors.New("required config missing")
	ErrUnsupportedDBScheme = errors.New("unsupported database scheme, use 'postgres'")
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
				s.logger.Error("server error", "error", err)
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
