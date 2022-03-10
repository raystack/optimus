package server

import (
	"fmt"
	"github.com/odpf/optimus/models"
	"gorm.io/gorm"
	"net/http"
	"sync"

	"github.com/odpf/salt/log"
	"google.golang.org/grpc"

	"github.com/odpf/optimus/config"
)

type setupFn func() error

type OptimusServer struct {
	conf   config.Optimus
	logger log.Logger

	appKey models.ApplicationKey
	dbConn *gorm.DB

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

	server := &OptimusServer{
		conf:   conf,
		logger: l,
	}

	fns := []setupFn{
		server.setupAppKey,
		server.setupDB,
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
