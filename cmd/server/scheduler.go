package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/gossip"
	"github.com/odpf/optimus/ext/executor/noop"
	"github.com/odpf/optimus/ext/scheduler/airflow"
	"github.com/odpf/optimus/ext/scheduler/airflow2"
	"github.com/odpf/optimus/ext/scheduler/airflow2/compiler"
	"github.com/odpf/optimus/ext/scheduler/prime"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	"gorm.io/gorm"
)

func initScheduler(l log.Logger, conf config.Optimus, projectRepoFac *projectRepoFactory) (models.SchedulerUnit, error) {
	jobCompiler := compiler.NewCompiler(conf.Server.IngressHost)
	// init default scheduler
	var scheduler models.SchedulerUnit
	switch conf.Scheduler.Name {
	case "airflow":
		scheduler = airflow.NewScheduler(
			&airflowBucketFactory{},
			&http.Client{},
			jobCompiler,
		)
	case "airflow2":
		scheduler = airflow2.NewScheduler(
			&airflowBucketFactory{},
			&http.Client{},
			jobCompiler,
		)
	default:
		return nil, fmt.Errorf("unsupported scheduler: %s", conf.Scheduler.Name)
	}

	if !conf.Scheduler.SkipInit { // TODO: This should not be required
		registeredProjects, err := projectRepoFac.New().GetAll(context.Background())
		if err != nil {
			return nil, fmt.Errorf("projectRepoFactory.GetAll(): %w", err)
		}
		// bootstrap scheduler for registered projects
		for _, proj := range registeredProjects {
			bootstrapCtx, cancel := context.WithTimeout(context.Background(), BootstrapTimeout)
			l.Info("bootstrapping project", "project name", proj.Name)
			if err := scheduler.Bootstrap(bootstrapCtx, proj); err != nil {
				// Major ERROR, but we can't make this fatal
				// other projects might be working fine
				l.Error("no bootstrapping project", "error", err)
			}
			l.Info("bootstrapped project", "project name", proj.Name)
			cancel()
		}
	}
	return scheduler, nil
}

func initPrimeCluster(l log.Logger, conf config.Optimus, jobrunRepoFac *jobRunRepoFactory, dbConn *gorm.DB) (context.CancelFunc, error) {
	models.ManualScheduler = prime.NewScheduler( // careful global variable
		jobrunRepoFac,
		func() time.Time {
			return time.Now().UTC()
		},
	)

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
	cleanup := func() {
		// shutdown cluster
		clusterCancel()
		if clusterPlanner != nil {
			clusterPlanner.Close() // err is nil
		}
		if clusterServer != nil {
			clusterServer.Shutdown() // TODO: log error
		}
	}

	if conf.Scheduler.NodeID != "" {
		// start optimus cluster
		if err := clusterServer.Init(clusterCtx, conf.Scheduler); err != nil {
			return cleanup, err
		}

		clusterPlanner.Init(clusterCtx)
	}

	return cleanup, nil
}
