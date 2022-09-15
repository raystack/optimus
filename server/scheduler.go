package server

import (
	"fmt"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/ext/scheduler/airflow2"
	"github.com/odpf/optimus/ext/scheduler/airflow2/compiler"
	"github.com/odpf/optimus/models"
	"net/http"
)

func initScheduler(conf config.ServerConfig) (models.SchedulerUnit, error) {
	jobCompiler := compiler.NewCompiler(conf.Serve.IngressHost)
	// init default scheduler
	var scheduler models.SchedulerUnit
	switch conf.Scheduler.Name {
	case "airflow", "airflow2":
		scheduler = airflow2.NewScheduler(
			&airflowBucketFactory{},
			&http.Client{},
			jobCompiler,
		)
	default:
		return nil, fmt.Errorf("unsupported scheduler: %s", conf.Scheduler.Name)
	}

	return scheduler, nil
}
