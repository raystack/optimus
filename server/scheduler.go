package server

import (
	"fmt"
	"net/http"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/ext/scheduler/airflow"
	"github.com/odpf/optimus/ext/scheduler/airflow/bucket"
	"github.com/odpf/optimus/ext/scheduler/airflow/dag"
	"github.com/odpf/optimus/ext/scheduler/airflow2"
	"github.com/odpf/optimus/ext/scheduler/airflow2/compiler"
	"github.com/odpf/optimus/models"
)

func NewScheduler(conf config.ServerConfig, pluginRepo dag.PluginRepo, projecGetter airflow.ProjectGetter, secretGetter airflow.SecretGetter) (*airflow.Scheduler, error) {
	bucketFactory := bucket.NewFactory(projecGetter, secretGetter)

	dagCompiler, err := dag.NewDagCompiler(conf.Serve.IngressHost, pluginRepo)
	if err != nil {
		return nil, err
	}

	client := airflow.NewAirflowClient()
	scheduler := airflow.NewScheduler(bucketFactory, client, dagCompiler, projecGetter, secretGetter)
	return scheduler, nil
}

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
