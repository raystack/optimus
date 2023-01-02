package server

import (
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/ext/scheduler/airflow"
	"github.com/odpf/optimus/ext/scheduler/airflow/bucket"
	"github.com/odpf/optimus/ext/scheduler/airflow/dag"
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
