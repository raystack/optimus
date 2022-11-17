package dag

import (
	"bytes"
	_ "embed"
	"text/template"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/models"
)

//go:embed dag.py.tmpl
var dagTemplate []byte

type PluginRepo interface {
	GetByName(name string) (*models.Plugin, error)
}

type Compiler struct {
	hostname string

	template   *template.Template
	pluginRepo PluginRepo
}

func (c *Compiler) Compile(jobDetails *scheduler.JobWithDetails) ([]byte, error) {
	task, err := PrepareTask(jobDetails.Job, c.pluginRepo)
	if err != nil {
		return nil, err
	}

	hooks, err := PrepareHooksForJob(jobDetails.Job, c.pluginRepo)
	if err != nil {
		return nil, err
	}

	slaDuration, err := SlaMissDuration(jobDetails)
	if err != nil {
		return nil, err
	}

	runtimeConfig := SetupRuntimeConfig(jobDetails)

	upstreams := SetupUpstreams(jobDetails.Upstreams)

	templateContext := TemplateContext{
		JobDetails:      jobDetails,
		Tenant:          jobDetails.Job.Tenant,
		Version:         config.BuildVersion,
		SLAMissDuration: slaDuration,
		Hostname:        c.hostname,
		ExecutorTask:    scheduler.ExecutorTask,
		ExecutorHook:    scheduler.ExecutorHook,
		Task:            task,
		Hooks:           hooks,
		RuntimeConfig:   runtimeConfig,
		Priority:        jobDetails.Priority,
		Upstreams:       upstreams,
	}

	var buf bytes.Buffer
	if err = c.template.Execute(&buf, templateContext); err != nil {
		return nil, errors.InternalError(EntitySchedulerAirflow, "unable to compile template for job "+jobDetails.Name.String(), err)
	}

	return buf.Bytes(), nil
}

func NewDagCompiler(hostname string, repo PluginRepo) (*Compiler, error) {
	if len(dagTemplate) == 0 {
		return nil, errors.InternalError("SchedulerAirflow", "dag template is empty", nil)
	}

	tmpl, err := template.New("optimus_dag_compiler").Funcs(OptimusFuncMap()).Parse(string(dagTemplate))
	if err != nil {
		return nil, errors.InternalError(EntitySchedulerAirflow, "unable to parse scheduler dag template", err)
	}

	return &Compiler{
		hostname:   hostname,
		template:   tmpl,
		pluginRepo: repo,
	}, nil
}
