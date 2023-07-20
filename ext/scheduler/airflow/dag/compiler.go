package dag

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"

	"github.com/raystack/optimus/config"
	"github.com/raystack/optimus/core/scheduler"
	"github.com/raystack/optimus/internal/errors"
	"github.com/raystack/optimus/sdk/plugin"
)

//go:embed dag.py.tmpl
var dagTemplate []byte

type PluginRepo interface {
	GetByName(name string) (*plugin.Plugin, error)
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

	slaDuration, err := SLAMissDuration(jobDetails)
	if err != nil {
		return nil, err
	}

	runtimeConfig := SetupRuntimeConfig(jobDetails)

	upstreams := SetupUpstreams(jobDetails.Upstreams, c.hostname)

	templateContext := TemplateContext{
		JobDetails:      jobDetails,
		Tenant:          jobDetails.Job.Tenant,
		Version:         config.BuildVersion,
		SLAMissDuration: slaDuration,
		Hostname:        c.hostname,
		ExecutorTask:    scheduler.ExecutorTask.String(),
		ExecutorHook:    scheduler.ExecutorHook.String(),
		Task:            task,
		Hooks:           hooks,
		RuntimeConfig:   runtimeConfig,
		Priority:        jobDetails.Priority,
		Upstreams:       upstreams,
	}

	var buf bytes.Buffer
	if err = c.template.Execute(&buf, templateContext); err != nil {
		msg := fmt.Sprintf("unable to compile template for job %s, %s", jobDetails.Name.String(), err.Error())
		return nil, errors.InvalidArgument(EntitySchedulerAirflow, msg)
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
