package dag

import (
	"bytes"
	_ "embed"
	"fmt"
	"text/template"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/job_run"
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

func (c *Compiler) Compile(job *job_run.Job) ([]byte, error) {
	task, err := PrepareTask(job, c.pluginRepo)
	if err != nil {
		return nil, err
	}

	hooks, err := PrepareHooksForJob(job, c.pluginRepo)
	if err != nil {
		return nil, err
	}

	slaDuration, err := SlaMissDuration(job)
	if err != nil {
		return nil, err
	}

	runtimeConfig := SetupRuntimeConfig(job)

	templateContext := TemplateContext{
		Job:             job,
		Tenant:          job.Tenant,
		Version:         config.BuildVersion,
		SlaMissDuration: slaDuration,
		Hostname:        c.hostname,
		ExecutorTask:    job_run.ExecutorTask,
		ExecutorHook:    job_run.ExecutorHook,
		Task:            task,
		Hooks:           hooks,
		RuntimeConfig:   runtimeConfig,
		Priority:        1000, // TODO: compute
	}

	var buf bytes.Buffer
	if err = c.template.Execute(&buf, templateContext); err != nil {
		fmt.Println(err)
		return nil, errors.InternalError(EntitySchedulerAirflow, "unable to compile template for job "+job.Name.String(), err)
	}

	return buf.Bytes(), nil
}

func NewDagCompiler(hostname string, repo PluginRepo) (*Compiler, error) {
	if len(dagTemplate) == 0 {
		return nil, errors.InternalError("SchedulerAirflow", "dag template is empty", nil)
	}

	tmpl, err := template.New("optimus_dag_compiler").Funcs(OptimusFuncMap()).Parse(string(dagTemplate))
	if err != nil {
		fmt.Println(err)
		return nil, errors.InternalError(EntitySchedulerAirflow, "unable to parse scheduler dag template", err)
	}

	return &Compiler{
		hostname:   hostname,
		template:   tmpl,
		pluginRepo: repo,
	}, nil
}
