package dag

import (
	"context"
	_ "embed"
	"text/template"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/internal/errors"
)

//go:embed dag.py.tmpl
var dagTemplate []byte

type Compiler struct {
	hostname string

	template *template.Template
}

func (c Compiler) Compile(ctx context.Context, job *job_run.Job) ([]byte, error) {
	return nil, nil
}

func NewDagCompiler(hostname string) (*Compiler, error) {
	if len(dagTemplate) == 0 {
		return nil, errors.InternalError("SchedulerAirflow", "dag template is empty", nil)
	}

	tmpl, err := template.New("optimus_dag_compiler").Parse(string(dagTemplate))
	if err != nil {
		return nil, errors.InternalError("SchedulerAirflow", "unable to parse scheduler dag template", err)
	}

	return &Compiler{
		hostname: hostname,
		template: tmpl,
	}, nil
}
