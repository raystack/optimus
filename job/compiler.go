package job

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/pkg/errors"
	"github.com/odpf/optimus/models"
)

var (
	ErrEmptyTaskNamespace = errors.New("task namespace cannot be empty")
)

type Compiler struct {
	templatePath string // template path relative to resources for dag generation
	fs           http.FileSystem
}

func (com *Compiler) prepare(spec models.JobSpec) (models.JobSpec, error) {

	return spec, nil
}

// Compile use golang template engine to parse and insert job
// specific details in template file
func (com *Compiler) Compile(spec models.JobSpec) (job models.Job, err error) {
	spec, err = com.prepare(spec)
	if err != nil {
		return models.Job{}, err
	}

	airflowTemplate, err := com.getTemplate()
	if err != nil {
		return models.Job{}, err
	}

	tmpl, err := template.New("airflow").Funcs(sprig.TxtFuncMap()).Parse(string(airflowTemplate))
	if err != nil {
		return models.Job{}, err
	}

	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, spec); err != nil {
		return models.Job{}, err
	}

	return models.Job{
		Name:     spec.Name,
		Contents: buf.Bytes(),
	}, nil
}

func (com *Compiler) getTemplate() ([]byte, error) {
	airflowTemplateFile, err := com.fs.Open(com.templatePath)
	if err != nil {
		return nil, err
	}

	airflowTemplate, err := ioutil.ReadAll(airflowTemplateFile)
	if err != nil {
		return nil, err
	}

	return airflowTemplate, nil
}

// NewCompiler constructs a new Compiler that satisfies dag.Compiler
func NewCompiler(fs http.FileSystem, templatePath string) *Compiler {
	return &Compiler{
		fs:           fs,
		templatePath: templatePath,
	}
}
