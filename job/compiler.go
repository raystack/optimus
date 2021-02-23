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
	ErrEmptyTemplateFile = errors.New("empty template file for job")
)

// Compiler converts generic job spec data to scheduler specific file that will
// be consumed by the target scheduler
type Compiler struct {
	templatePath string // template path relative to resources for dag generation
	fs           http.FileSystem
	hostname     string
}

// Compile use golang template engine to parse and insert job
// specific details in template file
func (com *Compiler) Compile(jobSpec models.JobSpec, proj models.ProjectSpec) (job models.Job, err error) {
	airflowTemplate, err := com.getTemplate()
	if err != nil {
		return models.Job{}, err
	}
	if len(airflowTemplate) == 0 {
		return models.Job{}, ErrEmptyTemplateFile
	}

	tmpl, err := template.New("compiler").Funcs(sprig.TxtFuncMap()).Parse(string(airflowTemplate))
	if err != nil {
		return models.Job{}, err
	}

	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, struct {
		Project                    models.ProjectSpec
		Job                        models.JobSpec
		Hostname                   string
		HookTypePre                string
		HookTypePost               string
		InstanceTypeTransformation string
		InstanceTypeHook           string
	}{
		Project:                    proj,
		Job:                        jobSpec,
		Hostname:                   com.hostname,
		HookTypePre:                string(models.HookTypePre),
		HookTypePost:               string(models.HookTypePost),
		InstanceTypeTransformation: string(models.InstanceTypeTransformation),
		InstanceTypeHook:           string(models.InstanceTypeHook),
	}); err != nil {
		return models.Job{}, err
	}

	return models.Job{
		Name:     jobSpec.Name,
		Contents: buf.Bytes(),
	}, nil
}

func (com *Compiler) getTemplate() ([]byte, error) {
	airflowTemplateFile, err := com.fs.Open(com.templatePath)
	if err != nil {
		return nil, err
	}
	defer airflowTemplateFile.Close()
	return ioutil.ReadAll(airflowTemplateFile)
}

// NewCompiler constructs a new Compiler that satisfies dag.Compiler
func NewCompiler(fs http.FileSystem, templatePath string, hostname string) *Compiler {
	return &Compiler{
		fs:           fs,
		templatePath: templatePath,
		hostname:     hostname,
	}
}
