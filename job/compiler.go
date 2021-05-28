package job

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"text/template"

	"github.com/Masterminds/sprig/v3"
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
func (com *Compiler) Compile(namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (job models.Job, err error) {
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
		TaskSchemaRequest          models.GetTaskSchemaRequest
		HookSchemaRequest          models.GetHookSchemaRequest
		Context                    context.Context
		Hostname                   string
		HookTypePre                string
		HookTypePost               string
		InstanceTypeTask           string
		InstanceTypeHook           string
		JobSpecDependencyTypeIntra string
		JobSpecDependencyTypeInter string
		JobSpecDependencyTypeExtra string
	}{
		Project:                    namespaceSpec.ProjectSpec,
		Job:                        jobSpec,
		Hostname:                   com.hostname,
		TaskSchemaRequest:          models.GetTaskSchemaRequest{},
		HookSchemaRequest:          models.GetHookSchemaRequest{},
		Context:                    context.Background(),
		HookTypePre:                string(models.HookTypePre),
		HookTypePost:               string(models.HookTypePost),
		InstanceTypeTask:           string(models.InstanceTypeTask),
		InstanceTypeHook:           string(models.InstanceTypeHook),
		JobSpecDependencyTypeIntra: string(models.JobSpecDependencyTypeIntra),
		JobSpecDependencyTypeInter: string(models.JobSpecDependencyTypeInter),
		JobSpecDependencyTypeExtra: string(models.JobSpecDependencyTypeExtra),
	}); err != nil {
		return models.Job{}, errors.Wrap(err, "failed to templatize job")
	}

	return models.Job{
		Name:        jobSpec.Name,
		Contents:    buf.Bytes(),
		NamespaceID: namespaceSpec.ID.String(),
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
