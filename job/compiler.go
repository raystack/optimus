package job

import (
	"bytes"
	"context"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

var (
	ErrEmptyTemplateFile = errors.New("empty template file for job")
)

// Compiler converts generic job spec data to scheduler specific file that will
// be consumed by the target scheduler
type Compiler struct {
	schedulerTemplate []byte // template string for dag generation
	hostname          string
}

// Compile use golang template engine to parse and insert job
// specific details in template file
func (com *Compiler) Compile(namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (job models.Job, err error) {
	if len(com.schedulerTemplate) == 0 {
		return models.Job{}, ErrEmptyTemplateFile
	}

	tmpl, err := template.New("compiler").Funcs(sprig.TxtFuncMap()).Parse(string(com.schedulerTemplate))
	if err != nil {
		return models.Job{}, err
	}

	var slaMissDurationInSec int64
	for _, notify := range jobSpec.Behavior.Notify {
		if notify.On == models.JobEventTypeSLAMiss {
			if _, ok := notify.Config["duration"]; !ok {
				continue
			}

			dur, err := time.ParseDuration(notify.Config["duration"])
			if err != nil {
				return models.Job{}, errors.Wrapf(err, "failed to parse sla_miss duration %s", notify.Config["duration"])
			}
			slaMissDurationInSec = int64(dur.Seconds())
		}
	}

	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, struct {
		Namespace                  models.NamespaceSpec
		Job                        models.JobSpec
		TaskSchemaRequest          models.GetTaskSchemaRequest
		HookSchemaRequest          models.GetHookSchemaRequest
		Context                    context.Context
		Hostname                   string
		HookTypePre                string
		HookTypePost               string
		HookTypeFail               string
		InstanceTypeTask           string
		InstanceTypeHook           string
		JobSpecDependencyTypeIntra string
		JobSpecDependencyTypeInter string
		JobSpecDependencyTypeExtra string
		SLAMissDurationInSec       int64
	}{
		Namespace:                  namespaceSpec,
		Job:                        jobSpec,
		Hostname:                   com.hostname,
		TaskSchemaRequest:          models.GetTaskSchemaRequest{},
		HookSchemaRequest:          models.GetHookSchemaRequest{},
		Context:                    context.Background(),
		HookTypePre:                string(models.HookTypePre),
		HookTypePost:               string(models.HookTypePost),
		HookTypeFail:               string(models.HookTypeFail),
		InstanceTypeTask:           string(models.InstanceTypeTask),
		InstanceTypeHook:           string(models.InstanceTypeHook),
		JobSpecDependencyTypeIntra: string(models.JobSpecDependencyTypeIntra),
		JobSpecDependencyTypeInter: string(models.JobSpecDependencyTypeInter),
		JobSpecDependencyTypeExtra: string(models.JobSpecDependencyTypeExtra),
		SLAMissDurationInSec:       slaMissDurationInSec,
	}); err != nil {
		return models.Job{}, errors.Wrap(err, "failed to templatize job")
	}

	return models.Job{
		Name:        jobSpec.Name,
		Contents:    buf.Bytes(),
		NamespaceID: namespaceSpec.ID.String(),
	}, nil
}

// NewCompiler constructs a new Compiler that satisfies dag.Compiler
func NewCompiler(schedulerTemplate []byte, hostname string) *Compiler {
	return &Compiler{
		schedulerTemplate: schedulerTemplate,
		hostname:          hostname,
	}
}
