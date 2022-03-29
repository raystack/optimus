package compiler

import (
	"bytes"
	"errors"
	"fmt"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

var ErrEmptyTemplateFile = errors.New("empty template file for job")

// Compiler converts generic job spec data to scheduler specific file that will
// be consumed by the target scheduler
type Compiler struct {
	hostname string
}

// Compile use golang template engine to parse and insert job
// specific details in template file
func (com *Compiler) Compile(schedulerTemplate []byte, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (job models.Job, err error) {
	if len(schedulerTemplate) == 0 {
		return models.Job{}, ErrEmptyTemplateFile
	}

	tmpl, err := template.New("compiler").Funcs(sprig.TxtFuncMap()).Parse(string(schedulerTemplate))
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
				return models.Job{}, fmt.Errorf("failed to parse sla_miss duration %s: %w", notify.Config["duration"], err)
			}
			slaMissDurationInSec = int64(dur.Seconds())
		}
	}

	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, struct {
		Namespace                  models.NamespaceSpec
		Job                        models.JobSpec
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
		Version                    string
		Metadata                   models.JobSpecMetadata
	}{
		Namespace:                  namespaceSpec,
		Job:                        jobSpec,
		Hostname:                   com.hostname,
		HookTypePre:                string(models.HookTypePre),
		HookTypePost:               string(models.HookTypePost),
		HookTypeFail:               string(models.HookTypeFail),
		InstanceTypeTask:           string(models.InstanceTypeTask),
		InstanceTypeHook:           string(models.InstanceTypeHook),
		JobSpecDependencyTypeIntra: string(models.JobSpecDependencyTypeIntra),
		JobSpecDependencyTypeInter: string(models.JobSpecDependencyTypeInter),
		JobSpecDependencyTypeExtra: string(models.JobSpecDependencyTypeExtra),
		SLAMissDurationInSec:       slaMissDurationInSec,
		Version:                    config.Version,
		Metadata:                   jobSpec.Metadata,
	}); err != nil {
		return models.Job{}, fmt.Errorf("failed to templatize job: %w", err)
	}

	return models.Job{
		Name:     jobSpec.Name,
		Contents: buf.Bytes(),
	}, nil
}

// NewCompiler constructs a new Compiler that satisfies dag.Compiler
func NewCompiler(hostname string) *Compiler {
	return &Compiler{
		hostname: hostname,
	}
}
