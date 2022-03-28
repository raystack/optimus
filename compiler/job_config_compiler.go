package compiler

import (
	"strings"

	"github.com/odpf/optimus/models"
)

const (
	SecretsStringToMatch = ".secret."
)

type JobConfigCompiler struct {
	engine models.TemplateEngine
}

func NewJobConfigCompiler(engine models.TemplateEngine) *JobConfigCompiler {
	return &JobConfigCompiler{
		engine: engine,
	}
}

type CompiledConfigs struct {
	Configs map[string]string
	Secrets map[string]string
}

func (c *JobConfigCompiler) CompileConfigs(configs models.JobSpecConfigs, templateCtx map[string]interface{}) (*CompiledConfigs, error) {
	conf, secretsConfig := splitConfigForSecrets(configs)

	var err error
	if conf, err = c.compileTemplates(conf, templateCtx); err != nil {
		return nil, err
	}

	if secretsConfig, err = c.compileTemplates(secretsConfig, templateCtx); err != nil {
		return nil, err
	}

	return &CompiledConfigs{
		Configs: conf,
		Secrets: secretsConfig,
	}, nil
}

func (c *JobConfigCompiler) compileTemplates(templateValueMap map[string]string, templateContext map[string]interface{}) (map[string]string, error) {
	for key, val := range templateValueMap {
		compiledValue, err := c.engine.CompileString(val, templateContext)
		if err != nil {
			return nil, err
		}
		templateValueMap[key] = compiledValue
	}
	return templateValueMap, nil
}

func splitConfigForSecrets(jobSpecConfig models.JobSpecConfigs) (map[string]string, map[string]string) {
	configs := map[string]string{}
	configWithSecrets := map[string]string{}
	for _, val := range jobSpecConfig {
		if strings.Contains(val.Value, SecretsStringToMatch) {
			configWithSecrets[val.Name] = val.Value
			continue
		}
		configs[val.Name] = val.Value
	}

	return configs, configWithSecrets
}
