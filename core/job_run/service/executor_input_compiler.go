package service

import (
	"context"
	"time"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
)

const (
	// taskConfigPrefix will be used to prefix all the config variables of
	// transformation instance, i.e. task
	taskConfigPrefix = "TASK__"

	// projectConfigPrefix will be used to prefix all the config variables of
	// a project, i.e. registered entities
	projectConfigPrefix = "GLOBAL__"

	contextProject       = "proj"
	contextSecret        = "secret"
	contextSystemDefined = "inst"
)

type TenantService interface {
	GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error)
	GetSecrets(ctx context.Context, projName tenant.ProjectName, nsName string) ([]*tenant.PlainTextSecret, error)
}

type InputCompiler struct {
	tenantService TenantService
}

func (i InputCompiler) Compile(ctx context.Context, job *job_run.Job, config job_run.RunConfig, executedAt time.Time) (job_run.ExecutorInput, error) {
	tenantDetails, err := i.tenantService.GetDetails(ctx, job.Tenant())
	if err != nil {
		return job_run.ExecutorInput{}, err
	}

	secrets, err := i.tenantService.GetSecrets(ctx, job.Tenant().ProjectName(), job.Tenant().NamespaceName().String())
	if err != nil {
		return job_run.ExecutorInput{}, err
	}

	systemDefinedVars := getSystemDefinedConfigs(job, config, executedAt)

	// Prepare template context and compile task config
	_ = compiler.PrepareContext(
		compiler.From(tenantDetails.GetConfigs()).WithName(contextProject).WithKeyPrefix(projectConfigPrefix),
		compiler.From(secretsToMap(secrets)).WithName(contextSecret),
		compiler.From(systemDefinedVars).WithName(contextSystemDefined).AddToContext(),
	)

	//taskCompiledConfs, err := i.configCompiler.CompileConfigs(jobSpec.Task.Config, taskContext)
	//if err != nil {
	//	return nil, err
	//}
	return job_run.ExecutorInput{}, nil
}

func getSystemDefinedConfigs(job *job_run.Job, runConfig job_run.RunConfig, executedAt time.Time) map[string]string {
	return nil
}

/*

func getJobRunSpecData(executedAt time.Time, scheduledAt time.Time, jobSpec models.JobSpec) ([]models.JobRunSpecData, error) {
	startTime, err := jobSpec.Task.Window.GetStartTime(scheduledAt)
	if err != nil {
		return nil, err
	}
	endTime, err := jobSpec.Task.Window.GetEndTime(scheduledAt)
	if err != nil {
		return nil, err
	}
	jobRunSpecData := []models.JobRunSpecData{
		{
			Name:  models.ConfigKeyExecutionTime,
			Value: executedAt.Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDstart,
			Value: startTime.Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDend,
			Value: endTime.Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDestination,
			Value: jobSpec.ResourceDestination,
			Type:  models.InstanceDataTypeEnv,
		},
	}
	return jobRunSpecData, nil
}
*/

func secretsToMap(secrets []*tenant.PlainTextSecret) map[string]string {
	mapping := make(map[string]string, len(secrets))
	for _, s := range secrets {
		mapping[s.Name().String()] = s.Value()
	}
	return mapping
}
