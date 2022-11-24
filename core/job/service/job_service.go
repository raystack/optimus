package service

import (
	"context"
	"fmt"

	"github.com/odpf/salt/log"

	"github.com/odpf/optimus/api/writer"
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/job/service/filter"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
	"github.com/odpf/optimus/internal/lib/tree"
	"github.com/odpf/optimus/models"
)

type JobService struct {
	repo JobRepository

	pluginService    PluginService
	upstreamResolver UpstreamResolver

	tenantDetailsGetter TenantDetailsGetter

	logger log.Logger
}

func NewJobService(repo JobRepository, pluginService PluginService, upstreamResolver UpstreamResolver, tenantDetailsGetter TenantDetailsGetter, logger log.Logger) *JobService {
	return &JobService{
		repo:                repo,
		pluginService:       pluginService,
		upstreamResolver:    upstreamResolver,
		tenantDetailsGetter: tenantDetailsGetter,
		logger:              logger,
	}
}

type PluginService interface {
	Info(context.Context, *job.Task) (*models.PluginInfoResponse, error)
	GenerateDestination(context.Context, *tenant.WithDetails, *job.Task) (job.ResourceURN, error)
	GenerateUpstreams(ctx context.Context, jobTenant *tenant.WithDetails, spec *job.Spec, dryRun bool) ([]job.ResourceURN, error)
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, jobTenant tenant.Tenant) (*tenant.WithDetails, error)
}

type JobRepository interface {
	// TODO: remove `savedJobs` since the method's main purpose is to add, not to get
	Add(context.Context, []*job.Job) (addedJobs []*job.Job, err error)
	Update(context.Context, []*job.Job) (updatedJobs []*job.Job, err error)
	GetJobNameWithInternalUpstreams(context.Context, tenant.ProjectName, []job.Name) (map[job.Name][]*job.Upstream, error)
	ReplaceUpstreams(context.Context, []*job.WithUpstream) error

	GetDownstreamFullNames(context.Context, tenant.ProjectName, job.Name) ([]job.FullName, error)
	Delete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, cleanHistory bool) error

	GetByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error)
	GetAllByResourceDestination(ctx context.Context, resourceDestination job.ResourceURN) ([]*job.Job, error)
	GetAllByTenant(ctx context.Context, jobTenant tenant.Tenant) ([]*job.Job, error)
	GetAllByProjectName(ctx context.Context, projectName tenant.ProjectName) ([]*job.Job, error)
}

type UpstreamResolver interface {
	Resolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job, logWriter writer.LogWriter) (jobWithUpstreams []*job.WithUpstream, err error)
}

func (j JobService) Add(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) error {
	logWriter := writer.NewLogWriter(j.logger)
	me := errors.NewMultiError("add specs errors")

	validatedSpecs, err := j.getValidatedSpecs(specs)
	me.Append(err)

	jobs, err := j.generateJobs(ctx, jobTenant, validatedSpecs)
	me.Append(err)

	addedJobs, err := j.repo.Add(ctx, jobs)
	me.Append(err)

	jobsWithUpstreams, err := j.upstreamResolver.Resolve(ctx, jobTenant.ProjectName(), addedJobs, logWriter)
	me.Append(err)

	err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	return errors.MultiToError(me)
}

func (j JobService) Update(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) error {
	logWriter := writer.NewLogWriter(j.logger)
	me := errors.NewMultiError("update specs errors")

	validatedSpecs, err := j.getValidatedSpecs(specs)
	me.Append(err)

	jobs, err := j.generateJobs(ctx, jobTenant, validatedSpecs)
	me.Append(err)

	updatedJobs, err := j.repo.Update(ctx, jobs)
	me.Append(err)

	jobsWithUpstreams, err := j.upstreamResolver.Resolve(ctx, jobTenant.ProjectName(), updatedJobs, logWriter)
	me.Append(err)

	err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	return errors.MultiToError(me)
}

func (j JobService) Delete(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, cleanFlag bool, forceFlag bool) (affectedDownstream []job.FullName, err error) {
	downstreamFullNames, err := j.repo.GetDownstreamFullNames(ctx, jobTenant.ProjectName(), jobName)
	if err != nil {
		return nil, err
	}

	if len(downstreamFullNames) > 0 && !forceFlag {
		errorMsg := fmt.Sprintf("job is being used by %s", downstreamFullNames)
		return nil, errors.NewError(errors.ErrFailedPrecond, job.EntityJob, errorMsg)
	}

	return downstreamFullNames, j.repo.Delete(ctx, jobTenant.ProjectName(), jobName, cleanFlag)
}

func (j JobService) Get(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name) (*job.Job, error) {
	jobs, err := j.GetAll(ctx,
		filter.WithString(filter.ProjectName, jobTenant.ProjectName().String()),
		filter.WithString(filter.JobName, jobName.String()),
	)
	if err != nil {
		return nil, err
	}

	return jobs[0], nil
}

func (j JobService) GetTaskInfo(ctx context.Context, task *job.Task) (*job.Task, error) {
	taskInfo, err := j.pluginService.Info(ctx, task)
	if err != nil {
		return nil, err
	}

	return job.NewTaskBuilder(
		task.Name(),
		task.Config(),
	).WithInfo(taskInfo).Build(), nil
}

func (j JobService) GetAll(ctx context.Context, filters ...filter.FilterOpt) ([]*job.Job, error) {
	f := filter.NewFilter(filters...)

	// when resource destination exist, filter by destination
	if f.Contains(filter.ResourceDestination) {
		resourceDestination, _ := job.ResourceURNFrom(f.GetStringValue(filter.ResourceDestination))
		return j.repo.GetAllByResourceDestination(ctx, resourceDestination)
	}

	// when project name and job names exist, filter by project and job names
	if f.Contains(filter.ProjectName, filter.JobNames) {
		me := errors.NewMultiError("get all job specs errors")

		projectName, _ := tenant.ProjectNameFrom(f.GetStringValue(filter.ProjectName))
		jobNames := f.GetStringArrayValue(filter.JobNames)

		var jobs []*job.Job
		for _, jobNameStr := range jobNames {
			jobName, _ := job.NameFrom(jobNameStr)
			fetchedJob, err := j.repo.GetByJobName(ctx, projectName, jobName)
			if err != nil {
				me.Append(err)
				continue
			}
			jobs = append(jobs, fetchedJob)
		}
		return jobs, errors.MultiToError(me)
	}

	// when project name and job name exist, filter by project name and job name
	if f.Contains(filter.ProjectName, filter.JobName) {
		projectName, _ := tenant.ProjectNameFrom(f.GetStringValue(filter.ProjectName))
		jobName, _ := job.NameFrom(f.GetStringValue(filter.JobName))
		fetchedJob, err := j.repo.GetByJobName(ctx, projectName, jobName)
		if err != nil {
			return nil, err
		}
		return []*job.Job{fetchedJob}, nil
	}

	// when project name and namespace names exist, filter by tenant
	if f.Contains(filter.ProjectName, filter.NamespaceNames) {
		var jobs []*job.Job
		namespaceNames := f.GetStringArrayValue(filter.NamespaceNames)
		for _, namespaceName := range namespaceNames {
			jobTenant, err := tenant.NewTenant(f.GetStringValue(filter.ProjectName), namespaceName)
			if err != nil {
				return nil, err
			}
			tenantJobs, err := j.repo.GetAllByTenant(ctx, jobTenant)
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, tenantJobs...)
		}
		return jobs, nil
	}

	// when project name exist, filter by project name
	if f.Contains(filter.ProjectName) {
		projectName, _ := tenant.ProjectNameFrom(f.GetStringValue(filter.ProjectName))
		return j.repo.GetAllByProjectName(ctx, projectName)
	}

	return nil, nil
}

func (j JobService) ReplaceAll(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("replace all specs errors")

	validatedSpecs, err := j.getValidatedSpecs(specs)
	me.Append(err)

	toAdd, toUpdate, toDelete, err := j.differentiateSpecs(ctx, jobTenant, validatedSpecs)
	logWriter.Write(writer.LogLevelInfo, fmt.Sprintf("found %d new, %d modified, and %d deleted job specs", len(toAdd), len(toUpdate), len(toDelete)))
	me.Append(err)

	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	me.Append(err)

	addedJobs, err := j.bulkAdd(ctx, tenantWithDetails, toAdd, logWriter)
	me.Append(err)

	updatedJobs, err := j.bulkUpdate(ctx, tenantWithDetails, toUpdate, logWriter)
	me.Append(err)

	err = j.bulkDelete(ctx, jobTenant, toDelete, logWriter)
	me.Append(err)

	err = j.resolveAndSaveUpstreams(ctx, jobTenant.ProjectName(), logWriter, addedJobs, updatedJobs)
	me.Append(err)

	return errors.MultiToError(me)
}

func (j JobService) Refresh(ctx context.Context, projectName tenant.ProjectName, logWriter writer.LogWriter, filters ...filter.FilterOpt) (err error) {
	me := errors.NewMultiError("refresh all specs errors")

	jobs, err := j.GetAll(ctx, filters...)
	me.Append(err)

	jobsWithUpstreams, err := j.upstreamResolver.Resolve(ctx, projectName, jobs, logWriter)
	me.Append(err)

	err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	return errors.MultiToError(me)
}

func (j JobService) Validate(ctx context.Context, jobTenant tenant.Tenant, jobSpecs []*job.Spec, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("validate specs errors")
	validatedJobSpecs, err := j.getValidatedSpecs(jobSpecs)
	me.Append(err)

	jobs, err := j.generateJobs(ctx, jobTenant, validatedJobSpecs)
	me.Append(err)

	if len(me.Errors) > 0 {
		return me
	}

	// NOTE: only check cyclic deps across internal upstreams (sources), need further discussion to check cyclic deps for external upstream
	// asumption, all job specs from input are also the job within same project

	// populate all jobs in project
	jobsInProject, err := j.GetAll(ctx, filter.WithString(filter.ProjectName, jobTenant.ProjectName().String()))
	if err != nil {
		return err
	}
	jobMap := make(map[job.Name]*job.Job)
	for _, jobEntity := range jobsInProject {
		jobMap[jobEntity.Spec().Name()] = jobEntity
	}

	// check cyclic deps for every job
	for _, jobEntity := range jobs {
		if err = j.validateCyclic(ctx, jobEntity.Spec().Name(), jobMap); err != nil {
			me.Append(err)
		}
	}

	if len(me.Errors) > 0 {
		return me
	}

	return nil
}

func (j JobService) resolveAndSaveUpstreams(ctx context.Context, projectName tenant.ProjectName, logWriter writer.LogWriter, jobsToResolve ...[]*job.Job) error {
	var allJobsToResolve []*job.Job
	for _, group := range jobsToResolve {
		allJobsToResolve = append(allJobsToResolve, group...)
	}
	if len(allJobsToResolve) == 0 {
		return nil
	}

	me := errors.NewMultiError("resolve and save upstream errors")
	jobsWithUpstreams, err := j.upstreamResolver.Resolve(ctx, projectName, allJobsToResolve, logWriter)
	me.Append(err)

	err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	return errors.MultiToError(err)
}

func (j JobService) bulkAdd(ctx context.Context, tenantWithDetails *tenant.WithDetails, specsToAdd []*job.Spec, logWriter writer.LogWriter) ([]*job.Job, error) {
	me := errors.NewMultiError("bulk add specs errors")

	//TODO: parallelize this
	var jobsToAdd []*job.Job
	for _, spec := range specsToAdd {
		generatedJob, err := j.generateJob(ctx, tenantWithDetails, spec)
		if err != nil {
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] unable to add job %s", tenantWithDetails.Namespace().Name().String(), spec.Name().String()))
			me.Append(err)
			continue
		}
		jobsToAdd = append(jobsToAdd, generatedJob)
		logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] adding job %s", tenantWithDetails.Namespace().Name().String(), spec.Name().String()))
	}

	if len(jobsToAdd) == 0 {
		return nil, errors.MultiToError(me)
	}

	addedJobs, err := j.repo.Add(ctx, jobsToAdd)
	me.Append(err)

	return addedJobs, errors.MultiToError(me)
}

func (j JobService) bulkUpdate(ctx context.Context, tenantWithDetails *tenant.WithDetails, specsToUpdate []*job.Spec, logWriter writer.LogWriter) ([]*job.Job, error) {
	me := errors.NewMultiError("bulk add specs errors")

	//TODO: parallelize this
	var jobsToUpdate []*job.Job
	for _, spec := range specsToUpdate {
		generatedJob, err := j.generateJob(ctx, tenantWithDetails, spec)
		if err != nil {
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] unable to update job %s", tenantWithDetails.Namespace().Name().String(), spec.Name().String()))
			me.Append(err)
			continue
		}
		jobsToUpdate = append(jobsToUpdate, generatedJob)
		logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] updating job %s", tenantWithDetails.Namespace().Name().String(), spec.Name().String()))
	}

	if len(jobsToUpdate) == 0 {
		return nil, errors.MultiToError(me)
	}

	updatedJobs, err := j.repo.Update(ctx, jobsToUpdate)
	me.Append(err)

	return updatedJobs, errors.MultiToError(me)
}

func (j JobService) bulkDelete(ctx context.Context, jobTenant tenant.Tenant, toDelete []*job.Spec, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("bulk delete specs errors")

	for _, spec := range toDelete {
		downstreamFullNames, err := j.repo.GetDownstreamFullNames(ctx, jobTenant.ProjectName(), spec.Name())
		if err != nil {
			me.Append(err)
			continue
		}

		if len(downstreamFullNames) > 0 {
			errorMsg := fmt.Sprintf("deleting job %s failed. job is being used by %s", spec.Name().String(), downstreamFullNames)
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] %s", jobTenant.NamespaceName().String(), spec.Name().String()))
			me.Append(errors.NewError(errors.ErrFailedPrecond, job.EntityJob, errorMsg))
			continue
		}

		err = j.repo.Delete(ctx, jobTenant.ProjectName(), spec.Name(), false)
		if err == nil {
			logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] job %s deleted", jobTenant.NamespaceName().String(), spec.Name().String()))
		}
		me.Append(err)
	}
	return errors.MultiToError(me)
}

func (j JobService) differentiateSpecs(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) (added []*job.Spec, modified []*job.Spec, deleted []*job.Spec, err error) {
	me := errors.NewMultiError("differentiate specs errors")

	existingJobs, err := j.repo.GetAllByTenant(ctx, jobTenant)
	me.Append(err)

	var addedSpecs, modifiedSpecs, deletedSpecs []*job.Spec

	existingSpecsMap := job.Jobs(existingJobs).GetNameAndSpecMap()
	for _, incomingSpec := range specs {
		if spec, ok := existingSpecsMap[incomingSpec.Name()]; !ok {
			addedSpecs = append(addedSpecs, incomingSpec)
		} else if !spec.IsEqual(incomingSpec) {
			modifiedSpecs = append(modifiedSpecs, incomingSpec)
		}
	}

	incomingSpecsMap := job.Specs(specs).ToNameAndSpecMap()
	for _, existingJob := range existingJobs {
		if _, ok := incomingSpecsMap[existingJob.Spec().Name()]; !ok {
			deletedSpecs = append(deletedSpecs, existingJob.Spec())
		}
	}
	return addedSpecs, modifiedSpecs, deletedSpecs, errors.MultiToError(me)
}

func (JobService) getValidatedSpecs(jobs []*job.Spec) ([]*job.Spec, error) {
	me := errors.NewMultiError("spec validation errors")

	var validatedSpecs []*job.Spec
	for _, spec := range jobs {
		if err := spec.Validate(); err != nil {
			me.Append(err)
			continue
		}
		validatedSpecs = append(validatedSpecs, spec)
	}
	return validatedSpecs, errors.MultiToError(me)
}

func (j JobService) generateJobs(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) ([]*job.Job, error) {
	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		return nil, err
	}

	me := errors.NewMultiError("generate jobs errors")
	var output []*job.Job
	for _, spec := range specs {
		generatedJob, err := j.generateJob(ctx, tenantWithDetails, spec)
		if err != nil {
			me.Append(err)
			continue
		}
		output = append(output, generatedJob)
	}
	return output, errors.MultiToError(me)
}

func (j JobService) generateJob(ctx context.Context, tenantWithDetails *tenant.WithDetails, spec *job.Spec) (*job.Job, error) {
	destination, err := j.pluginService.GenerateDestination(ctx, tenantWithDetails, spec.Task())
	if err != nil && !errors.Is(err, ErrUpstreamModNotFound) {
		errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
		return nil, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
	}

	sources, err := j.pluginService.GenerateUpstreams(ctx, tenantWithDetails, spec, true)
	if err != nil && !errors.Is(err, ErrUpstreamModNotFound) {
		errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
		return nil, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
	}

	return job.NewJob(tenantWithDetails.ToTenant(), spec, destination, sources), nil
}

func (j JobService) validateCyclic(ctx context.Context, rootName job.Name, jobMap map[job.Name]*job.Job) error {
	dagTree, err := j.buildDAGTree(ctx, rootName, jobMap)
	if err != nil {
		return err
	}

	return dagTree.ValidateCyclic()
}

func (j JobService) buildDAGTree(ctx context.Context, rootName job.Name, jobMap map[job.Name]*job.Job) (*tree.MultiRootTree, error) {
	rootJob, ok := jobMap[rootName]
	if !ok {
		return nil, fmt.Errorf("couldn't find any job with name %s", rootName)
	}

	dagTree := tree.NewMultiRootTree()
	dagTree.AddNode(tree.NewTreeNode(rootJob))

	// source: https://github.com/odpf/optimus/blob/a6dafbc1fbeb8e1f1eb8d4a6e9582ada4a7f639e/job/replay.go#L111
	me := errors.NewMultiError("build DAGTree errors")
	for _, childJob := range jobMap {
		childNode := findOrCreateDAGNode(dagTree, childJob)
		for _, source := range childJob.Sources() {
			parents, err := j.GetAll(ctx, filter.WithString(filter.ResourceDestination, source.String()))
			if err != nil {
				me.Append(err)
				continue
			}

			for _, parentJob := range parents {
				if _, ok := jobMap[parentJob.Spec().Name()]; !ok {
					// as of now, we're not providing the capability to build tree for the external job spec. skip
					continue
				}
				parentNode := findOrCreateDAGNode(dagTree, parentJob)
				parentNode.AddDependent(childNode)
				dagTree.AddNode(parentNode)
			}
		}

		if len(childJob.Sources()) == 0 {
			dagTree.MarkRoot(childNode)
		}
	}

	return dagTree, nil
}

// sources: https://github.com/odpf/optimus/blob/a6dafbc1fbeb8e1f1eb8d4a6e9582ada4a7f639e/job/replay.go#L101
func findOrCreateDAGNode(dagTree *tree.MultiRootTree, dag tree.TreeData) *tree.TreeNode {
	node, ok := dagTree.GetNodeByName(dag.GetName())
	if !ok {
		node = tree.NewTreeNode(dag)
		dagTree.AddNode(node)
	}
	return node
}
