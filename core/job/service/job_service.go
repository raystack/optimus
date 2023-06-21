package service

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/goto/salt/log"
	"github.com/kushsharma/parallel"

	"github.com/goto/optimus/core/event"
	"github.com/goto/optimus/core/event/moderator"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/job/service/filter"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/tree"
	"github.com/goto/optimus/internal/telemetry"
	"github.com/goto/optimus/internal/writer"
	"github.com/goto/optimus/sdk/plugin"
)

const (
	ConcurrentTicketPerSec = 50
	ConcurrentLimit        = 100
)

type JobService struct {
	repo JobRepository

	pluginService    PluginService
	upstreamResolver UpstreamResolver
	eventHandler     EventHandler

	tenantDetailsGetter TenantDetailsGetter

	jobDeploymentService JobDeploymentService

	logger log.Logger
}

func NewJobService(repo JobRepository, pluginService PluginService, upstreamResolver UpstreamResolver,
	tenantDetailsGetter TenantDetailsGetter, eventHandler EventHandler, logger log.Logger,
	jobDeploymentService JobDeploymentService,
) *JobService {
	return &JobService{
		repo:                 repo,
		pluginService:        pluginService,
		upstreamResolver:     upstreamResolver,
		eventHandler:         eventHandler,
		tenantDetailsGetter:  tenantDetailsGetter,
		logger:               logger,
		jobDeploymentService: jobDeploymentService,
	}
}

type PluginService interface {
	Info(context.Context, job.TaskName) (*plugin.Info, error)
	GenerateDestination(context.Context, *tenant.WithDetails, job.Task) (job.ResourceURN, error)
	GenerateUpstreams(ctx context.Context, jobTenant *tenant.WithDetails, spec *job.Spec, dryRun bool) ([]job.ResourceURN, error)
}

type TenantDetailsGetter interface {
	GetDetails(ctx context.Context, jobTenant tenant.Tenant) (*tenant.WithDetails, error)
}

type JobDeploymentService interface {
	UploadJobs(ctx context.Context, jobTenant tenant.Tenant, toUpdate, toDelete []string) error
}

type JobRepository interface {
	// TODO: remove `savedJobs` since the method's main purpose is to add, not to get
	Add(context.Context, []*job.Job) (addedJobs []*job.Job, err error)
	Update(context.Context, []*job.Job) (updatedJobs []*job.Job, err error)
	Delete(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, cleanHistory bool) error

	ChangeJobNamespace(ctx context.Context, jobName job.Name, tenant, newTenant tenant.Tenant) error

	GetByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) (*job.Job, error)
	GetAllByResourceDestination(ctx context.Context, resourceDestination job.ResourceURN) ([]*job.Job, error)
	GetAllByTenant(ctx context.Context, jobTenant tenant.Tenant) ([]*job.Job, error)
	GetAllByProjectName(ctx context.Context, projectName tenant.ProjectName) ([]*job.Job, error)

	ResolveUpstreams(context.Context, tenant.ProjectName, []job.Name) (map[job.Name][]*job.Upstream, error)
	ReplaceUpstreams(context.Context, []*job.WithUpstream) error
	GetUpstreams(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Upstream, error)

	GetDownstreamByDestination(ctx context.Context, projectName tenant.ProjectName, destination job.ResourceURN) ([]*job.Downstream, error)
	GetDownstreamByJobName(ctx context.Context, projectName tenant.ProjectName, jobName job.Name) ([]*job.Downstream, error)
}

type EventHandler interface {
	HandleEvent(moderator.Event)
}

type UpstreamResolver interface {
	BulkResolve(ctx context.Context, projectName tenant.ProjectName, jobs []*job.Job, logWriter writer.LogWriter) (jobWithUpstreams []*job.WithUpstream, err error)
	Resolve(ctx context.Context, subjectJob *job.Job, logWriter writer.LogWriter) ([]*job.Upstream, error)
}

func (j *JobService) Add(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) error {
	logWriter := writer.NewLogWriter(j.logger)
	me := errors.NewMultiError("add specs errors")

	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		j.logger.Error("error getting tenant details: %s", err)
		return err
	}

	jobs, err := j.generateJobs(ctx, tenantWithDetails, specs, logWriter)
	me.Append(err)

	addedJobs, err := j.repo.Add(ctx, jobs)
	me.Append(err)

	jobsWithUpstreams, err := j.upstreamResolver.BulkResolve(ctx, jobTenant.ProjectName(), addedJobs, logWriter)
	me.Append(err)

	err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	err = j.uploadJobs(ctx, jobTenant, addedJobs, nil, nil)
	me.Append(err)

	for _, job := range addedJobs {
		j.raiseCreateEvent(job)
	}
	raiseJobEventMetric(jobTenant, job.MetricJobEventStateAdded, len(addedJobs))

	if len(addedJobs) < len(specs) {
		totalFailed := len(specs) - len(addedJobs)
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateUpsertFailed, totalFailed)
	}

	return me.ToErr()
}

func (j *JobService) Update(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec) error {
	logWriter := writer.NewLogWriter(j.logger)
	me := errors.NewMultiError("update specs errors")

	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		j.logger.Error("error getting tenant details: %s", err)
		return err
	}

	jobs, err := j.generateJobs(ctx, tenantWithDetails, specs, logWriter)
	me.Append(err)

	updatedJobs, err := j.repo.Update(ctx, jobs)
	me.Append(err)

	jobsWithUpstreams, err := j.upstreamResolver.BulkResolve(ctx, jobTenant.ProjectName(), updatedJobs, logWriter)
	me.Append(err)

	err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	err = j.uploadJobs(ctx, jobTenant, nil, updatedJobs, nil)
	me.Append(err)

	for _, job := range updatedJobs {
		j.raiseUpdateEvent(job)
	}
	raiseJobEventMetric(jobTenant, job.MetricJobEventStateUpdated, len(updatedJobs))

	if len(updatedJobs) < len(specs) {
		totalFailed := len(specs) - len(updatedJobs)
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateUpsertFailed, totalFailed)
	}

	return me.ToErr()
}

func (j *JobService) Delete(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, cleanFlag, forceFlag bool) (affectedDownstream []job.FullName, err error) {
	downstreamList, err := j.repo.GetDownstreamByJobName(ctx, jobTenant.ProjectName(), jobName)
	if err != nil {
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleteFailed, 1)
		j.logger.Error("error getting downstream jobs for [%s]: %s", jobName, err)
		return nil, err
	}

	downstreamFullNames := job.DownstreamList(downstreamList).GetDownstreamFullNames()

	if len(downstreamList) > 0 && !forceFlag {
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleteFailed, 1)
		errorMsg := fmt.Sprintf("%s depends on this job. consider do force delete to proceed.", downstreamFullNames)
		j.logger.Error(errorMsg)
		return nil, errors.NewError(errors.ErrFailedPrecond, job.EntityJob, errorMsg)
	}

	if err := j.repo.Delete(ctx, jobTenant.ProjectName(), jobName, cleanFlag); err != nil {
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleteFailed, 1)
		j.logger.Error("error deleting job [%s]: %s", jobName, err)
		return downstreamFullNames, err
	}

	raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleted, 1)

	if err := j.uploadJobs(ctx, jobTenant, nil, nil, []job.Name{jobName}); err != nil {
		j.logger.Error("error uploading job [%s]: %s", jobName, err)
		return downstreamFullNames, err
	}

	j.raiseDeleteEvent(jobTenant, jobName)

	return downstreamFullNames, nil
}

func (j *JobService) ChangeNamespace(ctx context.Context, jobTenant, jobNewTenant tenant.Tenant, jobName job.Name) error {
	err := j.repo.ChangeJobNamespace(ctx, jobName, jobTenant, jobNewTenant)
	if err != nil {
		errorsMsg := fmt.Sprintf("unable to successfully finish job namespace change transaction : %s", err.Error())
		return errors.NewError(errors.ErrInternalError, job.EntityJob, errorsMsg)
	}

	newJobSpec, err := j.repo.GetByJobName(ctx, jobNewTenant.ProjectName(), jobName)
	if err != nil {
		errorsMsg := fmt.Sprintf(" unable fetch jobSpecs for newly modified job : %s, namespace: %s, err: %s", jobName, jobNewTenant.NamespaceName(), err.Error())
		return errors.NewError(errors.ErrInternalError, job.EntityJob, errorsMsg)
	}

	err = j.uploadJobs(ctx, jobTenant, nil, nil, []job.Name{jobName})
	if err != nil {
		errorsMsg := fmt.Sprintf(" unable to remove old job : %s", err.Error())
		return errors.NewError(errors.ErrInternalError, job.EntityJob, errorsMsg)
	}

	err = j.uploadJobs(ctx, jobNewTenant, []*job.Job{newJobSpec}, nil, nil)
	if err != nil {
		errorsMsg := fmt.Sprintf(" unable to create new job on scheduler : %s", err.Error())
		return errors.NewError(errors.ErrInternalError, job.EntityJob, errorsMsg)
	}
	j.raiseUpdateEvent(newJobSpec)
	return nil
}

func (j *JobService) Get(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name) (*job.Job, error) {
	jobs, err := j.GetByFilter(ctx,
		filter.WithString(filter.ProjectName, jobTenant.ProjectName().String()),
		filter.WithString(filter.JobName, jobName.String()),
	)
	if err != nil {
		j.logger.Error("error getting job specified by the filter: %s", err)
		return nil, err
	}
	if len(jobs) == 0 {
		j.logger.Error("job [%s] is not found", jobName)
		return nil, errors.NotFound(job.EntityJob, fmt.Sprintf("job %s is not found", jobName))
	}
	return jobs[0], nil
}

func (j *JobService) GetTaskInfo(ctx context.Context, task job.Task) (*plugin.Info, error) {
	return j.pluginService.Info(ctx, task.Name())
}

func (j *JobService) GetByFilter(ctx context.Context, filters ...filter.FilterOpt) ([]*job.Job, error) {
	f := filter.NewFilter(filters...)

	// when resource destination exist, filter by destination
	if f.Contains(filter.ResourceDestination) {
		j.logger.Debug("getting all jobs by resource destination [%s]", f.GetStringValue(filter.ResourceDestination))

		resourceDestination := job.ResourceURN(f.GetStringValue(filter.ResourceDestination))
		return j.repo.GetAllByResourceDestination(ctx, resourceDestination)
	}

	// when project name and job names exist, filter by project and job names
	if f.Contains(filter.ProjectName, filter.JobNames) {
		j.logger.Debug("getting all jobs by project name [%s] and job names", f.GetStringValue(filter.ProjectName))

		me := errors.NewMultiError("get all job specs errors")

		projectName, _ := tenant.ProjectNameFrom(f.GetStringValue(filter.ProjectName))
		jobNames := f.GetStringArrayValue(filter.JobNames)

		var jobs []*job.Job
		for _, jobNameStr := range jobNames {
			jobName, _ := job.NameFrom(jobNameStr)
			fetchedJob, err := j.repo.GetByJobName(ctx, projectName, jobName)
			if err != nil {
				if !errors.IsErrorType(err, errors.ErrNotFound) {
					j.logger.Error("error getting job [%s] from db: %s", jobName, err)
					me.Append(err)
				}
				continue
			}
			jobs = append(jobs, fetchedJob)
		}
		return jobs, me.ToErr()
	}

	// when project name and job name exist, filter by project name and job name
	if f.Contains(filter.ProjectName, filter.JobName) {
		j.logger.Debug("getting all jobs by project name [%s] and job name [%s]", f.GetStringValue(filter.ProjectName), f.GetStringValue(filter.JobName))

		projectName, _ := tenant.ProjectNameFrom(f.GetStringValue(filter.ProjectName))
		jobName, _ := job.NameFrom(f.GetStringValue(filter.JobName))
		fetchedJob, err := j.repo.GetByJobName(ctx, projectName, jobName)
		if err != nil {
			if errors.IsErrorType(err, errors.ErrNotFound) {
				return []*job.Job{}, nil
			}
			j.logger.Error("error getting job [%s] from db: %s", jobName, err)
			return nil, err
		}
		return []*job.Job{fetchedJob}, nil
	}

	// when project name and namespace names exist, filter by tenant
	if f.Contains(filter.ProjectName, filter.NamespaceNames) {
		j.logger.Debug("getting all jobs by project name [%s] and namespace names", f.GetStringValue(filter.ProjectName))

		var jobs []*job.Job
		namespaceNames := f.GetStringArrayValue(filter.NamespaceNames)
		for _, namespaceName := range namespaceNames {
			jobTenant, err := tenant.NewTenant(f.GetStringValue(filter.ProjectName), namespaceName)
			if err != nil {
				j.logger.Error("invalid tenant request information project [%s] namespace [%s]: %s", f.GetStringValue(filter.ProjectName), f.GetStringValue(filter.NamespaceName), err)
				return nil, err
			}
			tenantJobs, err := j.repo.GetAllByTenant(ctx, jobTenant)
			if err != nil {
				j.logger.Error("error getting all jobs under project [%s] namespace [%s]: %s", jobTenant.ProjectName().String(), jobTenant.NamespaceName().String(), err)
				return nil, err
			}
			jobs = append(jobs, tenantJobs...)
		}
		return jobs, nil
	}

	// when project name and namespace name exist, filter by tenant
	if f.Contains(filter.ProjectName, filter.NamespaceName) {
		j.logger.Debug("getting all jobs by project name [%s] and namespace name [%s]", f.GetStringValue(filter.ProjectName), f.GetStringValue(filter.NamespaceName))

		jobTenant, err := tenant.NewTenant(f.GetStringValue(filter.ProjectName), f.GetStringValue(filter.NamespaceName))
		if err != nil {
			j.logger.Error("invalid tenant request information project [%s] namespace [%s]: %s", f.GetStringValue(filter.ProjectName), f.GetStringValue(filter.NamespaceName), err)
			return nil, err
		}
		return j.repo.GetAllByTenant(ctx, jobTenant)
	}

	// when project name exist, filter by project name
	if f.Contains(filter.ProjectName) {
		j.logger.Debug("getting all jobs by project name [%s]", f.GetStringValue(filter.ProjectName))

		projectName, _ := tenant.ProjectNameFrom(f.GetStringValue(filter.ProjectName))
		return j.repo.GetAllByProjectName(ctx, projectName)
	}

	j.logger.Error("filter combination is not recognized")
	return nil, fmt.Errorf("no filter matched")
}

func (j *JobService) ReplaceAll(ctx context.Context, jobTenant tenant.Tenant, specs []*job.Spec, jobNamesWithInvalidSpec []job.Name, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("replace all specs errors")

	existingJobs, err := j.repo.GetAllByTenant(ctx, jobTenant)
	me.Append(err)

	toAdd, toUpdate, toDelete, _, err := j.differentiateSpecs(existingJobs, specs, jobNamesWithInvalidSpec)
	logWriter.Write(writer.LogLevelInfo, fmt.Sprintf("[%s] found %d new, %d modified, and %d deleted job specs", jobTenant.NamespaceName().String(), len(toAdd), len(toUpdate), len(toDelete)))
	me.Append(err)

	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		j.logger.Error("error getting tenant details: %s", err)
		me.Append(err)
		return me.ToErr()
	}

	addedJobs, err := j.bulkAdd(ctx, tenantWithDetails, toAdd, logWriter)
	me.Append(err)
	failedToAdd := len(toAdd) - len(addedJobs)

	updatedJobs, err := j.bulkUpdate(ctx, tenantWithDetails, toUpdate, logWriter)
	me.Append(err)
	failedToUpdate := len(toUpdate) - len(updatedJobs)

	deletedJobNames, err := j.bulkDelete(ctx, jobTenant, toDelete, logWriter)
	me.Append(err)

	err = j.resolveAndSaveUpstreams(ctx, jobTenant, logWriter, addedJobs, updatedJobs)
	me.Append(err)

	err = j.uploadJobs(ctx, jobTenant, addedJobs, updatedJobs, deletedJobNames)
	me.Append(err)

	raiseJobEventMetric(tenantWithDetails.ToTenant(), job.MetricJobEventStateUpsertFailed, failedToAdd+failedToUpdate)

	return me.ToErr()
}

func (j *JobService) uploadJobs(ctx context.Context, jobTenant tenant.Tenant, addedJobs, updatedJobs []*job.Job, deletedJobNames []job.Name) error {
	if len(addedJobs) == 0 && len(updatedJobs) == 0 && len(deletedJobNames) == 0 {
		j.logger.Warn("no jobs to be uploaded")
		return nil
	}

	var jobNamesToUpload, jobNamesToRemove []string
	for _, addedJob := range append(addedJobs, updatedJobs...) {
		jobNamesToUpload = append(jobNamesToUpload, addedJob.GetName())
	}

	for _, deletedJobName := range deletedJobNames {
		jobNamesToRemove = append(jobNamesToRemove, deletedJobName.String())
	}

	return j.jobDeploymentService.UploadJobs(ctx, jobTenant, jobNamesToUpload, jobNamesToRemove)
}

func (j *JobService) Refresh(ctx context.Context, projectName tenant.ProjectName, namespaceNames, jobNames []string, logWriter writer.LogWriter) (err error) {
	projectFilter := filter.WithString(filter.ProjectName, projectName.String())
	namespacesFilter := filter.WithStringArray(filter.NamespaceNames, namespaceNames)
	jobNamesFilter := filter.WithStringArray(filter.JobNames, jobNames)

	allJobs, err := j.GetByFilter(ctx, projectFilter, namespacesFilter, jobNamesFilter)
	if err != nil {
		j.logger.Error("error getting jobs by filter: %s", err)
		return err
	}

	me := errors.NewMultiError("refresh all specs errors")
	namespaceAndJobsMap := job.Jobs(allJobs).GetNamespaceNameAndJobsMap()
	for namespaceName, jobs := range namespaceAndJobsMap {
		jobTenant, err := tenant.NewTenant(projectName.String(), namespaceName.String())
		if err != nil {
			j.logger.Error("invalid tenant information requet project [%s] namespace [%s]: %s", projectName.String(), namespaceName.String(), err)
			me.Append(err)
			continue
		}

		tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
		if err != nil {
			j.logger.Error("error getting tenant details: %s", err)
			me.Append(err)
			continue
		}

		specs := job.Jobs(jobs).GetSpecs()
		updatedJobs, err := j.bulkUpdate(ctx, tenantWithDetails, specs, logWriter)
		me.Append(err)

		j.logger.Debug("resolving upstreams for [%d] jobs of project [%s] namespace [%s]", len(updatedJobs), projectName, namespaceName)
		jobsWithUpstreams, err := j.upstreamResolver.BulkResolve(ctx, projectName, updatedJobs, logWriter)
		me.Append(err)

		err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
		me.Append(err)
	}

	return me.ToErr()
}

func (j *JobService) Validate(ctx context.Context, jobTenant tenant.Tenant, jobSpecs []*job.Spec, jobNamesWithInvalidSpec []job.Name, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("validate specs errors")

	tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
	if err != nil {
		j.logger.Error("error getting tenant details: %s", err)
		return err
	}

	existingJobs, err := j.repo.GetAllByTenant(ctx, jobTenant)
	me.Append(err)

	toAdd, toUpdate, toDelete, unmodifiedSpecs, err := j.differentiateSpecs(existingJobs, jobSpecs, jobNamesWithInvalidSpec)
	logWriter.Write(writer.LogLevelInfo, fmt.Sprintf("[%s] found %d new, %d modified, and %d deleted job specs", jobTenant.NamespaceName().String(), len(toAdd), len(toUpdate), len(toDelete)))
	me.Append(err)

	incomingJobs, err := j.generateJobs(ctx, tenantWithDetails, append(toAdd, toUpdate...), logWriter)
	me.Append(err)

	err = j.validateDeleteJobs(ctx, jobTenant, toDelete, logWriter)
	me.Append(err)

	// NOTE: only check cyclic deps across internal upstreams (sources), need further discussion to check cyclic deps for external upstream
	// assumption, all job specs from input are also the job within same project
	jobsToValidateMap := getAllJobsToValidateMap(incomingJobs, existingJobs, unmodifiedSpecs)
	identifierToJobsMap := getIdentifierToJobsMap(jobsToValidateMap)
	for _, jobEntity := range jobsToValidateMap {
		if _, err := j.validateCyclic(jobEntity.Job().Spec().Name(), jobsToValidateMap, identifierToJobsMap); err != nil {
			j.logger.Error("error when executing cyclic validation on [%s]: %s", jobEntity.Job().Spec().Name(), err)
			me.Append(err)
			break
		}
	}

	return me.ToErr()
}

func (j *JobService) validateDeleteJobs(ctx context.Context, jobTenant tenant.Tenant, toDelete []*job.Spec, logWriter writer.LogWriter) error {
	me := errors.NewMultiError("delete job specs check errors")
	toDeleteMap := job.Specs(toDelete).ToFullNameAndSpecMap(jobTenant.ProjectName())

	for _, jobToDelete := range toDelete {
		downstreams, err := j.getAllDownstreams(ctx, jobTenant.ProjectName(), jobToDelete.Name(), map[job.FullName]bool{})
		if err != nil {
			j.logger.Error("error getting all downstreams for job [%s]: %s", jobToDelete.Name().String(), err)
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] pre-delete check for job %s failed: %s", jobTenant.NamespaceName().String(), jobToDelete.Name().String(), err.Error()))
			me.Append(err)
			continue
		}
		validateDeleteJob(jobTenant, downstreams, toDeleteMap, jobToDelete, logWriter, me)
	}
	return me.ToErr()
}

func validateDeleteJob(jobTenant tenant.Tenant, downstreams []*job.Downstream, toDeleteMap map[job.FullName]*job.Spec, jobToDelete *job.Spec, logWriter writer.LogWriter, me *errors.MultiError) bool {
	notDeleted, safeToDelete := isJobSafeToDelete(toDeleteMap, job.DownstreamList(downstreams).GetDownstreamFullNames())

	if !safeToDelete {
		// TODO: refactor to put the log writer outside
		errorMsg := fmt.Sprintf("deletion of job %s will fail. job is being used by %s", jobToDelete.Name().String(), job.FullNames(notDeleted).String())
		logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] %s", jobTenant.NamespaceName().String(), errorMsg))
		me.Append(errors.NewError(errors.ErrFailedPrecond, job.EntityJob, errorMsg))
		return false
	}

	return true
}

func isJobSafeToDelete(toDeleteMap map[job.FullName]*job.Spec, downstreamFullNames []job.FullName) ([]job.FullName, bool) {
	notDeleted := []job.FullName{}
	for _, downstreamFullName := range downstreamFullNames {
		if _, ok := toDeleteMap[downstreamFullName]; !ok {
			notDeleted = append(notDeleted, downstreamFullName)
		}
	}

	return notDeleted, len(notDeleted) == 0
}

func (j *JobService) getAllDownstreams(ctx context.Context, projectName tenant.ProjectName, jobName job.Name, visited map[job.FullName]bool) ([]*job.Downstream, error) {
	currentJobFullName := job.FullNameFrom(projectName, jobName)
	downstreams := []*job.Downstream{}
	visited[currentJobFullName] = true
	childJobs, err := j.repo.GetDownstreamByJobName(ctx, projectName, jobName)
	if err != nil {
		j.logger.Error("error getting downstream jobs for job [%s]: %s", jobName, err)
		return nil, err
	}
	for _, childJob := range childJobs {
		downstreams = append(downstreams, childJob)
		if visited[childJob.FullName()] {
			continue
		}
		childDownstreams, err := j.getAllDownstreams(ctx, childJob.ProjectName(), childJob.Name(), visited)
		if err != nil {
			j.logger.Error("error getting all downstreams for job [%s]: %s", childJob.Name(), err)
			return nil, err
		}
		downstreams = append(downstreams, childDownstreams...)
	}
	return downstreams, nil
}

func getAllJobsToValidateMap(incomingJobs, existingJobs []*job.Job, unmodifiedSpecs []*job.Spec) map[job.Name]*job.WithUpstream {
	// TODO: check whether we need to accumulate encountered errors
	me := errors.NewMultiError("validate specs errors")

	existingJobMap := job.Jobs(existingJobs).GetNameAndJobMap()
	var unmodifiedJobs []*job.Job
	for _, unmodifiedSpec := range unmodifiedSpecs {
		if unmodifiedJob, ok := existingJobMap[unmodifiedSpec.Name()]; ok {
			unmodifiedJobs = append(unmodifiedJobs, unmodifiedJob)
			continue
		}
		errorsMsg := fmt.Sprintf("unable to validate existing job %s", unmodifiedSpec.Name().String())
		me.Append(errors.NewError(errors.ErrInternalError, job.EntityJob, errorsMsg))
	}

	jobsToValidateMap := make(map[job.Name]*job.WithUpstream)
	for _, jobToValidate := range append(incomingJobs, unmodifiedJobs...) {
		jobWithUpstream, err := jobToValidate.GetJobWithUnresolvedUpstream()
		if err != nil {
			me.Append(err)
			continue
		}
		jobsToValidateMap[jobToValidate.Spec().Name()] = jobWithUpstream
	}
	return jobsToValidateMap
}

func getIdentifierToJobsMap(jobsToValidateMap map[job.Name]*job.WithUpstream) map[string][]*job.WithUpstream {
	identifierToJobsMap := make(map[string][]*job.WithUpstream)
	for _, jobEntity := range jobsToValidateMap {
		jobIdentifiers := []string{jobEntity.Job().FullName()}
		if jobDestination := jobEntity.Job().Destination().String(); jobDestination != "" {
			jobIdentifiers = append(jobIdentifiers, jobDestination)
		}
		for _, jobIdentifier := range jobIdentifiers {
			if _, ok := identifierToJobsMap[jobIdentifier]; !ok {
				identifierToJobsMap[jobIdentifier] = []*job.WithUpstream{}
			}
			identifierToJobsMap[jobIdentifier] = append(identifierToJobsMap[jobIdentifier], jobEntity)
		}
	}
	return identifierToJobsMap
}

func (j *JobService) resolveAndSaveUpstreams(ctx context.Context, jobTenant tenant.Tenant, logWriter writer.LogWriter, jobsToResolve ...[]*job.Job) error {
	var allJobsToResolve []*job.Job
	for _, group := range jobsToResolve {
		allJobsToResolve = append(allJobsToResolve, group...)
	}
	if len(allJobsToResolve) == 0 {
		j.logger.Warn("no jobs to be resolved")
		return nil
	}

	me := errors.NewMultiError("resolve and save upstream errors")

	j.logger.Debug("resolving upstreams for %d jobs of project [%s] namespace [%s]", len(allJobsToResolve), jobTenant.ProjectName(), jobTenant.NamespaceName())
	jobsWithUpstreams, err := j.upstreamResolver.BulkResolve(ctx, jobTenant.ProjectName(), allJobsToResolve, logWriter)
	me.Append(err)

	j.logger.Debug("replacing upstreams for %d jobs of project [%s] namespace [%s]", len(jobsWithUpstreams), jobTenant.ProjectName(), jobTenant.NamespaceName())
	err = j.repo.ReplaceUpstreams(ctx, jobsWithUpstreams)
	me.Append(err)

	return me.ToErr()
}

func (j *JobService) bulkAdd(ctx context.Context, tenantWithDetails *tenant.WithDetails, specsToAdd []*job.Spec, logWriter writer.LogWriter) ([]*job.Job, error) {
	me := errors.NewMultiError("bulk add specs errors")

	jobsToAdd, err := j.generateJobs(ctx, tenantWithDetails, specsToAdd, logWriter)
	me.Append(err)

	if len(jobsToAdd) == 0 {
		j.logger.Warn("no jobs to be added")
		return nil, me.ToErr()
	}

	// TODO: consider do add inside parallel
	addedJobs, err := j.repo.Add(ctx, jobsToAdd)
	if err != nil {
		j.logger.Error("error adding jobs for namespace [%s]: %s", tenantWithDetails.Namespace().Name(), err)
		logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] add jobs failure found: %s", tenantWithDetails.Namespace().Name().String(), err.Error()))
		me.Append(err)
	}

	if len(addedJobs) > 0 {
		logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] successfully added %d jobs", tenantWithDetails.Namespace().Name().String(), len(addedJobs)))
		for _, job := range addedJobs {
			j.raiseCreateEvent(job)
		}
		raiseJobEventMetric(tenantWithDetails.ToTenant(), job.MetricJobEventStateAdded, len(addedJobs))
	}

	return addedJobs, me.ToErr()
}

func (j *JobService) bulkUpdate(ctx context.Context, tenantWithDetails *tenant.WithDetails, specsToUpdate []*job.Spec, logWriter writer.LogWriter) ([]*job.Job, error) {
	me := errors.NewMultiError("bulk update specs errors")

	jobsToUpdate, err := j.generateJobs(ctx, tenantWithDetails, specsToUpdate, logWriter)
	me.Append(err)

	if len(jobsToUpdate) == 0 {
		j.logger.Warn("no jobs to be updated")
		return nil, me.ToErr()
	}

	updatedJobs, err := j.repo.Update(ctx, jobsToUpdate)
	if err != nil {
		j.logger.Error("error updating jobs for namespace [%s]: %s", tenantWithDetails.Namespace().Name(), err)
		logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] update jobs failure found: %s", tenantWithDetails.Namespace().Name().String(), err.Error()))
		me.Append(err)
	}

	if len(updatedJobs) > 0 {
		logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] successfully updated %d jobs", tenantWithDetails.Namespace().Name().String(), len(updatedJobs)))
		for _, job := range updatedJobs {
			j.raiseUpdateEvent(job)
		}
		raiseJobEventMetric(tenantWithDetails.ToTenant(), job.MetricJobEventStateUpdated, len(updatedJobs))
	}

	return updatedJobs, me.ToErr()
}

func (j *JobService) bulkDelete(ctx context.Context, jobTenant tenant.Tenant, toDelete []*job.Spec, logWriter writer.LogWriter) ([]job.Name, error) {
	me := errors.NewMultiError("bulk delete specs errors")
	var deletedJobNames []job.Name
	toDeleteMap := job.Specs(toDelete).ToFullNameAndSpecMap(jobTenant.ProjectName())

	alreadyDeleted := map[job.FullName]bool{}
	for _, spec := range toDelete {
		// TODO: reuse Delete method and pass forceFlag as false
		fullName := job.FullNameFrom(jobTenant.ProjectName(), spec.Name())
		downstreams, err := j.getAllDownstreams(ctx, jobTenant.ProjectName(), spec.Name(), map[job.FullName]bool{})
		if err != nil {
			j.logger.Error("error getting downstreams for job [%s]: %s", spec.Name(), err)
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] pre-delete check for job %s failed: %s", jobTenant.NamespaceName().String(), spec.Name().String(), err.Error()))
			me.Append(err)
			continue
		}

		isSafeToDelete := validateDeleteJob(jobTenant, downstreams, toDeleteMap, spec, logWriter, me)
		if !isSafeToDelete {
			j.logger.Warn("job [%s] is not safe to be deleted", spec.Name())
			continue
		}

		logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] deleting job %s", jobTenant.NamespaceName().String(), spec.Name().String()))

		isDeletionFail := false
		for i := len(downstreams) - 1; i >= 0 && !isDeletionFail; i-- {
			if alreadyDeleted[downstreams[i].FullName()] {
				continue
			}
			if err = j.repo.Delete(ctx, downstreams[i].ProjectName(), downstreams[i].Name(), false); err != nil {
				j.logger.Error("error deleting [%s] as downstream of [%s]", downstreams[i].Name(), spec.Name())
				logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] deleting job %s failed: %s", downstreams[i].NamespaceName().String(), downstreams[i].Name().String(), err.Error()))
				me.Append(err)
				isDeletionFail = true
			} else {
				alreadyDeleted[downstreams[i].FullName()] = true
				j.raiseDeleteEvent(jobTenant, spec.Name())
				raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleted, 1)
				deletedJobNames = append(deletedJobNames, downstreams[i].Name())
			}
		}

		if alreadyDeleted[fullName] || isDeletionFail {
			j.logger.Warn("job [%s] deletion is skipped [already deleted or failure in deleting downstreams]", spec.Name())
			continue
		}
		if err = j.repo.Delete(ctx, jobTenant.ProjectName(), spec.Name(), false); err != nil {
			j.logger.Error("error deleting job [%s]", spec.Name())
			logWriter.Write(writer.LogLevelError, fmt.Sprintf("[%s] deleting job %s failed: %s", jobTenant.NamespaceName().String(), spec.Name().String(), err.Error()))
			me.Append(err)
		} else {
			alreadyDeleted[fullName] = true
			j.raiseDeleteEvent(jobTenant, spec.Name())
			raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleted, 1)
			deletedJobNames = append(deletedJobNames, spec.Name())
		}
	}

	if len(deletedJobNames) > 0 {
		logWriter.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] successfully deleted %d jobs", jobTenant.NamespaceName().String(), len(deletedJobNames)))
	}

	if len(deletedJobNames) < len(toDelete) {
		totalFailed := len(toDelete) - len(deletedJobNames)
		raiseJobEventMetric(jobTenant, job.MetricJobEventStateDeleteFailed, totalFailed)
	}
	return deletedJobNames, me.ToErr()
}

func (*JobService) differentiateSpecs(existingJobs []*job.Job, specs []*job.Spec, jobNamesWithInvalidSpec []job.Name) (added, modified, deleted, unmodified []*job.Spec, err error) {
	// TODO: consider checking multi-error if it is required here
	me := errors.NewMultiError("differentiate specs errors")

	var addedSpecs, modifiedSpecs, unmodifiedSpecs, deletedSpecs []*job.Spec

	existingSpecsMap := job.Jobs(existingJobs).GetNameAndSpecMap()
	for _, jobNameToSkip := range jobNamesWithInvalidSpec {
		delete(existingSpecsMap, jobNameToSkip)
	}

	for _, incomingSpec := range specs {
		if spec, ok := existingSpecsMap[incomingSpec.Name()]; !ok {
			addedSpecs = append(addedSpecs, incomingSpec)
		} else if !reflect.DeepEqual(spec, incomingSpec) {
			modifiedSpecs = append(modifiedSpecs, incomingSpec)
		} else {
			unmodifiedSpecs = append(unmodifiedSpecs, incomingSpec)
		}
	}

	incomingSpecsMap := job.Specs(specs).ToNameAndSpecMap()
	for existingJobName, existingJobSpec := range existingSpecsMap {
		if _, ok := incomingSpecsMap[existingJobName]; !ok {
			deletedSpecs = append(deletedSpecs, existingJobSpec)
		}
	}
	return addedSpecs, modifiedSpecs, deletedSpecs, unmodifiedSpecs, me.ToErr()
}

func (j *JobService) generateJobs(ctx context.Context, tenantWithDetails *tenant.WithDetails, specs []*job.Spec, logWriter writer.LogWriter) ([]*job.Job, error) {
	me := errors.NewMultiError("bulk generate jobs errors")

	runner := parallel.NewRunner(parallel.WithTicket(ConcurrentTicketPerSec), parallel.WithLimit(ConcurrentLimit))
	for _, spec := range specs {
		runner.Add(func(currentSpec *job.Spec, lw writer.LogWriter) func() (interface{}, error) {
			return func() (interface{}, error) {
				generatedJob, err := j.generateJob(ctx, tenantWithDetails, currentSpec)
				if err != nil {
					j.logger.Error("error generating job [%s]: %s", currentSpec.Name(), err)
					lw.Write(writer.LogLevelError, fmt.Sprintf("[%s] unable to generate job %s: %s", tenantWithDetails.Namespace().Name().String(), currentSpec.Name().String(), err.Error()))
					return nil, err
				}
				lw.Write(writer.LogLevelDebug, fmt.Sprintf("[%s] processing job %s", tenantWithDetails.Namespace().Name().String(), currentSpec.Name().String()))
				return generatedJob, nil
			}
		}(spec, logWriter))
	}

	var generatedJobs []*job.Job
	for _, result := range runner.Run() {
		if result.Err != nil {
			me.Append(result.Err)
		} else {
			specVal := result.Val.(*job.Job)
			generatedJobs = append(generatedJobs, specVal)
		}
	}
	return generatedJobs, me.ToErr()
}

func (j *JobService) generateJob(ctx context.Context, tenantWithDetails *tenant.WithDetails, spec *job.Spec) (*job.Job, error) {
	destination, err := j.pluginService.GenerateDestination(ctx, tenantWithDetails, spec.Task())
	if err != nil && !errors.Is(err, ErrUpstreamModNotFound) {
		j.logger.Error("error generating destination for [%s]: %s", spec.Name(), err)
		errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
		return nil, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
	}

	sources, err := j.pluginService.GenerateUpstreams(ctx, tenantWithDetails, spec, true)
	if err != nil && !errors.Is(err, ErrUpstreamModNotFound) {
		j.logger.Error("error generating upstream for [%s]: %s", spec.Name(), err)
		errorMsg := fmt.Sprintf("unable to add %s: %s", spec.Name().String(), err.Error())
		return nil, errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg)
	}

	return job.NewJob(tenantWithDetails.ToTenant(), spec, destination, sources), nil
}

func (j *JobService) validateCyclic(rootName job.Name, jobMap map[job.Name]*job.WithUpstream, identifierToJobMap map[string][]*job.WithUpstream) ([]string, error) {
	dagTree := j.buildDAGTree(rootName, jobMap, identifierToJobMap)
	return dagTree.ValidateCyclic()
}

func (*JobService) buildDAGTree(rootName job.Name, jobMap map[job.Name]*job.WithUpstream, identifierToJobMap map[string][]*job.WithUpstream) *tree.MultiRootTree {
	rootJob := jobMap[rootName]

	dagTree := tree.NewMultiRootTree()
	dagTree.AddNode(tree.NewTreeNode(rootJob))

	for _, childJob := range jobMap {
		childNode := findOrCreateDAGNode(dagTree, childJob)
		for _, upstream := range childJob.Upstreams() {
			identifier := upstream.Resource().String()
			if _, ok := identifierToJobMap[identifier]; !ok {
				identifier = upstream.FullName()
				if _, ok := identifierToJobMap[identifier]; !ok {
					// resource maybe from external optimus or outside project,
					// as of now, we're not providing the capability to build tree from external optimus or outside project. skip
					continue
				}
			}

			parents := identifierToJobMap[identifier]
			for _, parentJob := range parents {
				parentNode := findOrCreateDAGNode(dagTree, parentJob)
				parentNode.AddDependent(childNode)
				dagTree.AddNode(parentNode)
			}
		}

		if len(childJob.Upstreams()) == 0 {
			dagTree.MarkRoot(childNode)
		}
	}

	return dagTree
}

// sources: https://github.com/goto/optimus/blob/a6dafbc1fbeb8e1f1eb8d4a6e9582ada4a7f639e/job/replay.go#L101
func findOrCreateDAGNode(dagTree *tree.MultiRootTree, dag tree.TreeData) *tree.TreeNode {
	node, ok := dagTree.GetNodeByName(dag.GetName())
	if !ok {
		node = tree.NewTreeNode(dag)
		dagTree.AddNode(node)
	}
	return node
}

func (j *JobService) GetJobBasicInfo(ctx context.Context, jobTenant tenant.Tenant, jobName job.Name, spec *job.Spec) (*job.Job, writer.BufferedLogger) {
	var subjectJob *job.Job
	var logger writer.BufferedLogger
	var err error
	if spec != nil {
		tenantWithDetails, err := j.tenantDetailsGetter.GetDetails(ctx, jobTenant)
		if err != nil {
			j.logger.Info("error getting tenant details: %s", err)
			logger.Write(writer.LogLevelError, fmt.Sprintf("unable to get tenant detail, err: %v", err))
			return nil, logger
		}
		subjectJob, err = j.generateJob(ctx, tenantWithDetails, spec)
		if err != nil {
			j.logger.Info("error generating job for [%s]: %s", spec.Name(), err)
			logger.Write(writer.LogLevelError, fmt.Sprintf("unable to generate job, err: %v", err))
			return nil, logger
		}
	} else {
		subjectJob, err = j.Get(ctx, jobTenant, jobName)
		if err != nil {
			j.logger.Info("error getting job [%s]: %s", jobName, err)
			logger.Write(writer.LogLevelError, fmt.Sprintf("unable to get job, err: %v", err))
			return nil, logger
		}
	}

	if len(subjectJob.Sources()) == 0 {
		j.logger.Warn("no job sources detected")
		logger.Write(writer.LogLevelInfo, "no job sources detected")
	}

	if dupDestJobNames, err := j.getJobNamesWithSameDestination(ctx, subjectJob); err != nil {
		logger.Write(writer.LogLevelError, "could not perform duplicate job destination check, err: "+err.Error())
	} else if dupDestJobNames != "" {
		logger.Write(writer.LogLevelWarning, "job already exists with same Destination: "+subjectJob.Destination().String()+" existing jobNames: "+dupDestJobNames)
	}

	return subjectJob, logger
}

func (j *JobService) getJobNamesWithSameDestination(ctx context.Context, subjectJob *job.Job) (string, error) {
	sameDestinationJobs, err := j.repo.GetAllByResourceDestination(ctx, subjectJob.Destination())
	if err != nil {
		j.logger.Error("error getting all jobs by destination [%s]: %s", subjectJob.Destination(), err)
		return "", err
	}
	var jobNames []string
	for _, sameDestinationJob := range sameDestinationJobs {
		if sameDestinationJob.FullName() == subjectJob.FullName() {
			continue
		}
		jobNames = append(jobNames, sameDestinationJob.GetName())
	}
	return strings.Join(jobNames, ", "), nil
}

func (j *JobService) GetUpstreamsToInspect(ctx context.Context, subjectJob *job.Job, localJob bool) ([]*job.Upstream, error) {
	logWriter := writer.NewLogWriter(j.logger)
	if localJob {
		return j.upstreamResolver.Resolve(ctx, subjectJob, logWriter)
	}
	return j.repo.GetUpstreams(ctx, subjectJob.ProjectName(), subjectJob.Spec().Name())
}

func (j *JobService) GetDownstream(ctx context.Context, subjectJob *job.Job, localJob bool) ([]*job.Downstream, error) {
	if localJob {
		return j.repo.GetDownstreamByDestination(ctx, subjectJob.ProjectName(), subjectJob.Destination())
	}
	return j.repo.GetDownstreamByJobName(ctx, subjectJob.ProjectName(), subjectJob.Spec().Name())
}

func (j *JobService) raiseCreateEvent(job *job.Job) {
	jobEvent, err := event.NewJobCreatedEvent(job)
	if err != nil {
		j.logger.Error("error creating event for job create: %s", err)
		return
	}
	j.eventHandler.HandleEvent(jobEvent)
}

func (j *JobService) raiseUpdateEvent(job *job.Job) {
	jobEvent, err := event.NewJobUpdateEvent(job)
	if err != nil {
		j.logger.Error("error creating event for job update: %s", err)
		return
	}
	j.eventHandler.HandleEvent(jobEvent)
}

func (j *JobService) raiseDeleteEvent(tnnt tenant.Tenant, jobName job.Name) {
	jobEvent, err := event.NewJobDeleteEvent(tnnt, jobName)
	if err != nil {
		j.logger.Error("error creating event for job delete: %s", err)
		return
	}
	j.eventHandler.HandleEvent(jobEvent)
}

func raiseJobEventMetric(jobTenant tenant.Tenant, state string, metricValue int) {
	telemetry.NewCounter(job.MetricJobEvent, map[string]string{
		"project":   jobTenant.ProjectName().String(),
		"namespace": jobTenant.NamespaceName().String(),
		"status":    state,
	}).Add(float64(metricValue))
}
