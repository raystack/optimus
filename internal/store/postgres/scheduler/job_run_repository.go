package scheduler

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const (
	columnsToStore = `job_name, namespace_name, project_name, scheduled_at, start_time, end_time, status, sla_definition, sla_alert`
	jobRunColumns  = `id, ` + columnsToStore + `, monitoring`
	dbTimeFormat   = "2006-01-02 15:04:05.000000"
)

type JobRunRepository struct {
	db *pgxpool.Pool
}

type jobRun struct {
	ID uuid.UUID

	JobName       string
	NamespaceName string
	ProjectName   string

	ScheduledAt time.Time
	StartTime   time.Time
	EndTime     *time.Time

	Status        string
	SLAAlert      bool
	SLADefinition int64

	CreatedAt time.Time
	UpdatedAt time.Time

	Monitoring json.RawMessage
}

func (j *jobRun) toJobRun() (*scheduler.JobRun, error) {
	t, err := tenant.NewTenant(j.ProjectName, j.NamespaceName)
	if err != nil {
		return nil, err
	}
	state, err := scheduler.StateFromString(j.Status)
	if err != nil {
		return nil, errors.AddErrContext(err, scheduler.EntityJobRun, "invalid job run state in database")
	}
	var monitoring map[string]any
	if j.Monitoring != nil {
		if err := json.Unmarshal(j.Monitoring, &monitoring); err != nil {
			return nil, errors.AddErrContext(err, scheduler.EntityJobRun, "invalid monitoring values in database")
		}
	}
	return &scheduler.JobRun{
		ID:            j.ID,
		JobName:       scheduler.JobName(j.JobName),
		Tenant:        t,
		State:         state,
		ScheduledAt:   j.ScheduledAt,
		SLAAlert:      j.SLAAlert,
		StartTime:     j.StartTime,
		EndTime:       j.EndTime,
		SLADefinition: j.SLADefinition,
		Monitoring:    monitoring,
	}, nil
}

func (j *JobRunRepository) GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error) {
	var jr jobRun
	getJobRunByID := `SELECT ` + jobRunColumns + ` FROM job_run where id = $1`
	err := j.db.QueryRow(ctx, getJobRunByID, id.UUID()).
		Scan(&jr.ID, &jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.Status, &jr.SLADefinition, &jr.SLAAlert, &jr.Monitoring)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for job run id "+id.UUID().String())
		}
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job run", err)
	}
	return jr.toJobRun()
}

func (j *JobRunRepository) GetByScheduledAt(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	var jr jobRun
	// todo: check if `order by created_at desc limit 1` is required
	getJobRunByScheduledAt := `SELECT ` + jobRunColumns + `, created_at FROM job_run j where project_name = $1 and namespace_name = $2 and job_name = $3 and scheduled_at = $4 order by created_at desc limit 1`
	err := j.db.QueryRow(ctx, getJobRunByScheduledAt, t.ProjectName(), t.NamespaceName(), jobName, scheduledAt).
		Scan(&jr.ID, &jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.Status, &jr.SLADefinition, &jr.SLAAlert, &jr.Monitoring, &jr.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for job:"+jobName.String()+" scheduled at: "+scheduledAt.String())
		}
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting run", err)
	}
	return jr.toJobRun()
}

func (j *JobRunRepository) GetByScheduledTimes(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduleTimes []time.Time) ([]*scheduler.JobRun, error) {
	var jobRunList []*scheduler.JobRun
	var scheduledTimesString []string
	for _, scheduleTime := range scheduleTimes {
		scheduledTimesString = append(scheduledTimesString, scheduleTime.UTC().Format(dbTimeFormat))
	}

	getJobRunByScheduledTimesTemp := `SELECT ` + jobRunColumns + `,created_at FROM job_run j where project_name = $1 and namespace_name = $2 and job_name = $3 and scheduled_at in ('` + strings.Join(scheduledTimesString, "', '") + `')`
	rows, err := j.db.Query(ctx, getJobRunByScheduledTimesTemp, t.ProjectName(), t.NamespaceName(), jobName.String())
	if err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs", err)
	}
	for rows.Next() {
		var jr jobRun
		err := rows.Scan(&jr.ID, &jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.Status, &jr.SLADefinition, &jr.SLAAlert, &jr.Monitoring, &jr.CreatedAt)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NotFound(scheduler.EntityJobRun, "no record of job run :"+jobName.String()+" for schedule Times : "+strings.Join(scheduledTimesString, ", "))
			}
			return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs", err)
		}
		jobRun, err := jr.toJobRun()
		if err != nil {
			return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting job runs", err)
		}
		jobRunList = append(jobRunList, jobRun)
	}

	return jobRunList, nil
}

func (j *JobRunRepository) UpdateState(ctx context.Context, jobRunID uuid.UUID, status scheduler.State) error {
	updateJobRun := "update job_run set status = $1, updated_at = NOW() where id = $2"
	_, err := j.db.Exec(ctx, updateJobRun, status, jobRunID)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to update job run", err)
}

func (j *JobRunRepository) Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, status scheduler.State) error {
	updateJobRun := "update job_run set status = $1, end_time = $2, updated_at = NOW() where id = $3"
	_, err := j.db.Exec(ctx, updateJobRun, status, endTime, jobRunID)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to update job run", err)
}

func (j *JobRunRepository) UpdateSLA(ctx context.Context, jobName scheduler.JobName, projectName tenant.ProjectName, scheduleTimes []time.Time) error {
	if len(scheduleTimes) == 0 {
		return nil
	}
	var scheduleTimesListString []string
	for _, scheduleTime := range scheduleTimes {
		scheduleTimesListString = append(scheduleTimesListString, scheduleTime.UTC().Format(dbTimeFormat))
	}
	query := `
update
    job_run
set
    sla_alert = True, updated_at = NOW()
where
    job_name = $1 and project_name = $2 and scheduled_at IN ('` + strings.Join(scheduleTimesListString, "', '") + "')"
	_, err := j.db.Exec(ctx, query, jobName, projectName)

	return errors.WrapIfErr(scheduler.EntityJobRun, "cannot update job Run State as Sla miss", err)
}

func (j *JobRunRepository) UpdateMonitoring(ctx context.Context, jobRunID uuid.UUID, monitoringValues map[string]any) error {
	monitoringBytes, err := json.Marshal(monitoringValues)
	if err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "error marshalling monitoring values", err)
	}
	query := `update job_run set monitoring = $1 where id = $2`
	_, err = j.db.Exec(ctx, query, monitoringBytes, jobRunID)
	return errors.WrapIfErr(scheduler.EntityJobRun, "cannot update monitoring", err)
}

func (j *JobRunRepository) Create(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error {
	// TODO: startTime should be event time
	insertJobRun := `INSERT INTO job_run (` + columnsToStore + `, created_at, updated_at) values ($1, $2, $3, $4, NOW(), null, $5, $6, FALSE, NOW(), NOW()) ON CONFLICT DO NOTHING`
	_, err := j.db.Exec(ctx, insertJobRun, jobName, t.NamespaceName(), t.ProjectName(), scheduledAt, scheduler.StateRunning, slaDefinitionInSec)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to create job run", err)
}

func NewJobRunRepository(pool *pgxpool.Pool) *JobRunRepository {
	return &JobRunRepository{
		db: pool,
	}
}
