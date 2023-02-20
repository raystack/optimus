package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	columnsToStore = `job_name, namespace_name, project_name, scheduled_at, start_time, end_time, status, sla_definition, sla_alert`
	jobRunColumns  = `id, ` + columnsToStore + `, monitoring`
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
	EndTime     time.Time

	Status        string
	SLAAlert      bool
	SLADefinition int64

	CreatedAt time.Time
	UpdatedAt time.Time

	Monitoring json.RawMessage
}

func (j jobRun) toJobRun() (*scheduler.JobRun, error) {
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
		ID:         j.ID,
		JobName:    scheduler.JobName(j.JobName),
		Tenant:     t,
		State:      state,
		StartTime:  j.StartTime,
		SLAAlert:   j.SLAAlert,
		EndTime:    j.EndTime,
		Monitoring: monitoring,
	}, nil
}

func (j *JobRunRepository) GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error) {
	var jr jobRun
	getJobRunByID := `SELECT ` + jobRunColumns + ` FROM job_run where id = $1`
	err := j.db.QueryRow(ctx, getJobRunByID, id.UUID()).
		Scan(&jr.ID, &jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.Status, &jr.SLADefinition, &jr.SLAAlert, &jr.Monitoring)
	if err != nil {
		return nil, err
	}
	return jr.toJobRun()
}

func (j *JobRunRepository) GetByScheduledAt(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	var jr jobRun
	getJobRunByID := `SELECT ` + jobRunColumns + `, created_at FROM job_run j where project_name = $1 and namespace_name = $2 and job_name = $3 and scheduled_at = $4 order by created_at desc limit 1`
	err := j.db.QueryRow(ctx, getJobRunByID, t.ProjectName(), t.NamespaceName(), jobName, scheduledAt).
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

func (j *JobRunRepository) Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, status scheduler.State) error {
	updateJobRun := "update job_run set status = $1, end_time = $2, updated_at = NOW() where id = $3"
	_, err := j.db.Exec(ctx, updateJobRun, status, endTime, jobRunID)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to update job run", err)
}

func (j *JobRunRepository) UpdateSLA(ctx context.Context, slaObjects []*scheduler.SLAObject) error {
	var jobIDListString string
	totalIds := len(slaObjects)
	for i, slaObject := range slaObjects {
		jobIDListString += fmt.Sprintf("('%s','%s')", slaObject.JobName, slaObject.JobScheduledAt.UTC().Format("2006-01-02 15:04:05.000000"))
		if !(i == totalIds-1) {
			jobIDListString += ", "
		}
	}
	query := "update job_run set sla_alert = True, updated_at = NOW() where (job_name, scheduled_at) IN (" + jobIDListString + ")"
	_, err := j.db.Exec(ctx, query)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to update SLA", err)
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
	insertJobRun := `INSERT INTO job_run (` + columnsToStore + `, created_at, updated_at) values ($1, $2, $3, $4, NOW(), TIMESTAMP '3000-01-01 00:00:00', $5, $6, FALSE, NOW(), NOW())`
	_, err := j.db.Exec(ctx, insertJobRun, jobName, t.NamespaceName(), t.ProjectName(), scheduledAt, scheduler.StateRunning, slaDefinitionInSec)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to create job run", err)
}

func NewJobRunRepository(pool *pgxpool.Pool) *JobRunRepository {
	return &JobRunRepository{
		db: pool,
	}
}
