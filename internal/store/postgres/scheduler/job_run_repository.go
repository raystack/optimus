package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

type JobRunRepository struct {
	pool *pgxpool.Pool
}

type jobRun struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	JobName       string
	NamespaceName string
	ProjectName   string

	ScheduledAt time.Time `gorm:"not null"`
	StartTime   time.Time `gorm:"not null"`
	EndTime     time.Time `gorm:"default:TIMESTAMP '3000-01-01 00:00:00'"`

	Status        string
	SLADefinition int64

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

func (j jobRun) toJobRun() (*scheduler.JobRun, error) {
	t, err := tenant.NewTenant(j.ProjectName, j.NamespaceName)
	if err != nil {
		return nil, err
	}
	return &scheduler.JobRun{
		ID:        j.ID,
		JobName:   scheduler.JobName(j.JobName),
		Tenant:    t,
		StartTime: j.StartTime,
	}, nil
}

func (j *JobRunRepository) GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error) {
	var jr jobRun
	getJobRunByID := `SELECT  job_name, namespace_name, project_name, scheduled_at, start_time, end_time, status, sla_definition FROM job_run where id = $1`
	err := j.pool.QueryRow(ctx, getJobRunByID, id).
		Scan(&jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.Status, &jr.SLADefinition)
	if err != nil {
		return nil, err
	}
	return jr.toJobRun()
}

func (j *JobRunRepository) GetByScheduledAt(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	var jr jobRun
	getJobRunByID := `SELECT id, job_name, namespace_name, project_name, scheduled_at, start_time, end_time, status, sla_definition FROM job_run j where project_name = $1 and namespace_name = $2 and job_name = $3 and scheduled_at = $4 order by created_at desc limit 1`
	err := j.pool.QueryRow(ctx, getJobRunByID, t.ProjectName(), t.NamespaceName(), jobName, scheduledAt).
		Scan(&jr.JobName, &jr.NamespaceName, &jr.ProjectName, &jr.ScheduledAt, &jr.StartTime, &jr.EndTime,
			&jr.Status, &jr.SLADefinition)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for job:"+jobName.String()+" scheduled at: "+scheduledAt.String())
		}
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting run", err)
	}
	return jr.toJobRun()
}

func (j *JobRunRepository) Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, status scheduler.State) error {
	updateJobRun := "update job_run set status = ?, end_time = ? , updated_at = NOW() where id = ?"
	_, err := j.pool.Exec(ctx, updateJobRun, status, endTime, jobRunID)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to update job run", err)
}

func (j *JobRunRepository) UpdateSLA(ctx context.Context, slaObjects []*scheduler.SLAObject) error {
	var jobIDList []string
	for _, slaObject := range slaObjects {
		jobIDs := fmt.Sprintf("('%s','%s')", slaObject.JobName, slaObject.JobScheduledAt.Format("2006-01-02 15:04:05"))
		jobIDList = append(jobIDList, jobIDs)
	}

	query := "update job_run set sla_alert = True, updated_at = NOW() where (job_name, scheduled_at) = any ($1)"
	_, err := j.pool.Exec(ctx, query, jobIDList)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to update SLA", err)
}

func (j *JobRunRepository) Create(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error {
	insertJobRun := `INSERT INTO job_run (job_name, namespace_name, project_name, scheduled_at, start_time, end_time, status, sla_definition, created_at, updated_at) values ($1, $2, $3, $4, NOW(), TIMESTAMP '3000-01-01 00:00:00', ?, ?, NOW(), NOW())`
	_, err := j.pool.Exec(ctx, insertJobRun, jobName, t.NamespaceName(), t.ProjectName(), scheduledAt, scheduler.StateRunning, slaDefinitionInSec)
	return errors.WrapIfErr(scheduler.EntityJobRun, "unable to create job run", err)
}

func NewJobRunRepository(pool *pgxpool.Pool) *JobRunRepository {
	return &JobRunRepository{
		pool: pool,
	}
}
