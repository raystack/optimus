package job_run

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
)

const (
	jobRunColumns = "job_name, namespace_name, project_name, scheduled_at, start_time, end_time, status, sla_definition"
)

type JobRunRepository struct {
	db *gorm.DB
}

type JobRun struct {
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
	DeletedAt gorm.DeletedAt
}

func (j JobRun) toJobRun() (*job_run.JobRun, error) {
	t, err := tenant.NewTenant(j.ProjectName, j.NamespaceName)
	if err != nil {
		return nil, err
	}
	return &job_run.JobRun{
		ID:        j.ID,
		JobName:   job_run.JobName(j.JobName),
		Tenant:    t,
		StartTime: j.StartTime,
	}, nil
}

func (j *JobRunRepository) GetByID(ctx context.Context, id job_run.JobRunID) (*job_run.JobRun, error) {
	var jobRun JobRun
	getJobRunById := `SELECT ` + jobRunColumns + ` FROM job_run j where id = ?`
	err := j.db.WithContext(ctx).Raw(getJobRunById, id).First(&jobRun).Error
	if err != nil {
		return &job_run.JobRun{}, err
	}
	return jobRun.toJobRun()
}

func (j *JobRunRepository) GetByScheduledAt(ctx context.Context, t *tenant.Tenant, jobName job_run.JobName, scheduledAt time.Time) (*job_run.JobRun, error) {
	var jobRun JobRun
	getJobRunById := `SELECT ` + jobRunColumns + ` FROM job_run j 
						where project_id = ? and namespace_id =?
						job_name = ? and schedule_at=?`
	err := j.db.WithContext(ctx).Raw(getJobRunById, t.ProjectName(), t.NamespaceName(), jobName, scheduledAt).Order(clause.OrderByColumn{Column: clause.Column{Name: "created_at"}, Desc: true}).First(&jobRun).Error
	if err != nil {
		return &job_run.JobRun{}, err
	}
	return jobRun.toJobRun()
}

func (j *JobRunRepository) Update(ctx context.Context, t *tenant.Tenant, jobName job_run.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error {

	return nil
}

func (j *JobRunRepository) Create(ctx context.Context, t *tenant.Tenant, jobName job_run.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error {
	insertJobRun := `INSERT INTO job_run (` + jobRunColumns + `) values (?, ?, ?, ?, now(), TIMESTAMP '3000-01-01 00:00:00' , ?, ?) `
	return j.db.WithContext(ctx).Exec(insertJobRun,
		jobName.String(), t.NamespaceName().String(), t.ProjectName().String(),
		scheduledAt, job_run.StateRunning, slaDefinitionInSec).Error
}
