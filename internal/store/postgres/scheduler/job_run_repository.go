package scheduler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	jobRunColumns   = "job_name, namespace_name, project_name, scheduled_at, start_time, end_time, status, sla_definition"
	jobRunTableName = "job_run"
)

type JobRunRepository struct {
	db *gorm.DB
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
	var jobRun jobRun
	getJobRunByID := `SELECT ` + jobRunColumns + ` FROM ` + jobRunTableName + ` j where id = ?`
	err := j.db.WithContext(ctx).Raw(getJobRunByID, id).First(&jobRun).Error
	if err != nil {
		return &scheduler.JobRun{}, err
	}
	return jobRun.toJobRun()
}

func (j *JobRunRepository) GetByScheduledAt(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	var jobRun jobRun
	getJobRunByID := `SELECT ` + jobRunColumns + ` FROM job_run j 
						where project_id = ? and namespace_id =?
						job_name = ? and schedule_at = ?`
	err := j.db.WithContext(ctx).Raw(getJobRunByID, t.ProjectName(), t.NamespaceName(), jobName, scheduledAt).
		Order(clause.OrderByColumn{Column: clause.Column{Name: "created_at"}, Desc: true}).
		First(&jobRun).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for job:"+jobName.String()+" scheduled at: "+scheduledAt.String())
		}
		return nil, err
	}
	return jobRun.toJobRun()
}

func (j *JobRunRepository) Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, status string) error {
	updateJobRun := "update" + jobRunTableName + "set status = " + status + " end_time = " + endTime.String() + " where id = " + jobRunID.String()
	return j.db.WithContext(ctx).Exec(updateJobRun).Error
}

func (j *JobRunRepository) Create(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error {
	insertJobRun := `INSERT INTO job_run (` + jobRunColumns + `) values (?, ?, ?, ?, now(), TIMESTAMP '3000-01-01 00:00:00' , ?, ?) `
	return j.db.WithContext(ctx).Exec(insertJobRun,
		jobName.String(), t.NamespaceName().String(), t.ProjectName().String(),
		scheduledAt, scheduler.StateRunning, slaDefinitionInSec).Error
}

func NewJobRunRepository(db *gorm.DB) *JobRunRepository {
	return &JobRunRepository{
		db: db,
	}
}
