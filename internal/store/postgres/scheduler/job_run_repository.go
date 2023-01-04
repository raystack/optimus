package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
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
	SLAAlert      bool
	SLADefinition int64

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

func (j jobRun) toJobRun() (*scheduler.JobRun, error) {
	t, err := tenant.NewTenant(j.ProjectName, j.NamespaceName)
	if err != nil {
		return nil, err
	}
	state, err := scheduler.StateFromString(j.Status)
	if err != nil {
		return nil, err
	}
	return &scheduler.JobRun{
		ID:        j.ID,
		JobName:   scheduler.JobName(j.JobName),
		Tenant:    t,
		State:     state,
		StartTime: j.StartTime,
		SLAAlert:  j.SLAAlert,
		EndTime:   j.EndTime,
	}, nil
}

func (j *JobRunRepository) GetByID(ctx context.Context, id scheduler.JobRunID) (*scheduler.JobRun, error) {
	var jobRun jobRun
	getJobRunByID := `SELECT id, job_name, namespace_name, project_name, scheduled_at, start_time, end_time, status, sla_definition , sla_alert FROM job_run j where id = ?`
	err := j.db.WithContext(ctx).Raw(getJobRunByID, id.UUID()).First(&jobRun).Error
	if err != nil {
		return &scheduler.JobRun{}, err
	}
	return jobRun.toJobRun()
}

func (j *JobRunRepository) GetByScheduledAt(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (*scheduler.JobRun, error) {
	var jobRun jobRun
	getJobRunByID := `SELECT id, job_name, namespace_name, project_name, scheduled_at, start_time, end_time, status, sla_definition FROM job_run j where project_name = ? and namespace_name = ? and job_name = ? and scheduled_at = ? order by created_at desc limit 1`
	err := j.db.WithContext(ctx).Raw(getJobRunByID, t.ProjectName(), t.NamespaceName(), jobName, scheduledAt).First(&jobRun).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for job:"+jobName.String()+" scheduled at: "+scheduledAt.String())
		}
		return nil, err
	}
	return jobRun.toJobRun()
}

func (j *JobRunRepository) Update(ctx context.Context, jobRunID uuid.UUID, endTime time.Time, status scheduler.State) error {
	updateJobRun := "update job_run set status = ?, end_time = ? , updated_at = NOW() where id = ?"
	return j.db.WithContext(ctx).Exec(updateJobRun, status.String(), endTime, jobRunID).Error
}

func (j *JobRunRepository) UpdateSLA(ctx context.Context, slaObjects []*scheduler.SLAObject) error {
	jobIDListString := ""
	totalIds := len(slaObjects)
	for i, slaObject := range slaObjects {
		jobIDListString += fmt.Sprintf("('%s','%s')", slaObject.JobName, slaObject.JobScheduledAt.UTC().Format("2006-01-02 15:04:05.000000"))
		if !(i == totalIds-1) {
			jobIDListString += ", "
		}
	}
	query := "update job_run set sla_alert = True, updated_at = NOW() where (job_name, scheduled_at) in (" + jobIDListString + ")"
	return j.db.WithContext(ctx).Exec(query).Error
}

func (j *JobRunRepository) Create(ctx context.Context, t tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time, slaDefinitionInSec int64) error {
	insertJobRun := `INSERT INTO job_run (job_name, namespace_name, project_name, scheduled_at, start_time, end_time, status, sla_definition, created_at, updated_at) values (?, ?, ?, ?, NOW(), TIMESTAMP '3000-01-01 00:00:00', ?, ?, NOW(), NOW())`
	return j.db.WithContext(ctx).Exec(insertJobRun, jobName.String(), t.NamespaceName().String(), t.ProjectName().String(), scheduledAt, scheduler.StateRunning, slaDefinitionInSec).Error
}

func NewJobRunRepository(db *gorm.DB) *JobRunRepository {
	return &JobRunRepository{
		db: db,
	}
}
