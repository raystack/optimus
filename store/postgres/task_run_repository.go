package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
)

type TaskRun struct {
	TaskRunId uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	JobID uuid.UUID
	Job   Job `gorm:"foreignKey:JobID"`

	JobRunID      uuid.UUID
	JobRunMetrics JobRunMetrics `gorm:"foreignKey:JobRunID"`

	NamespaceID uuid.UUID
	Namespace   Namespace `gorm:"foreignKey:NamespaceID"`

	ProjectID uuid.UUID

	ScheduledAt time.Time `gorm:"not null"`

	StartTime time.Time `gorm:"not null"`
	EndTime   time.Time

	Status       string
	Attempt      int
	SlaMissDelay int
	Duration     int

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

type TaskRunRepository struct {
	db      *gorm.DB
	adapter *JobSpecAdapter
	logger  log.Logger
}

func (repo *TaskRunRepository) GetTaskRunIfExists(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec, jobRunSpec models.JobRunSpec) (models.TaskRunSpec, error) {
	eventPayload := event.Value
	startedAtTimeStamp := time.Unix(int64(eventPayload["job_start_timestamp"].GetNumberValue()), 0)

	taskRun := TaskRun{}

	if err := repo.db.WithContext(ctx).Where("job_run_id = ? and  job_id = ? and project_id = ? and namespace_id = ? and scheduled_at = ? and atempt = ? ", jobRunSpec.JobRunId, jobSpec.ID, uuid.UUID(namespaceSpec.ProjectSpec.ID), namespaceSpec.ID, startedAtTimeStamp).Find(&taskRun).Error; err != nil {
		return models.TaskRunSpec{}, errors.New("could not update existing job run, Error :: " + err.Error())
	}
	taskRunSpec := models.TaskRunSpec{
		taskRun.TaskRunId,
		taskRun.JobID,
		taskRun.JobRunID,
		taskRun.NamespaceID,
		taskRun.ProjectID,
		taskRun.ScheduledAt,
		taskRun.StartTime,
		taskRun.EndTime,
		taskRun.Status,
		taskRun.Attempt,
		taskRun.SlaMissDelay,
		taskRun.Duration,
	}

	return taskRunSpec, nil
}

func (repo *TaskRunRepository) Save(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value
	startedAtTimeStamp := time.Unix(int64(eventPayload["job_start_timestamp"].GetNumberValue()), 0)

	taskRun := TaskRun{
		TaskRunId: uuid.New(),

		JobID:       jobSpec.ID,
		JobRunID:    jobRunSpec.JobRunId,
		NamespaceID: namespaceSpec.ID,
		ProjectID:   uuid.UUID(namespaceSpec.ProjectSpec.ID),

		StartTime: startedAtTimeStamp,
		Status:    jobRunStatusRunning,
		Attempt:   int(eventPayload["attempt"].GetNumberValue()),
	}

	resourceMarsh, _ := json.Marshal(taskRun)
	repo.logger.Info(" TaskRun to create %v ", string(resourceMarsh))
	return repo.db.WithContext(ctx).Omit("Namespace", "Instances").Create(&taskRun).Error
}

func (repo *TaskRunRepository) Update(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value
	eventPayloadString, _ := json.Marshal(eventPayload)
	repo.logger.Info(string(eventPayloadString))

	scheduledAtTimeStamp, err := time.Parse(airflowDateFormat, eventPayload["scheduled_at"].GetStringValue())
	if err != nil {
		repo.logger.Info(err.Error())
	}
	taskRun := TaskRun{}

	if err := repo.db.WithContext(ctx).Where("job_run_id = ? and  job_id = ? and project_id = ? and namespace_id = ? and scheduled_at = ? ", jobRunSpec.JobRunId, jobSpec.ID, uuid.UUID(namespaceSpec.ProjectSpec.ID), namespaceSpec.ID, scheduledAtTimeStamp).Find(&taskRun).Error; err != nil {
		return errors.New("could not update existing job run, Error :: " + err.Error())
	}

	taskRun.Status = eventPayload["Status"].GetStringValue()
	taskRun.Duration = int(eventPayload["duration"].GetNumberValue())

	return repo.db.WithContext(ctx).Save(&taskRun).Error
}

func NewTaskRunRepository(db *gorm.DB, logger log.Logger) *TaskRunRepository {
	return &TaskRunRepository{
		db:     db,
		logger: logger,
	}
}
