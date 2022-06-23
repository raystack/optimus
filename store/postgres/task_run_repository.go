package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type TaskRun struct {
	TaskRunID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	JobRunID uuid.UUID

	StartTime time.Time `gorm:"not null"`
	EndTime   time.Time

	Status        string
	Attempt       int
	JobRunAttempt int
	Duration      int64

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

type TaskRunRepository struct {
	db *gorm.DB
}

func (repo *TaskRunRepository) GetTaskRunIfExists(ctx context.Context, jobRunSpec models.JobRunSpec) (models.TaskRunSpec, error) {
	taskRun := TaskRun{}
	if err := repo.db.WithContext(ctx).Where("job_run_id = ?  and job_run_attempt = ?", jobRunSpec.JobRunID, jobRunSpec.Attempt).First(&taskRun).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.TaskRunSpec{}, store.ErrResourceNotFound
		}
		return models.TaskRunSpec{}, err
	}
	taskRunSpec := models.TaskRunSpec{
		TaskRunID:     taskRun.TaskRunID,
		JobRunID:      taskRun.JobRunID,
		StartTime:     taskRun.StartTime,
		EndTime:       taskRun.EndTime,
		Status:        taskRun.Status,
		Attempt:       taskRun.Attempt,
		Duration:      taskRun.Duration,
		JobRunAttempt: taskRun.JobRunAttempt,
	}
	return taskRunSpec, nil
}

func (repo *TaskRunRepository) Save(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value
	startedAtTimeStamp := time.Unix(int64(eventPayload["task_start_timestamp"].GetNumberValue()), 0)

	taskRun := TaskRun{
		TaskRunID:     uuid.New(),
		JobRunID:      jobRunSpec.JobRunID,
		StartTime:     startedAtTimeStamp,
		Status:        jobRunStatusRunning,
		Attempt:       int(eventPayload["attempt"].GetNumberValue()),
		JobRunAttempt: jobRunSpec.Attempt,
	}

	return repo.db.WithContext(ctx).Omit("Namespace", "Instances").Create(&taskRun).Error
}

func (repo *TaskRunRepository) Update(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value
	taskRun := TaskRun{}

	if err := repo.db.WithContext(ctx).Where("job_run_id = ?  and job_run_attempt = ?", jobRunSpec.JobRunID, jobRunSpec.Attempt).First(&taskRun).Error; err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return store.ErrResourceNotFound
		}
		return err
	}
	if event.Type == models.TaskFailEvent ||
		event.Type == models.TaskSuccessEvent ||
		event.Type == models.TaskRetryEvent {
		taskRun.EndTime = time.Unix(int64(eventPayload["event_time"].GetNumberValue()), 0)
		taskRun.Duration = int64(taskRun.EndTime.Sub(taskRun.StartTime).Seconds())
		taskRun.Status = strings.ToUpper(strings.Split(string(event.Type), "task_")[1])
	} else {
		taskRun.Status = eventPayload["status"].GetStringValue()
	}
	taskRun.Attempt = int(eventPayload["attempt"].GetNumberValue())

	return repo.db.WithContext(ctx).Save(&taskRun).Error
}

func NewTaskRunRepository(db *gorm.DB) *TaskRunRepository {
	return &TaskRunRepository{
		db: db,
	}
}
