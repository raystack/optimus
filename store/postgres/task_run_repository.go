package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
)

type TaskRun struct {
	TaskRunId uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

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
	db      *gorm.DB
	adapter *JobSpecAdapter
	logger  log.Logger
}

func (repo *TaskRunRepository) GetTaskRunIfExists(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) (models.TaskRunSpec, error) {
	taskRun := TaskRun{}
	if err := repo.db.WithContext(ctx).Where("job_run_id = ?  and job_run_attempt = ?", jobRunSpec.JobRunId, jobRunSpec.Attempt).Find(&taskRun).Error; (err != nil || taskRun == TaskRun{}) {
		if err != nil {
			return models.TaskRunSpec{}, errors.New("could not get existing task run, Error :: " + err.Error())
		} else {
			return models.TaskRunSpec{}, errors.New("could not get existing task run ")
		}
	}
	taskRunSpec := models.TaskRunSpec{
		TaskRunId:     taskRun.TaskRunId,
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
		TaskRunId:     uuid.New(),
		JobRunID:      jobRunSpec.JobRunId,
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

	if err := repo.db.WithContext(ctx).Where("job_run_id = ?  and job_run_attempt = ?", jobRunSpec.JobRunId, jobRunSpec.Attempt).Find(&taskRun).Error; err != nil {
		return errors.New("could not update existing task run, Error :: " + err.Error())
	}
	resourceString1, _ := json.Marshal(taskRun)
	repo.logger.Info("taskRun obj to persist before changing")
	repo.logger.Info(string(resourceString1))
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

func NewTaskRunRepository(db *gorm.DB, logger log.Logger) *TaskRunRepository {
	return &TaskRunRepository{
		db:     db,
		logger: logger,
	}
}
