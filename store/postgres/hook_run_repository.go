package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
)

type HookRun struct {
	HookRunId uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

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

type HookRunRepository struct {
	db      *gorm.DB
	adapter *JobSpecAdapter
	logger  log.Logger
}

func (repo *HookRunRepository) Save(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value
	startedAtTimeStamp := time.Unix(int64(eventPayload["task_start_timestamp"].GetNumberValue()), 0)
	resource := HookRun{
		JobRunID:      jobRunSpec.JobRunId,
		StartTime:     startedAtTimeStamp,
		Status:        jobRunStatusRunning,
		Attempt:       int(eventPayload["attempt"].GetNumberValue()),
		JobRunAttempt: jobRunSpec.Attempt,
	}
	return repo.db.WithContext(ctx).Omit("Namespace", "Instances").Create(&resource).Error
}

func (repo *HookRunRepository) GetHookRunIfExists(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) (models.HookRunSpec, error) {

	hookRun := HookRun{}
	if err := repo.db.WithContext(ctx).Where("job_run_id = ? and job_run_attempt = ?", jobRunSpec.JobRunId, jobRunSpec.Attempt).Find(&hookRun).Error; (err != nil || hookRun == HookRun{}) {
		if err != nil {
			return models.HookRunSpec{}, errors.New("could not update existing hook run, Error :: " + err.Error())
		} else {
			return models.HookRunSpec{}, errors.New("could not get existing hook run")
		}
	}
	hookRunSpec := models.HookRunSpec{
		HookRunId:     hookRun.HookRunId,
		JobRunID:      hookRun.JobRunID,
		StartTime:     hookRun.StartTime,
		EndTime:       hookRun.EndTime,
		Status:        hookRun.Status,
		Attempt:       hookRun.Attempt,
		Duration:      hookRun.Duration,
		JobRunAttempt: hookRun.JobRunAttempt,
	}
	return hookRunSpec, nil
}

func (repo *HookRunRepository) Update(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value

	hookRun := HookRun{}
	if err := repo.db.WithContext(ctx).Where(" job_run_id = ? and job_run_attempt = ?", jobRunSpec.JobRunId, jobRunSpec.Attempt).Find(&hookRun).Error; err != nil {
		return errors.New("could not update existing hook run, Error :: " + err.Error())
	}
	hookRun.Status = eventPayload["Status"].GetStringValue()
	hookRun.Attempt = int(eventPayload["attempt"].GetNumberValue())

	return repo.db.WithContext(ctx).Save(&hookRun).Error
}

func NewHookRunRepository(db *gorm.DB, logger log.Logger) *HookRunRepository {
	return &HookRunRepository{
		db:     db,
		logger: logger,
	}
}
