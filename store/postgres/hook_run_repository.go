package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type HookRun struct {
	HookRunID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

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
	db *gorm.DB
}

func (repo *HookRunRepository) Save(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value
	startedAtTimeStamp := time.Unix(int64(eventPayload["event_time"].GetNumberValue()), 0)
	resource := HookRun{
		JobRunID:      jobRunSpec.JobRunID,
		StartTime:     startedAtTimeStamp,
		Status:        jobRunStatusRunning,
		Attempt:       int(eventPayload["attempt"].GetNumberValue()),
		JobRunAttempt: jobRunSpec.Attempt,
	}
	return repo.db.WithContext(ctx).Create(&resource).Error
}

func (repo *HookRunRepository) GetHookRun(ctx context.Context, jobRunSpec models.JobRunSpec) (models.HookRunSpec, error) {
	var hookRun HookRun
	if err := repo.db.WithContext(ctx).Where("job_run_id = ? and job_run_attempt = ?", jobRunSpec.JobRunID, jobRunSpec.Attempt).First(&hookRun).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.HookRunSpec{}, store.ErrResourceNotFound
		}
		return models.HookRunSpec{}, err
	}
	hookRunSpec := models.HookRunSpec{
		HookRunID:     hookRun.HookRunID,
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

	var hookRun HookRun
	err := repo.db.WithContext(ctx).Where(" job_run_id = ? and job_run_attempt = ?", jobRunSpec.JobRunID, jobRunSpec.Attempt).First(&hookRun).Error
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return store.ErrResourceNotFound
		}
		return err
	}
	if event.Type == models.HookFailEvent ||
		event.Type == models.HookSuccessEvent ||
		event.Type == models.HookRetryEvent {
		hookRun.StartTime = time.Unix(int64(eventPayload["start_time"].GetNumberValue()), 0)
		hookRun.EndTime = time.Unix(int64(eventPayload["event_time"].GetNumberValue()), 0)
		hookRun.Duration = int64(hookRun.EndTime.Sub(hookRun.StartTime).Seconds())
		hookRun.Status = strings.ToUpper(strings.Split(string(event.Type), "hook_")[1])
	} else {
		hookRun.Status = eventPayload["Status"].GetStringValue()
	}
	hookRun.Attempt = int(eventPayload["attempt"].GetNumberValue())
	fmt.Println(hookRun)
	return repo.db.WithContext(ctx).Save(&hookRun).Error
}

func NewHookRunRepository(db *gorm.DB) *HookRunRepository {
	return &HookRunRepository{
		db: db,
	}
}
