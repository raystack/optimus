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

type SensorRun struct {
	SensorRunID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

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

type SensorRunRepository struct {
	db *gorm.DB
}

func (repo *SensorRunRepository) Save(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value

	startedAtTimeStamp := time.Unix(int64(eventPayload["event_time"].GetNumberValue()), 0)

	resource := SensorRun{
		JobRunID:      jobRunSpec.JobRunID,
		StartTime:     startedAtTimeStamp,
		Status:        jobRunStatusRunning,
		Attempt:       int(eventPayload["attempt"].GetNumberValue()),
		JobRunAttempt: jobRunSpec.Attempt,
	}
	return repo.db.WithContext(ctx).Create(&resource).Error
}

func (repo *SensorRunRepository) Update(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value

	var sensorRun SensorRun
	err := repo.db.WithContext(ctx).Where("job_run_id = ?  and job_run_attempt = ?", jobRunSpec.JobRunID, jobRunSpec.Attempt).First(&sensorRun).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return store.ErrResourceNotFound
		}
		return err
	}
	if event.Type == models.SensorFailEvent ||
		event.Type == models.SensorSuccessEvent ||
		event.Type == models.SensorRetryEvent {
		sensorRun.EndTime = time.Unix(int64(eventPayload["event_time"].GetNumberValue()), 0)
		sensorRun.Duration = int64(sensorRun.EndTime.Sub(sensorRun.StartTime).Seconds())
		sensorRun.Status = strings.ToUpper(strings.Split(string(event.Type), "sensor_")[1])
	} else {
		sensorRun.Status = eventPayload["Status"].GetStringValue()
	}
	sensorRun.Attempt = int(eventPayload["attempt"].GetNumberValue())

	return repo.db.WithContext(ctx).Save(&sensorRun).Error
}

func (repo *SensorRunRepository) GetSensorRun(ctx context.Context, jobRunSpec models.JobRunSpec) (models.SensorRunSpec, error) {
	var sensorRun SensorRun
	if err := repo.db.WithContext(ctx).Where(" job_run_id = ? and job_run_attempt = ?", jobRunSpec.JobRunID, jobRunSpec.Attempt).First(&sensorRun).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.SensorRunSpec{}, store.ErrResourceNotFound
		}
		return models.SensorRunSpec{}, err
	}
	sensorRunSpec := models.SensorRunSpec{
		SensorRunID:   sensorRun.SensorRunID,
		JobRunID:      sensorRun.JobRunID,
		StartTime:     sensorRun.StartTime,
		EndTime:       sensorRun.EndTime,
		Status:        sensorRun.Status,
		Attempt:       sensorRun.Attempt,
		JobRunAttempt: sensorRun.JobRunAttempt,
		Duration:      sensorRun.Duration,
	}
	return sensorRunSpec, nil
}

func NewSensorRunRepository(db *gorm.DB) *SensorRunRepository {
	return &SensorRunRepository{
		db: db,
	}
}
