package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
)

type SensorRun struct {
	SensorRunId uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

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
	db      *gorm.DB
	adapter *JobSpecAdapter
	logger  log.Logger
}

func (repo *SensorRunRepository) Save(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value

	startedAtTimeStamp := time.Unix(int64(eventPayload["task_start_timestamp"].GetNumberValue()), 0)

	resource := SensorRun{
		JobRunID:      jobRunSpec.JobRunId,
		StartTime:     startedAtTimeStamp,
		Status:        jobRunStatusRunning,
		Attempt:       int(eventPayload["attempt"].GetNumberValue()),
		JobRunAttempt: jobRunSpec.Attempt,
	}

	return repo.db.WithContext(ctx).Create(&resource).Error
}

func (repo *SensorRunRepository) Update(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) error {
	eventPayload := event.Value

	sensorRun := SensorRun{}
	if err := repo.db.WithContext(ctx).Where("job_run_id = ?  and job_run_attempt = ?", jobRunSpec.JobRunId, jobRunSpec.Attempt).Find(&sensorRun).Error; err != nil {
		return errors.New("could not update existing sensor run, Error :: " + err.Error())
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

func (repo *SensorRunRepository) GetSensorRunIfExists(ctx context.Context, event models.JobEvent, jobRunSpec models.JobRunSpec) (models.SensorRunSpec, error) {

	sensorRun := SensorRun{}
	if err := repo.db.WithContext(ctx).Where(" job_run_id = ? and job_run_attempt = ?", jobRunSpec.JobRunId, jobRunSpec.Attempt).Find(&sensorRun).Error; err != nil || sensorRun == (SensorRun{}) {
		if err != nil {
			return models.SensorRunSpec{}, errors.New("sensor run not found :: " + err.Error())
		} else {
			return models.SensorRunSpec{}, errors.New("sensor run not found")
		}
	}
	sensorRunSpec := models.SensorRunSpec{
		SensorRunId:   sensorRun.SensorRunId,
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

func NewSensorRunRepository(db *gorm.DB, logger log.Logger) *SensorRunRepository {
	return &SensorRunRepository{
		db:     db,
		logger: logger,
	}
}
