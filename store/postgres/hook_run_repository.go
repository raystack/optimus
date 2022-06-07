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

type HookRun struct {
	HookRunId uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

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

type HookRunRepository struct {
	db      *gorm.DB
	adapter *JobSpecAdapter
	logger  log.Logger
}

func (repo *HookRunRepository) Save(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	eventPayload := event.Value
	eventPayloadString, _ := json.Marshal(eventPayload)
	repo.logger.Info(string(eventPayloadString))

	scheduledAtTimeStamp, err := time.Parse(airflowDateFormat, eventPayload["scheduled_at"].GetStringValue())
	startedAtTimeStamp := time.Unix(int64(eventPayload["job_start_timestamp"].GetNumberValue()), 0)
	if err != nil {
		repo.logger.Info(err.Error())
	}
	resource := HookRun{
		JobID:       jobSpec.ID,
		NamespaceID: namespaceSpec.ID,
		ProjectID:   uuid.UUID(namespaceSpec.ProjectSpec.ID),

		ScheduledAt: scheduledAtTimeStamp,

		StartTime: startedAtTimeStamp,

		Status:  jobRunStatusRunning,
		Attempt: int(eventPayload["attempt"].GetNumberValue()),
	}
	return repo.db.WithContext(ctx).Omit("Namespace", "Instances").Create(&resource).Error
}

func (repo *HookRunRepository) Update(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	eventPayload := event.Value
	eventPayloadString, _ := json.Marshal(eventPayload)
	repo.logger.Info(string(eventPayloadString))

	scheduledAtTimeStamp, err := time.Parse(airflowDateFormat, eventPayload["scheduled_at"].GetStringValue())
	attemptNumber := int(eventPayload["attempt"].GetNumberValue())
	if err != nil {
		repo.logger.Info(err.Error())
	}
	hookRun := HookRun{}

	if err := repo.db.WithContext(ctx).Where("job_id = ? and project_id = ? and namespace_id = ? and scheduled_at = ? and atempt = ? ", jobSpec.ID, uuid.UUID(namespaceSpec.ProjectSpec.ID), namespaceSpec.ID, scheduledAtTimeStamp, attemptNumber).Find(&hookRun).Error; err != nil {
		return errors.New("could not update existing job run, Error :: " + err.Error())
	}

	hookRun.Status = eventPayload["Status"].GetStringValue()

	return repo.db.WithContext(ctx).Save(&hookRun).Error
}

func NewHookRunRepository(db *gorm.DB, logger log.Logger) *HookRunRepository {
	return &HookRunRepository{
		db:     db,
		logger: logger,
	}
}
