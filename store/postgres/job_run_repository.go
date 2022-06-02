package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/salt/log"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
)

type JobRunMetrics struct {
	JobRunId uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	JobID uuid.UUID
	Job   Job `gorm:"foreignKey:JobID"`

	NamespaceID uuid.UUID
	Namespace   Namespace `gorm:"foreignKey:NamespaceID"`

	ProjectId uuid.UUID

	ScheduledAt time.Time `gorm:"not null"`

	StartTime time.Time `gorm:"not null"`
	EndTime   time.Time

	Status         string
	Attempt        int
	sla_miss_delay int
	duration       int

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

const (
	airflowDateFormat = "2006-01-02T15:04:05+00:00"
)

type JobRunMetricsRepository struct {
	db           *gorm.DB
	adapter      *JobSpecAdapter
	logger       log.Logger
	instanceRepo *InstanceRepository
}

// TableName overrides the table name used by User to `profiles`
func (JobRunMetrics) TableName() string {
	return "job_run"
}

func (repo *JobRunMetricsRepository) Insert(ctx context.Context, namespace models.NamespaceSpec, spec models.JobRun, jobDestination string) error {
	resource, err := repo.adapter.FromJobRun(spec, namespace, jobDestination)
	if err != nil {
		return err
	}
	return repo.db.WithContext(ctx).Omit("Namespace", "Instances").Create(&resource).Error
}

func (repo *JobRunMetricsRepository) Save(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {

	eventPayload := event.Value
	eventPayloadString, _ := json.Marshal(eventPayload)

	repo.logger.Info(string(eventPayloadString))
	scheduledAtTimeStamp, err := time.Parse(airflowDateFormat, eventPayload["scheduled_at"].GetStringValue())
	if err != nil {
		repo.logger.Info(err.Error())
	}
	resource := JobRunMetrics{
		JobID: jobSpec.ID,

		NamespaceID: namespaceSpec.ID,
		ProjectId:   uuid.UUID(namespaceSpec.ProjectSpec.ID),

		ScheduledAt: scheduledAtTimeStamp,
	}
	return repo.db.WithContext(ctx).Omit("Namespace", "Instances").Create(&resource).Error
}

func NewJobRunMetricsRepository(db *gorm.DB, logger log.Logger) *JobRunMetricsRepository {
	return &JobRunMetricsRepository{
		db:     db,
		logger: logger,
	}
}
