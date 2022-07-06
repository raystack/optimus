package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type JobRunMetrics struct {
	JobRunID uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`

	JobID uuid.UUID

	NamespaceID uuid.UUID

	ProjectID uuid.UUID

	ScheduledAt time.Time `gorm:"not null"`

	StartTime time.Time `gorm:"not null"`
	EndTime   time.Time

	Status        string
	Attempt       int
	SLAMissDelay  int
	Duration      int64
	SLADefinition int64

	Data datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

const (
	jobRunStatusRunning = "STARTED"
)

type JobRunMetricsRepository struct {
	db *gorm.DB
}

// TableName overrides the table name used by User to `profiles`
func (JobRunMetrics) TableName() string {
	return "job_run"
}

func jsonToInstanceSpec(j datatypes.JSON) []models.InstanceSpecData {
	var data []models.InstanceSpecData
	if j != nil {
		if err := json.Unmarshal(j, &data); err != nil {
			return []models.InstanceSpecData{}
		}
	}
	return data
}

func (repo *JobRunMetricsRepository) Update(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) error {
	eventPayload := event.Value

	jobRunMetrics := JobRunMetrics{}

	scheduledAtTimeStamp, err := time.Parse(store.ISODateFormat, eventPayload["scheduled_at"].GetStringValue())
	attemptNumber := int(eventPayload["attempt"].GetNumberValue())

	if err != nil {
		return err
	}
	err = repo.db.WithContext(ctx).Where("job_id = ? and project_id = ? and namespace_id = ? and scheduled_at = ? and attempt = ? ", jobSpec.ID, namespaceSpec.ProjectSpec.ID.UUID(), namespaceSpec.ID, scheduledAtTimeStamp, attemptNumber).First(&jobRunMetrics).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return store.ErrResourceNotFound
		}
		return err
	}
	jobRunMetrics.Status = eventPayload["status"].GetStringValue()
	jobRunMetrics.Duration = int64(eventPayload["job_duration"].GetNumberValue())
	jobRunMetrics.EndTime = time.Unix(int64(eventPayload["event_time"].GetNumberValue()), 0)

	return repo.db.WithContext(ctx).Save(&jobRunMetrics).Error
}

// GetActiveJobRun get the latest jobRun instance for a given schedule time
func (repo *JobRunMetricsRepository) GetActiveJobRun(ctx context.Context, scheduledAt string, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	scheduledAtTimeStamp, err := time.Parse(store.ISODateFormat, scheduledAt)
	if err != nil {
		return models.JobRunSpec{}, err
	}

	var jobRunMetrics JobRunMetrics
	err = repo.db.WithContext(ctx).Where("job_id = ? and project_id = ? and namespace_id = ? and scheduled_at = ? ", jobSpec.ID, namespaceSpec.ProjectSpec.ID.UUID(), namespaceSpec.ID, scheduledAtTimeStamp).Order("attempt desc").First(&jobRunMetrics).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobRunSpec{}, store.ErrResourceNotFound
		}
		return models.JobRunSpec{}, err
	}

	jobRunSpec := models.JobRunSpec{
		JobRunID:      jobRunMetrics.JobRunID,
		JobID:         jobRunMetrics.JobID,
		NamespaceID:   jobRunMetrics.NamespaceID,
		ProjectID:     jobRunMetrics.ProjectID,
		ScheduledAt:   jobRunMetrics.ScheduledAt,
		StartTime:     jobRunMetrics.StartTime,
		EndTime:       jobRunMetrics.EndTime,
		Status:        jobRunMetrics.Status,
		Attempt:       jobRunMetrics.Attempt,
		SLAMissDelay:  jobRunMetrics.SLAMissDelay,
		Duration:      jobRunMetrics.Duration,
		Data:          jsonToInstanceSpec(jobRunMetrics.Data),
		SLADefinition: jobRunMetrics.SLADefinition,
	}
	return jobRunSpec, err
}

func (repo *JobRunMetricsRepository) GetByID(ctx context.Context, jobRunID uuid.UUID) (models.JobRunSpec, error) {
	var jobRunMetrics JobRunMetrics
	err := repo.db.WithContext(ctx).Where("job_run_id = ? ", jobRunID).First(&jobRunMetrics).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobRunSpec{}, store.ErrResourceNotFound
		}
		return models.JobRunSpec{}, err
	}
	jobRunSpec := models.JobRunSpec{
		JobRunID:      jobRunMetrics.JobRunID,
		JobID:         jobRunMetrics.JobID,
		NamespaceID:   jobRunMetrics.NamespaceID,
		ProjectID:     jobRunMetrics.ProjectID,
		ScheduledAt:   jobRunMetrics.ScheduledAt,
		StartTime:     jobRunMetrics.StartTime,
		EndTime:       jobRunMetrics.EndTime,
		Status:        jobRunMetrics.Status,
		Attempt:       jobRunMetrics.Attempt,
		SLAMissDelay:  jobRunMetrics.SLAMissDelay,
		Data:          jsonToInstanceSpec(jobRunMetrics.Data),
		Duration:      jobRunMetrics.Duration,
		SLADefinition: jobRunMetrics.SLADefinition,
	}
	return jobRunSpec, err
}
func (repo *JobRunMetricsRepository) Get(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	eventPayload := event.Value
	jobRunMetrics := JobRunMetrics{}

	scheduledAtTimeStamp, err := time.Parse(store.ISODateFormat, eventPayload["scheduled_at"].GetStringValue())
	if err != nil {
		return models.JobRunSpec{}, err
	}

	attemptNumber := int(eventPayload["attempt"].GetNumberValue())

	err = repo.db.WithContext(ctx).Where("job_id = ? and project_id = ? and namespace_id = ? and scheduled_at = ? and attempt = ? ", jobSpec.ID, namespaceSpec.ProjectSpec.ID.UUID(), namespaceSpec.ID, scheduledAtTimeStamp, attemptNumber).First(&jobRunMetrics).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobRunSpec{}, store.ErrResourceNotFound
		}
		return models.JobRunSpec{}, err
	}
	jobRunSpec := models.JobRunSpec{
		JobRunID:     jobRunMetrics.JobRunID,
		JobID:        jobRunMetrics.JobID,
		NamespaceID:  jobRunMetrics.NamespaceID,
		ProjectID:    jobRunMetrics.ProjectID,
		ScheduledAt:  jobRunMetrics.ScheduledAt,
		StartTime:    jobRunMetrics.StartTime,
		EndTime:      jobRunMetrics.EndTime,
		Status:       jobRunMetrics.Status,
		Attempt:      jobRunMetrics.Attempt,
		Data:         jsonToInstanceSpec(jobRunMetrics.Data),
		SLAMissDelay: jobRunMetrics.SLAMissDelay,
		Duration:     jobRunMetrics.Duration,
	}

	return jobRunSpec, err
}

func (repo *JobRunMetricsRepository) Save(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec, slaMissDurationInSec int64, jobDestination string) error {
	eventPayload := event.Value

	scheduledAtTimeStamp, err := time.Parse(store.ISODateFormat, eventPayload["scheduled_at"].GetStringValue())
	if err != nil {
		return err
	}
	executedAt := time.Unix(int64(eventPayload["task_start_timestamp"].GetNumberValue()), 0)

	instanceSpec := models.InstanceSpec{
		Data: []models.InstanceSpecData{
			{
				Name:  models.ConfigKeyExecutionTime,
				Value: executedAt.Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  models.ConfigKeyDstart,
				Value: jobSpec.Task.Window.GetStart(scheduledAtTimeStamp).Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  models.ConfigKeyDend,
				Value: jobSpec.Task.Window.GetEnd(scheduledAtTimeStamp).Format(models.InstanceScheduledAtTimeLayout),
				Type:  models.InstanceDataTypeEnv,
			},
			{
				Name:  models.ConfigKeyDestination,
				Value: jobDestination,
				Type:  models.InstanceDataTypeEnv,
			},
		},
	}
	dataJSON, err := instanceSpec.DataToJSON()
	if err != nil {
		return err
	}

	bigEndTime := time.Date(3000, 9, 16, 19, 17, 23, 0, time.UTC)

	resource := JobRunMetrics{
		JobID:       jobSpec.ID,
		NamespaceID: namespaceSpec.ID,
		ProjectID:   namespaceSpec.ProjectSpec.ID.UUID(),

		ScheduledAt:   scheduledAtTimeStamp,
		StartTime:     executedAt,
		EndTime:       bigEndTime,
		SLADefinition: slaMissDurationInSec,
		Data:          dataJSON,

		Status:  jobRunStatusRunning,
		Attempt: int(eventPayload["attempt"].GetNumberValue()),
	}
	return repo.db.WithContext(ctx).Create(&resource).Error
}

func NewJobRunMetricsRepository(db *gorm.DB) *JobRunMetricsRepository {
	return &JobRunMetricsRepository{
		db: db,
	}
}
