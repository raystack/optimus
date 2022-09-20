package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/models"
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
	jobRunMetrics.EndTime = time.Unix(int64(eventPayload["event_time"].GetNumberValue()), 0)
	jobRunMetrics.Duration = int64(jobRunMetrics.EndTime.Sub(jobRunMetrics.StartTime))

	return repo.db.WithContext(ctx).Save(&jobRunMetrics).Error
}

// GetLatestJobRunByScheduledTime get the latest jobRun instance for a given schedule time
func (repo *JobRunMetricsRepository) GetLatestJobRunByScheduledTime(ctx context.Context, scheduledAt string, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec) (models.JobRunSpec, error) {
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

	jobRunSpecData, err := getJobRunSpecData(jobRunMetrics.StartTime, jobRunMetrics.ScheduledAt, jobSpec)
	if err != nil {
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
		Data:          jobRunSpecData,
		SLADefinition: jobRunMetrics.SLADefinition,
	}
	return jobRunSpec, err
}

func (repo *JobRunMetricsRepository) GetByID(ctx context.Context, jobRunID uuid.UUID, jobSpec models.JobSpec) (models.JobRunSpec, error) {
	var jobRunMetrics JobRunMetrics
	err := repo.db.WithContext(ctx).Where("job_run_id = ? ", jobRunID).First(&jobRunMetrics).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.JobRunSpec{}, store.ErrResourceNotFound
		}
		return models.JobRunSpec{}, err
	}
	jobRunSpecData, err := getJobRunSpecData(jobRunMetrics.StartTime, jobRunMetrics.ScheduledAt, jobSpec)
	if err != nil {
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
		Data:          jobRunSpecData,
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

	jobRunSpecData, err := getJobRunSpecData(jobRunMetrics.StartTime, scheduledAtTimeStamp, jobSpec)
	if err != nil {
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
		Data:         jobRunSpecData,
		SLAMissDelay: jobRunMetrics.SLAMissDelay,
		Duration:     jobRunMetrics.Duration,
	}

	return jobRunSpec, err
}

func (repo *JobRunMetricsRepository) Save(ctx context.Context, event models.JobEvent, namespaceSpec models.NamespaceSpec, jobSpec models.JobSpec, slaMissDurationInSec int64) error {
	eventPayload := event.Value

	scheduledAtTimeStamp, err := time.Parse(store.ISODateFormat, eventPayload["scheduled_at"].GetStringValue())

	if err != nil {
		return err
	}
	// start time of "job_start_event" (scheduler task)
	executedAt := time.Unix(int64(eventPayload["event_time"].GetNumberValue()), 0)

	bigEndTime := time.Date(3000, 9, 16, 19, 17, 23, 0, time.UTC)
	resource := JobRunMetrics{
		JobID:       jobSpec.ID,
		NamespaceID: namespaceSpec.ID,
		ProjectID:   namespaceSpec.ProjectSpec.ID.UUID(),

		ScheduledAt:   scheduledAtTimeStamp,
		StartTime:     executedAt,
		EndTime:       bigEndTime,
		SLADefinition: slaMissDurationInSec,

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

func getJobRunSpecData(executedAt time.Time, scheduledAt time.Time, jobSpec models.JobSpec) ([]models.JobRunSpecData, error) {
	startTime, err := jobSpec.Task.Window.GetStartTime(scheduledAt)
	if err != nil {
		return nil, err
	}
	endTime, err := jobSpec.Task.Window.GetEndTime(scheduledAt)
	if err != nil {
		return nil, err
	}
	jobRunSpecData := []models.JobRunSpecData{
		{
			Name:  models.ConfigKeyExecutionTime,
			Value: executedAt.Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDstart,
			Value: startTime.Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDend,
			Value: endTime.Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDestination,
			Value: jobSpec.ResourceDestination,
			Type:  models.InstanceDataTypeEnv,
		},
	}
	return jobRunSpecData, nil
}
