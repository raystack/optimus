package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
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

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

const (
	jobRunStatusRunning = "STARTED"
)

type JobRunMetricsRepository struct {
	db                  *gorm.DB
	sensorRunRepository SensorRunRepository
	taskRunRepository   TaskRunRepository
	hookRunRepository   HookRunRepository
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

func (repo *JobRunMetricsRepository) DeleteAllByJobID(ctx context.Context, jobID uuid.UUID) error {
	var jobRun JobRunMetrics
	jobRunIDs, err := repo.GetAllJobRunIDByJobID(ctx, jobID)
	if err != nil {
		return err
	}

	err = repo.sensorRunRepository.DeleteByJobRunID(ctx, jobRunIDs)
	if err != nil {
		return err
	}
	err = repo.taskRunRepository.DeleteByJobRunID(ctx, jobRunIDs)
	if err != nil {
		return err
	}
	err = repo.hookRunRepository.DeleteByJobRunID(ctx, jobRunIDs)
	if err != nil {
		return err
	}

	return repo.db.WithContext(ctx).Unscoped().Where("job_id = ?", jobID).Delete(&jobRun).Error
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
		Data:          getJobRunSpecData(jobRunMetrics.StartTime, jobRunMetrics.ScheduledAt, jobSpec),
		SLADefinition: jobRunMetrics.SLADefinition,
	}
	return jobRunSpec, err
}

func (repo *JobRunMetricsRepository) GetAllJobRunIDByJobID(ctx context.Context, jobID uuid.UUID) ([]uuid.UUID, error) {
	var jobRunMetricsList []JobRunMetrics
	err := repo.db.WithContext(ctx).Select("job_run_id").Where("job_id = ? ", jobID).Find(&jobRunMetricsList).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return []uuid.UUID{}, store.ErrResourceNotFound
		}
		return []uuid.UUID{}, err
	}
	jobRunMetricsRunIDList := make([]uuid.UUID, len(jobRunMetricsList))
	for i, jobRun := range jobRunMetricsList {
		jobRunMetricsRunIDList[i] = jobRun.JobRunID
	}
	return jobRunMetricsRunIDList, err
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
		Data:          getJobRunSpecData(jobRunMetrics.StartTime, jobRunMetrics.ScheduledAt, jobSpec),
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
		Data:         getJobRunSpecData(jobRunMetrics.StartTime, scheduledAtTimeStamp, jobSpec),
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

func NewJobRunMetricsRepository(db *gorm.DB, sensorRunRepository SensorRunRepository, taskRunRepository TaskRunRepository, hookRunRepository HookRunRepository) *JobRunMetricsRepository {
	return &JobRunMetricsRepository{
		db:                  db,
		sensorRunRepository: sensorRunRepository,
		taskRunRepository:   taskRunRepository,
		hookRunRepository:   hookRunRepository,
	}
}

func getJobRunSpecData(executedAt time.Time, scheduledAt time.Time, jobSpec models.JobSpec) []models.JobRunSpecData {
	return []models.JobRunSpecData{
		{
			Name:  models.ConfigKeyExecutionTime,
			Value: executedAt.Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDstart,
			Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDend,
			Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
			Type:  models.InstanceDataTypeEnv,
		},
		{
			Name:  models.ConfigKeyDestination,
			Value: jobSpec.ResourceDestination,
			Type:  models.InstanceDataTypeEnv,
		},
	}
}
