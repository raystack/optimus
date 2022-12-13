package scheduler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/internal/errors"
)

const (
	sensorRunTableName = "sensor_run"
	taskRunTableName   = "task_run"
	hookRunTableName   = "hook_run"
)

type OperatorRunRepository struct {
	db *gorm.DB
}

type operatorRun struct {
	ID       uuid.UUID `gorm:"primary_key;type:uuid;default:uuid_generate_v4()"`
	JobRunID uuid.UUID

	Name         string
	OperatorType string
	Status       string

	StartTime time.Time `gorm:"not null"`
	EndTime   time.Time `gorm:"default:TIMESTAMP '3000-01-01 00:00:00'"`

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	// TODO:  add a remarks colum to capture failure reason
	DeletedAt gorm.DeletedAt
}

func operatorTypeToTableName(operatorType scheduler.OperatorType) (string, error) {
	switch operatorType {
	case scheduler.OperatorSensor:
		return sensorRunTableName, nil
	case scheduler.OperatorHook:
		return hookRunTableName, nil
	case scheduler.OperatorTask:
		return taskRunTableName, nil
	default:
		return "", errors.InvalidArgument(scheduler.EntityJobRun, "invalid operator Type:"+operatorType.String())
	}
}

func (o operatorRun) toOperatorRun() *scheduler.OperatorRun {
	return &scheduler.OperatorRun{
		ID:           o.ID,
		JobRunID:     o.JobRunID,
		Name:         o.Name,
		OperatorType: scheduler.OperatorType(o.OperatorType),
		Status:       o.Status,
		StartTime:    o.StartTime,
		EndTime:      o.EndTime,
	}
}

func (o *OperatorRunRepository) GetOperatorRun(ctx context.Context, name string, operatorType scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error) {
	var opRun operatorRun
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return nil, err
	}
	getJobRunByID := `SELECT id, name, job_run_id, status, start_time, end_time FROM ` + operatorTableName + ` j where job_run_id = ? and name =?`
	err = o.db.WithContext(ctx).Raw(getJobRunByID, jobRunID, name).
		Order(clause.OrderByColumn{Column: clause.Column{Name: "created_at"}, Desc: true}).
		First(&opRun).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for "+operatorType.String()+"/"+name+" for job_run ID: "+jobRunID.String())
		}
	}
	return opRun.toOperatorRun(), nil
}
func (o *OperatorRunRepository) CreateOperatorRun(ctx context.Context, name string, operatorType scheduler.OperatorType, jobRunID uuid.UUID, startTime time.Time) error {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return err
	}
	insertOperatorRun := `INSERT INTO ` + operatorTableName + ` ( job_run_id , name , status, start_time, end_time, created_at, updated_at) values ( ?, ?, ?, ?, TIMESTAMP '3000-01-01 00:00:00', NOW(), NOW())`
	return o.db.WithContext(ctx).Exec(insertOperatorRun, jobRunID, name, scheduler.StateRunning, startTime).Error
}

func (o *OperatorRunRepository) UpdateOperatorRun(ctx context.Context, operatorType scheduler.OperatorType, operatorRunID uuid.UUID, eventTime time.Time, state string) error {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return err
	}
	updateJobRun := "update ? set status = ?, end_time = ?, updated_at = NOW() where id = ?"
	return o.db.WithContext(ctx).Exec(updateJobRun, operatorTableName, state, eventTime, operatorRunID).Error
}

func NewOperatorRunRepository(db *gorm.DB) *OperatorRunRepository {
	return &OperatorRunRepository{
		db: db,
	}
}
