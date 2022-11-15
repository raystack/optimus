package job_run

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm/clause"

	"gorm.io/gorm"

	"github.com/odpf/optimus/core/job_run"
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

	OperatorType string
	State        string

	StartTime time.Time `gorm:"not null"`
	EndTime   time.Time `gorm:"default:TIMESTAMP '3000-01-01 00:00:00'"`

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
	DeletedAt gorm.DeletedAt
}

func operatorTypeToTableName(operatorType job_run.OperatorType) (string, error) {
	switch operatorType {
	case job_run.OperatorSensor:
		return sensorRunTableName, nil
	case job_run.OperatorHook:
		return hookRunTableName, nil
	case job_run.OperatorTask:
		return taskRunTableName, nil
	default:
		return "", errors.InvalidArgument(job_run.EntityJobRun, "invalid operator Type:"+operatorType.String())
	}
}

func (o operatorRun) toOperatorRun() *job_run.OperatorRun {
	return &job_run.OperatorRun{
		ID:           o.ID,
		JobRunID:     o.JobRunID,
		OperatorType: job_run.OperatorType(o.OperatorType),
		State:        o.State,
		StartTime:    o.StartTime,
		EndTime:      o.EndTime,
	}
}

func (o *OperatorRunRepository) GetOperatorRun(ctx context.Context, operatorType job_run.OperatorType, jobRunId uuid.UUID) (*job_run.OperatorRun, error) {
	var opRun operatorRun
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return nil, err
	}
	getJobRunById := `SELECT id, job_run_id, state, start_time, end_time FROM ` + operatorTableName + ` j where job_run_id = ?`
	err = o.db.WithContext(ctx).Raw(getJobRunById, jobRunId).
		Order(clause.OrderByColumn{Column: clause.Column{Name: "created_at"}, Desc: true}).
		First(&opRun).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.NotFound(job_run.EntityJobRun, "no record for Operator:"+operatorType.String()+" for job_run ID: "+jobRunId.String())
		}
	}
	return opRun.toOperatorRun(), nil
}
func (o *OperatorRunRepository) CreateOperatorRun(ctx context.Context, operatorType job_run.OperatorType, jobRunID uuid.UUID, startTime time.Time) error {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return err
	}
	insertOperatorRun := `INSERT INTO ? ( job_run_id , state, start_time, end_time ) 
	values (?, ?, ?, TIMESTAMP '3000-01-01 00:00:00' )`
	return o.db.WithContext(ctx).Exec(insertOperatorRun, operatorTableName,
		jobRunID, job_run.StateRunning, startTime).Error

}

func (o *OperatorRunRepository) UpdateOperatorRun(ctx context.Context, operatorType job_run.OperatorType, operatorRunID uuid.UUID, eventTime time.Time, state string) error {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return err
	}
	updateJobRun := "update ? set state = ? and end_time = ? where id = ?"
	return o.db.WithContext(ctx).Exec(updateJobRun, operatorTableName, state, eventTime, operatorRunID).Error
}
