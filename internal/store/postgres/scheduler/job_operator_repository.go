package scheduler

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/internal/errors"
)

const (
	sensorRunTableName = "sensor_run"
	taskRunTableName   = "task_run"
	hookRunTableName   = "hook_run"
)

type OperatorRunRepository struct {
	// TODO: Add test
	pool *pgxpool.Pool
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
	DeletedAt sql.NullTime
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

func (o operatorRun) toOperatorRun() (*scheduler.OperatorRun, error) {
	status, err := scheduler.StateFromString(o.Status)
	if err != nil {
		return nil, errors.NewError(scheduler.EntityJobRun, "invalid job run state in database", err.Error())
	}
	return &scheduler.OperatorRun{
		ID:           o.ID,
		JobRunID:     o.JobRunID,
		Name:         o.Name,
		OperatorType: scheduler.OperatorType(o.OperatorType),
		Status:       status,
		StartTime:    o.StartTime,
		EndTime:      o.EndTime,
	}, nil
}

func (o *OperatorRunRepository) GetOperatorRun(ctx context.Context, name string, operatorType scheduler.OperatorType, jobRunID uuid.UUID) (*scheduler.OperatorRun, error) {
	var opRun operatorRun
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return nil, err
	}
	getJobRunByID := `SELECT id, name, job_run_id, status, start_time, end_time FROM ` + operatorTableName + ` j where job_run_id = $1 and name = $2 order by created_at desc limit 1`
	err = o.pool.QueryRow(ctx, getJobRunByID, jobRunID, name).
		Scan(&opRun.ID, &opRun.Name, &opRun.JobRunID, &opRun.Status, &opRun.StartTime, &opRun.EndTime)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(scheduler.EntityJobRun, "no record for "+operatorType.String()+"/"+name+" for job_run ID: "+jobRunID.String())
		}
		return nil, errors.Wrap(scheduler.EntityJobRun, "error while getting operator run", err)
	}
	return opRun.toOperatorRun()
}
func (o *OperatorRunRepository) CreateOperatorRun(ctx context.Context, name string, operatorType scheduler.OperatorType, jobRunID uuid.UUID, startTime time.Time) error {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return err
	}
	insertOperatorRun := `INSERT INTO ` + operatorTableName + ` ( job_run_id , name , status, start_time, end_time, created_at, updated_at) values ( $1, $2, $3, $4, TIMESTAMP '3000-01-01 00:00:00', NOW(), NOW())`
	_, err = o.pool.Exec(ctx, insertOperatorRun, jobRunID, name, scheduler.StateRunning, startTime)
	return errors.WrapIfErr(scheduler.EntityJobRun, "error while inserting the run", err)
}

func (o *OperatorRunRepository) UpdateOperatorRun(ctx context.Context, operatorType scheduler.OperatorType, operatorRunID uuid.UUID, eventTime time.Time, state scheduler.State) error {
	operatorTableName, err := operatorTypeToTableName(operatorType)
	if err != nil {
		return err
	}
	updateJobRun := "update " + operatorTableName + " set status = ?, end_time = ?, updated_at = NOW() where id = ?"
	_, err = o.pool.Exec(ctx, updateJobRun, state.String(), eventTime, operatorRunID)
	return errors.WrapIfErr(scheduler.EntityJobRun, "error while updating the run", err)
}

func NewOperatorRunRepository(pool *pgxpool.Pool) *OperatorRunRepository {
	return &OperatorRunRepository{
		pool: pool,
	}
}
