package scheduler

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/internal/errors"
)

const (
	replayColumnsToStore = `job_name, namespace_name, project_name, start_time, end_time, description, parallel, status, message`
	replayColumns        = `id, ` + replayColumnsToStore

	replayRunColumns = `replay_id, scheduled_at, status`
)

type ReplayRepository struct {
	db *pgxpool.Pool
}

type replayRequest struct {
	ID uuid.UUID

	JobName       string
	NamespaceName string
	ProjectName   string

	StartTime   time.Time
	EndTime     time.Time
	Description string
	Parallel    bool

	Status  string
	Message string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (r ReplayRepository) RegisterReplay(ctx context.Context, replay *scheduler.Replay) (uuid.UUID, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return uuid.Nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback(context.TODO())
		} else {
			tx.Commit(context.TODO())
		}
	}()

	if err := r.insertReplay(ctx, tx, replay); err != nil {
		return uuid.Nil, err
	}

	storedReplay, err := r.getReplay(ctx, tx, replay)
	if err != nil {
		return uuid.Nil, err
	}

	// TODO: consider to store message of each run
	if err := r.insertReplayRuns(ctx, tx, storedReplay.ID, replay.Runs); err != nil {
		return uuid.Nil, err
	}

	return storedReplay.ID, nil
}

func (ReplayRepository) insertReplay(ctx context.Context, tx pgx.Tx, replay *scheduler.Replay) error {
	insertReplay := `INSERT INTO replay_request (` + replayColumnsToStore + `, created_at, updated_at) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())`
	_, err := tx.Exec(ctx, insertReplay, replay.JobName.String(), replay.Tenant.NamespaceName(), replay.Tenant.ProjectName(),
		replay.Config.StartTime, replay.Config.EndTime, replay.Config.Description, replay.Config.Parallel, replay.State, replay.Message)
	if err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to store replay", err)
	}
	return nil
}

func (ReplayRepository) getReplay(ctx context.Context, tx pgx.Tx, replay *scheduler.Replay) (replayRequest, error) {
	var rr replayRequest
	getReplayRequest := `SELECT ` + replayColumns + ` FROM replay_request where project_name = $1 and job_name = $2 and start_time = $3 and end_time = $4 order by created_at desc limit 1`
	if err := tx.QueryRow(ctx, getReplayRequest, replay.Tenant.ProjectName(), replay.JobName.String(), replay.Config.StartTime, replay.Config.EndTime).
		Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel,
			&rr.Status, &rr.Message); err != nil {
		return rr, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay", err)
	}
	return rr, nil
}

func (ReplayRepository) insertReplayRuns(ctx context.Context, tx pgx.Tx, replayID uuid.UUID, runs []*scheduler.JobRunStatus) error {
	insertReplayRun := `INSERT INTO replay_run (` + replayRunColumns + `, created_at, updated_at) values ($1, $2, $3, NOW(), NOW())`
	for _, run := range runs {
		_, err := tx.Exec(ctx, insertReplayRun, replayID, run.ScheduledAt, run.State)
		if err != nil {
			return errors.Wrap(scheduler.EntityJobRun, "unable to store replay", err)
		}
	}
	return nil
}

func NewReplayRepository(db *pgxpool.Pool) *ReplayRepository {
	return &ReplayRepository{db: db}
}
