package scheduler

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/net/context"

	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/errors"
)

const (
	replayColumnsToStore = `job_name, namespace_name, project_name, start_time, end_time, description, parallel, status, message`
	replayColumns        = `id, ` + replayColumnsToStore

	replayRunColumns       = `replay_id, scheduled_at, status`
	replayRunDetailColumns = `id as replay_id, job_name, namespace_name, project_name, start_time, end_time, description, 
parallel, r.status as replay_status, r.message as replay_message, scheduled_at, run.status as run_status, r.created_at as replay_created_at`
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

func (r replayRequest) toSchedulerReplayRequest() (*scheduler.ReplayRequest, error) {
	tnnt, err := tenant.NewTenant(r.ProjectName, r.NamespaceName)
	if err != nil {
		return nil, err
	}
	conf := scheduler.NewReplayConfig(r.StartTime, r.EndTime, r.Parallel, r.Description)
	replayStatus, err := scheduler.ReplayStateFromString(r.Status)
	if err != nil {
		return nil, err
	}
	jobName, err := scheduler.JobNameFrom(r.JobName)
	if err != nil {
		return nil, err
	}
	return scheduler.NewReplayRequest(jobName, tnnt, conf, replayStatus), nil
}

type replayRun struct {
	ID uuid.UUID

	JobName       string
	NamespaceName string
	ProjectName   string

	StartTime   time.Time
	EndTime     time.Time
	Description string
	Parallel    bool

	ReplayStatus string
	Message      string

	ScheduledTime time.Time
	RunStatus     string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (r replayRun) toReplayRequest() (*scheduler.ReplayRequest, error) {
	tnnt, err := tenant.NewTenant(r.ProjectName, r.NamespaceName)
	if err != nil {
		return nil, err
	}
	conf := scheduler.NewReplayConfig(r.StartTime, r.EndTime, r.Parallel, r.Description)
	replayStatus, err := scheduler.ReplayStateFromString(r.ReplayStatus)
	if err != nil {
		return nil, err
	}
	jobName, err := scheduler.JobNameFrom(r.JobName)
	if err != nil {
		return nil, err
	}
	return scheduler.NewReplayRequest(jobName, tnnt, conf, replayStatus), nil
}

func (r replayRun) toJobRunStatus() (*scheduler.JobRunStatus, error) {
	runState, err := scheduler.StateFromString(r.RunStatus)
	if err != nil {
		return nil, err
	}
	return &scheduler.JobRunStatus{
		ScheduledAt: r.ScheduledTime,
		State:       runState,
	}, nil
}

func (r ReplayRepository) RegisterReplay(ctx context.Context, replay *scheduler.ReplayRequest, runs []*scheduler.JobRunStatus) (uuid.UUID, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return uuid.Nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
	}()

	if err := r.insertReplay(ctx, tx, replay); err != nil {
		return uuid.Nil, err
	}

	storedReplay, err := r.getReplayRequest(ctx, tx, replay)
	if err != nil {
		return uuid.Nil, err
	}

	// TODO: consider to store message of each run
	if err := r.insertReplayRuns(ctx, tx, storedReplay.ID, runs); err != nil {
		return uuid.Nil, err
	}

	return storedReplay.ID, nil
}

func (r ReplayRepository) GetReplayToExecute(ctx context.Context) (*scheduler.Replay, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
	}()

	replayRuns, err := r.getExecutableReplayRuns(ctx, tx)
	if err != nil {
		return nil, err
	}
	storedReplay, err := toReplay(replayRuns)
	if err != nil {
		return nil, err
	}
	return storedReplay, r.updateReplayRequest(ctx, tx, storedReplay.ID, scheduler.ReplayStateInProgress, "")
}

func (r ReplayRepository) GetReplayRequestByStatus(ctx context.Context, statusList []scheduler.ReplayState) ([]*scheduler.ReplayRequest, error) {
	getReplayRequest := `SELECT ` + replayColumns + ` FROM replay_request WHERE status = ANY($1)`
	rows, err := r.db.Query(ctx, getReplayRequest, statusList)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "unable to get replay list", err)
	}
	defer rows.Close()

	var replayReqs []*scheduler.ReplayRequest
	for rows.Next() {
		var rr replayRequest
		if err := rows.Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel,
			&rr.Status, &rr.Message); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NotFound(job.EntityJob, fmt.Sprintf("no replay found for status %s", statusList))
			}
			return nil, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay", err)
		}
		schedulerReplayReq, err := rr.toSchedulerReplayRequest()
		if err != nil {
			return nil, err
		}
		replayReqs = append(replayReqs, schedulerReplayReq)
	}
	return replayReqs, nil
}

func toReplay(replayRuns []replayRun) (*scheduler.Replay, error) {
	var storedReplay *scheduler.Replay
	for _, run := range replayRuns {
		if storedReplay != nil {
			runState, err := scheduler.StateFromString(run.RunStatus)
			if err != nil {
				return nil, err
			}
			jobRun := &scheduler.JobRunStatus{
				ScheduledAt: run.ScheduledTime,
				State:       runState,
			}
			storedReplay.Runs = append(storedReplay.Runs, jobRun)
			continue
		}

		replay, err := run.toReplayRequest()
		if err != nil {
			return nil, err
		}

		jobRun, err := run.toJobRunStatus()
		if err != nil {
			return nil, err
		}

		storedReplay = &scheduler.Replay{
			ID:     run.ID,
			Replay: replay,
			Runs:   []*scheduler.JobRunStatus{jobRun},
		}
	}
	return storedReplay, nil
}

func (r ReplayRepository) UpdateReplay(ctx context.Context, id uuid.UUID, replayStatus scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
	}()

	if err := r.updateReplayRequest(ctx, tx, id, replayStatus, message); err != nil {
		return err
	}

	deleteRuns := `DELETE FROM replay_run WHERE replay_id = $1`
	if _, err := tx.Exec(ctx, deleteRuns, id); err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to delete runs of replay", err)
	}
	return r.insertReplayRuns(ctx, tx, id, runs)
}

func (ReplayRepository) updateReplayRequest(ctx context.Context, tx pgx.Tx, id uuid.UUID, replayStatus scheduler.ReplayState, message string) error {
	updateReplay := `UPDATE replay_request SET status = $1, message = $2, updated_at = NOW() WHERE id = $3`
	if _, err := tx.Exec(ctx, updateReplay, replayStatus, message, id); err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to update replay", err)
	}
	return nil
}

func (ReplayRepository) insertReplay(ctx context.Context, tx pgx.Tx, replay *scheduler.ReplayRequest) error {
	insertReplay := `INSERT INTO replay_request (` + replayColumnsToStore + `, created_at, updated_at) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())`
	_, err := tx.Exec(ctx, insertReplay, replay.JobName.String(), replay.Tenant.NamespaceName(), replay.Tenant.ProjectName(),
		replay.Config.StartTime, replay.Config.EndTime, replay.Config.Description, replay.Config.Parallel, replay.State, replay.Message)
	if err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to store replay", err)
	}
	return nil
}

func (ReplayRepository) getReplayRequest(ctx context.Context, tx pgx.Tx, replay *scheduler.ReplayRequest) (replayRequest, error) {
	var rr replayRequest
	getReplayRequest := `SELECT ` + replayColumns + ` FROM replay_request where project_name = $1 and job_name = $2 and start_time = $3 and end_time = $4 order by created_at desc limit 1`
	if err := tx.QueryRow(ctx, getReplayRequest, replay.Tenant.ProjectName(), replay.JobName.String(), replay.Config.StartTime, replay.Config.EndTime).
		Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel,
			&rr.Status, &rr.Message); err != nil {
		return rr, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay", err)
	}
	return rr, nil
}

func (ReplayRepository) getExecutableReplayRuns(ctx context.Context, tx pgx.Tx) ([]replayRun, error) {
	getReplayRequest := `
		WITH request AS (
			SELECT ` + replayColumns + `, created_at FROM replay_request WHERE status IN ('created', 'partial replayed', 'replayed') 
			ORDER BY updated_at DESC LIMIT 1
		)
		SELECT ` + replayRunDetailColumns + ` FROM replay_run AS run
		JOIN request AS r ON (replay_id = r.id)`

	rows, err := tx.Query(ctx, getReplayRequest)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "unable to get the stored replay", err)
	}
	defer rows.Close()

	var runs []replayRun
	for rows.Next() {
		var run replayRun
		if err := rows.Scan(&run.ID, &run.JobName, &run.NamespaceName, &run.ProjectName, &run.StartTime, &run.EndTime,
			&run.Description, &run.Parallel, &run.ReplayStatus, &run.Message, &run.ScheduledTime, &run.RunStatus, &run.CreatedAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NotFound(job.EntityJob, "executable replay not found")
			}
			return runs, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay", err)
		}
		runs = append(runs, run)
	}
	return runs, nil
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
