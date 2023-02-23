package scheduler

import (
	"github.com/odpf/optimus/core/job"
	"github.com/odpf/optimus/core/tenant"
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

	replayRunColumns       = `replay_id, scheduled_at, logical_time, status`
	replayRunDetailColumns = `id as replay_id, job_name, namespace_name, project_name, start_time, end_time, description, 
parallel, r.status as replay_status, r.message as replay_message, scheduled_at, logical_time, run.status as run_status, r.created_at as replay_created_at`
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
	LogicalTime   time.Time
	RunStatus     string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (r replayRun) toReplay() (*scheduler.Replay, error) {
	tnnt, err := tenant.NewTenant(r.ProjectName, r.NamespaceName)
	if err != nil {
		return nil, err
	}
	conf := scheduler.NewReplayConfig(r.StartTime, r.EndTime, r.Parallel, r.Description)

	runState, err := scheduler.StateFromString(r.RunStatus)
	if err != nil {
		return nil, err
	}
	jobRun := &scheduler.JobRunStatus{
		ScheduledAt: r.ScheduledTime,
		State:       runState,
		LogicalTime: r.LogicalTime,
	}
	replayStatus, err := scheduler.ReplayStateFromString(r.ReplayStatus)
	if err != nil {
		return nil, err
	}
	jobName, err := scheduler.JobNameFrom(r.JobName)
	if err != nil {
		return nil, err
	}
	return scheduler.NewReplay(jobName, tnnt, conf, []*scheduler.JobRunStatus{jobRun}, replayStatus), nil
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

func (r ReplayRepository) GetReplaysToExecute(ctx context.Context) ([]*scheduler.StoredReplay, error) {
	replayRuns, err := r.getExecutableReplays(ctx)
	if err != nil {
		return nil, err
	}

	replayIDMap := make(map[uuid.UUID]*scheduler.StoredReplay)
	for _, run := range replayRuns {
		if storedReplay, ok := replayIDMap[run.ID]; !ok {
			runState, err := scheduler.StateFromString(run.RunStatus)
			if err != nil {
				return nil, err
			}
			jobRun := &scheduler.JobRunStatus{
				ScheduledAt: run.ScheduledTime,
				State:       runState,
				LogicalTime: run.LogicalTime,
			}
			storedReplay.Replay.Runs = append(storedReplay.Replay.Runs, jobRun)
			continue
		}

		replay, err := run.toReplay()
		if err != nil {
			return nil, err
		}

		replayIDMap[run.ID] = &scheduler.StoredReplay{
			ID:     run.ID,
			Replay: replay,
		}
	}

	var storedReplays []*scheduler.StoredReplay
	for _, replay := range replayIDMap {
		storedReplays = append(storedReplays, replay)
	}
	return storedReplays, nil
}

func (r ReplayRepository) UpdateReplay(ctx context.Context, replayID uuid.UUID, state scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error {
	panic("unimplemented")
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

func (r ReplayRepository) getExecutableReplays(ctx context.Context) ([]replayRun, error) {
	getReplayRequest := `SELECT ` + replayRunDetailColumns + ` FROM replay_run AS run join replay_request AS r ON (replay_id = r.id)`

	rows, err := r.db.Query(ctx, getReplayRequest)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "unable to get the stored replay", err)
	}
	defer rows.Close()

	var runs []replayRun
	for rows.Next() {
		var run replayRun
		if err := rows.Scan(&run.ID, &run.JobName, &run.NamespaceName, &run.ProjectName, &run.StartTime, &run.EndTime,
			&run.Description, &run.Parallel, &run.ReplayStatus, &run.Message, &run.ScheduledTime, &run.LogicalTime, &run.RunStatus, &run.CreatedAt); err != nil {
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
	insertReplayRun := `INSERT INTO replay_run (` + replayRunColumns + `, created_at, updated_at) values ($1, $2, $3, $4, NOW(), NOW())`
	for _, run := range runs {
		_, err := tx.Exec(ctx, insertReplayRun, replayID, run.ScheduledAt, run.LogicalTime, run.State)
		if err != nil {
			return errors.Wrap(scheduler.EntityJobRun, "unable to store replay", err)
		}
	}
	return nil
}

func NewReplayRepository(db *pgxpool.Pool) *ReplayRepository {
	return &ReplayRepository{db: db}
}
