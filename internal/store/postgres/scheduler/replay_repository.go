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
	replayColumnsToStore = `job_name, namespace_name, project_name, start_time, end_time, description, parallel, job_config, status, message`
	replayColumns        = `id, ` + replayColumnsToStore + `, created_at`

	replayRunColumns       = `replay_id, scheduled_at, status`
	replayRunDetailColumns = `id as replay_id, job_name, namespace_name, project_name, start_time, end_time, description, 
parallel, job_config, r.status as replay_status, r.message as replay_message, scheduled_at, run.status as run_status, r.created_at as replay_created_at`

	updateReplayRequest = `UPDATE replay_request SET status = $1, message = $2, updated_at = NOW() WHERE id = $3`
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
	JobConfig   map[string]string

	Status  string
	Message string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (r *replayRequest) toSchedulerReplayRequest() (*scheduler.Replay, error) {
	tnnt, err := tenant.NewTenant(r.ProjectName, r.NamespaceName)
	if err != nil {
		return nil, err
	}
	conf := scheduler.NewReplayConfig(r.StartTime, r.EndTime, r.Parallel, r.JobConfig, r.Description)
	replayStatus, err := scheduler.ReplayStateFromString(r.Status)
	if err != nil {
		return nil, err
	}
	jobName, err := scheduler.JobNameFrom(r.JobName)
	if err != nil {
		return nil, err
	}
	return scheduler.NewReplay(r.ID, jobName, tnnt, conf, replayStatus, r.CreatedAt), nil
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
	JobConfig   map[string]string

	ReplayStatus string
	Message      string

	ScheduledTime time.Time
	RunStatus     string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (r *replayRun) toReplayRequest() (*scheduler.Replay, error) {
	tnnt, err := tenant.NewTenant(r.ProjectName, r.NamespaceName)
	if err != nil {
		return nil, err
	}
	conf := scheduler.NewReplayConfig(r.StartTime, r.EndTime, r.Parallel, r.JobConfig, r.Description)
	replayStatus, err := scheduler.ReplayStateFromString(r.ReplayStatus)
	if err != nil {
		return nil, err
	}
	jobName, err := scheduler.JobNameFrom(r.JobName)
	if err != nil {
		return nil, err
	}
	return scheduler.NewReplay(r.ID, jobName, tnnt, conf, replayStatus, r.CreatedAt), nil
}

func (r *replayRun) toJobRunStatus() (*scheduler.JobRunStatus, error) {
	runState, err := scheduler.StateFromString(r.RunStatus)
	if err != nil {
		return nil, err
	}
	return &scheduler.JobRunStatus{
		ScheduledAt: r.ScheduledTime.UTC(),
		State:       runState,
	}, nil
}

func (r ReplayRepository) RegisterReplay(ctx context.Context, replay *scheduler.Replay, runs []*scheduler.JobRunStatus) (uuid.UUID, error) {
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

func (r ReplayRepository) GetReplayToExecute(ctx context.Context) (*scheduler.ReplayWithRun, error) {
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
	if replayRuns == nil {
		return nil, errors.NotFound(scheduler.EntityJobRun, "no executable replay request found")
	}

	storedReplay, err := toReplay(replayRuns)
	if err != nil {
		return nil, err
	}

	// TODO: Avoid having In Progress, but instead use row lock (for update)
	if _, err := tx.Exec(ctx, updateReplayRequest, scheduler.ReplayStateInProgress, "", storedReplay.Replay.ID()); err != nil {
		return nil, errors.Wrap(scheduler.EntityJobRun, "unable to update replay", err)
	}
	return storedReplay, nil
}

func (r ReplayRepository) GetReplayRequestsByStatus(ctx context.Context, statusList []scheduler.ReplayState) ([]*scheduler.Replay, error) {
	getReplayRequest := `SELECT ` + replayColumns + ` FROM replay_request WHERE status = ANY($1)`
	rows, err := r.db.Query(ctx, getReplayRequest, statusList)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "unable to get replay list", err)
	}
	defer rows.Close()

	var replayReqs []*scheduler.Replay
	for rows.Next() {
		var rr replayRequest
		if err := rows.Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel, &rr.JobConfig,
			&rr.Status, &rr.Message, &rr.CreatedAt); err != nil {
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

func toReplay(replayRuns []*replayRun) (*scheduler.ReplayWithRun, error) {
	var storedReplay *scheduler.ReplayWithRun
	for _, run := range replayRuns {
		if storedReplay != nil {
			runState, err := scheduler.StateFromString(run.RunStatus)
			if err != nil {
				return nil, err
			}
			jobRun := &scheduler.JobRunStatus{
				ScheduledAt: run.ScheduledTime.UTC(),
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

		storedReplay = &scheduler.ReplayWithRun{
			Replay: replay,
			Runs:   []*scheduler.JobRunStatus{jobRun},
		}
	}
	return storedReplay, nil
}

func (r ReplayRepository) UpdateReplayStatus(ctx context.Context, id uuid.UUID, replayStatus scheduler.ReplayState, message string) error {
	return r.updateReplayRequest(ctx, id, replayStatus, message)
}

func (r ReplayRepository) UpdateReplay(ctx context.Context, id uuid.UUID, replayStatus scheduler.ReplayState, runs []*scheduler.JobRunStatus, message string) error {
	if err := r.updateReplayRequest(ctx, id, replayStatus, message); err != nil {
		return err
	}

	return r.updateReplayRuns(ctx, id, runs)
}

func (r ReplayRepository) GetReplayJobConfig(ctx context.Context, jobTenant tenant.Tenant, jobName scheduler.JobName, scheduledAt time.Time) (map[string]string, error) {
	getReplayRequest := `SELECT job_config FROM replay_request WHERE start_time<=$1 AND $1<=end_time ORDER BY created_at ASC`
	rows, err := r.db.Query(ctx, getReplayRequest, scheduledAt)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "unable to get replay job configs", err)
	}
	defer rows.Close()

	configs := map[string]string{}
	for rows.Next() {
		var rr replayRequest
		if err := rows.Scan(&rr.JobConfig); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, errors.NotFound(job.EntityJob, fmt.Sprintf("no replay found for scheduledAt %s", scheduledAt.String()))
			}
			return nil, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay job cnfig", err)
		}
		for k, v := range rr.JobConfig {
			configs[k] = v
		}
	}
	return configs, nil
}

func (r ReplayRepository) updateReplayRequest(ctx context.Context, id uuid.UUID, replayStatus scheduler.ReplayState, message string) error {
	if _, err := r.db.Exec(ctx, updateReplayRequest, replayStatus, message, id); err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to update replay", err)
	}
	return nil
}

func (r ReplayRepository) updateReplayRuns(ctx context.Context, id uuid.UUID, runs []*scheduler.JobRunStatus) error {
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

	deleteRuns := `DELETE FROM replay_run WHERE replay_id = $1`
	if _, err := tx.Exec(ctx, deleteRuns, id); err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to delete runs of replay", err)
	}
	return r.insertReplayRuns(ctx, tx, id, runs)
}

func (ReplayRepository) insertReplay(ctx context.Context, tx pgx.Tx, replay *scheduler.Replay) error {
	insertReplay := `INSERT INTO replay_request (` + replayColumnsToStore + `, created_at, updated_at) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())`
	_, err := tx.Exec(ctx, insertReplay, replay.JobName().String(), replay.Tenant().NamespaceName(), replay.Tenant().ProjectName(),
		replay.Config().StartTime, replay.Config().EndTime, replay.Config().Description, replay.Config().Parallel, replay.Config().JobConfig, replay.State(), replay.Message())
	if err != nil {
		return errors.Wrap(scheduler.EntityJobRun, "unable to store replay", err)
	}
	return nil
}

func (ReplayRepository) getReplayRequest(ctx context.Context, tx pgx.Tx, replay *scheduler.Replay) (replayRequest, error) {
	var rr replayRequest
	getReplayRequest := `SELECT ` + replayColumns + ` FROM replay_request where project_name = $1 and job_name = $2 and start_time = $3 and end_time = $4 order by created_at desc limit 1`
	if err := tx.QueryRow(ctx, getReplayRequest, replay.Tenant().ProjectName(), replay.JobName().String(), replay.Config().StartTime, replay.Config().EndTime).
		Scan(&rr.ID, &rr.JobName, &rr.NamespaceName, &rr.ProjectName, &rr.StartTime, &rr.EndTime, &rr.Description, &rr.Parallel, &rr.JobConfig,
			&rr.Status, &rr.Message, &rr.CreatedAt); err != nil {
		return rr, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay", err)
	}
	return rr, nil
}

func (ReplayRepository) getExecutableReplayRuns(ctx context.Context, tx pgx.Tx) ([]*replayRun, error) {
	getReplayRequest := `
		WITH request AS (
			SELECT ` + replayColumns + ` FROM replay_request WHERE status IN ('created', 'partial replayed', 'replayed') 
			ORDER BY updated_at DESC LIMIT 1
		)
		SELECT ` + replayRunDetailColumns + ` FROM replay_run AS run
		JOIN request AS r ON (replay_id = r.id)`

	rows, err := tx.Query(ctx, getReplayRequest)
	if err != nil {
		return nil, errors.Wrap(job.EntityJob, "unable to get the stored replay", err)
	}
	defer rows.Close()

	var runs []*replayRun
	for rows.Next() {
		var run replayRun
		if err := rows.Scan(&run.ID, &run.JobName, &run.NamespaceName, &run.ProjectName, &run.StartTime, &run.EndTime,
			&run.Description, &run.Parallel, &run.JobConfig, &run.ReplayStatus, &run.Message, &run.ScheduledTime, &run.RunStatus, &run.CreatedAt); err != nil {
			return runs, errors.Wrap(scheduler.EntityJobRun, "unable to get the stored replay", err)
		}
		runs = append(runs, &run)
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
