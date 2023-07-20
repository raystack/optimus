DROP TABLE IF EXISTS JOB_OLD;
DROP TABLE IF EXISTS JOB_DEPLOYMENT, MIGRATION_STEPS;
DROP TABLE IF EXISTS BACKUP_OLD, REPLAY_OLD, SECRET_OLD, RESOURCE_OLD;
DROP TABLE IF EXISTS NAMESPACE_OLD, PROJECT_OLD;

ALTER TABLE sensor_run DROP CONSTRAINT IF EXISTS sensor_run_job_id_fkey;
ALTER TABLE task_run   DROP CONSTRAINT IF EXISTS task_run_job_id_fkey;
ALTER TABLE hook_run   DROP CONSTRAINT IF EXISTS hook_run_job_id_fkey;

CREATE INDEX IF NOT EXISTS idx_job_run_start_time    ON job_run (start_time);
CREATE INDEX IF NOT EXISTS idx_sensor_run_start_time ON sensor_run (start_time);
CREATE INDEX IF NOT EXISTS idx_hook_run_start_time   ON hook_run (start_time);
CREATE INDEX IF NOT EXISTS idx_task_run_start_time   ON task_run (start_time);
