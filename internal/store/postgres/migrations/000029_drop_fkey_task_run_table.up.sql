ALTER TABLE task_run
DROP CONSTRAINT IF EXISTS task_run_job_run_id_fkey;

ALTER TABLE sensor_run
DROP CONSTRAINT IF EXISTS sensor_run_job_run_id_fkey;

ALTER TABLE hook_run
DROP CONSTRAINT IF EXISTS hook_run_job_run_id_fkey;