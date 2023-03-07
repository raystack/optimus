CREATE INDEX IF NOT EXISTS sensor_run_job_run_id ON sensor_run(job_run_id);
CREATE INDEX IF NOT EXISTS task_run_job_run_id ON task_run(job_run_id);
CREATE INDEX IF NOT EXISTS hook_run_job_run_id ON hook_run(job_run_id);