ALTER TABLE task_run
ADD CONSTRAINT IF NOT EXISTS task_run_job_run_id_fkey
   FOREIGN KEY (job_run_id)
   REFERENCES job_run(job_run_id);

ALTER TABLE sensor_run
ADD CONSTRAINT IF NOT EXISTS sensor_run_job_run_id_fkey
   FOREIGN KEY (job_run_id)
   REFERENCES job_run(job_run_id);


ALTER TABLE hook_run
ADD CONSTRAINT IF NOT EXISTS hook_run_job_run_id_fkey
   FOREIGN KEY (job_run_id)
   REFERENCES job_run(job_run_id);