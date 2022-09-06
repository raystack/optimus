ALTER TABLE task_run
DROP CONSTRAINT IF EXISTS task_run_job_run_id_fkey,
ADD CONSTRAINT task_run_job_run_id_fkey
   FOREIGN KEY (job_run_id)
   REFERENCES job_run(job_run_id);

ALTER TABLE sensor_run
DROP CONSTRAINT IF EXISTS sensor_run_job_run_id_fkey,
ADD CONSTRAINT sensor_run_job_run_id_fkey
   FOREIGN KEY (job_run_id)
   REFERENCES job_run(job_run_id);


ALTER TABLE hook_run
DROP CONSTRAINT IF EXISTS hook_run_job_run_id_fkey,
ADD CONSTRAINT hook_run_job_run_id_fkey
   FOREIGN KEY (job_run_id)
   REFERENCES job_run(job_run_id);