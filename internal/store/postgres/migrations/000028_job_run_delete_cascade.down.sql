ALTER TABLE job_run
DROP CONSTRAINT job_run_job_id_fkey,
ADD CONSTRAINT job_run_job_id_fkey
   FOREIGN KEY (job_id)
   REFERENCES job(id);

ALTER TABLE task_run
Drop CONSTRAINT task_run_job_id_fkey;

ALTER TABLE sensor_run
Drop CONSTRAINT sensor_run_job_id_fkey;

ALTER TABLE hook_run
Drop CONSTRAINT hook_run_job_id_fkey;