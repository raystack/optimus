
ALTER TABLE job_run
DROP CONSTRAINT job_run_job_id_fkey,
ADD CONSTRAINT job_run_job_id_fkey
   FOREIGN KEY (job_id)
   REFERENCES job(id)
   ON DELETE CASCADE;

ALTER TABLE task_run
ADD CONSTRAINT task_run_job_id_fkey
   FOREIGN KEY (job_run_id)
   REFERENCES job_run(job_run_id)
   ON DELETE CASCADE;

ALTER TABLE sensor_run
ADD CONSTRAINT sensor_run_job_id_fkey
   FOREIGN KEY (job_run_id)
   REFERENCES job_run(job_run_id)
   ON DELETE CASCADE;

ALTER TABLE hook_run
ADD CONSTRAINT hook_run_job_id_fkey
   FOREIGN KEY (job_run_id)
   REFERENCES job_run(job_run_id)
   ON DELETE CASCADE;