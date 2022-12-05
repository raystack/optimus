-- rename id colum
DROP INDEX IF EXISTS job_run_idx;

ALTER TABLE job_run DROP CONSTRAINT IF EXISTS job_run_job_id_fkey;
ALTER TABLE job_run DROP CONSTRAINT IF EXISTS job_run_namespace_id_fkey;
ALTER TABLE job_run DROP CONSTRAINT IF EXISTS job_run_project_id_fkey;

ALTER TABLE job_run RENAME COLUMN job_run_id TO id;

ALTER TABLE job_run ADD IF NOT EXISTS job_name          VARCHAR(220); -- lenght borowed from job table
ALTER TABLE job_run ADD IF NOT EXISTS namespace_name    VARCHAR(100); -- lenght borowed from namespace table
ALTER TABLE job_run ADD IF NOT EXISTS project_name      VARCHAR(100); -- lenght borowed from project table


-- migrate the job_run references
UPDATE job_run AS jr SET job_name       = j.name FROM job       AS j WHERE jr.job_id        = j.id;
UPDATE job_run AS jr SET namespace_name = n.name FROM namespace AS n WHERE jr.namespace_id  = n.id;
UPDATE job_run AS jr SET project_name   = p.name FROM project   AS p WHERE jr.project_id    = p.id;

ALTER TABLE job_run 
    DROP COLUMN IF EXISTS job_id, 
    DROP COLUMN IF EXISTS namespace_id, 
    DROP COLUMN IF EXISTS project_id, 
    DROP COLUMN IF EXISTS attempt, 
    DROP COLUMN IF EXISTS sla_miss_delay, 
    DROP COLUMN IF EXISTS duration;

CREATE INDEX IF NOT EXISTS job_run_scheduled_idx ON job_run (scheduled_at); -- required for cleanup activities