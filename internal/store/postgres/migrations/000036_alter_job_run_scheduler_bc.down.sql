ALTER TABLE job_run RENAME COLUMN id TO job_run_id;

ALTER TABLE job_run 
    ADD IF NOT EXISTS job_id uuid, 
    ADD IF NOT EXISTS namespace_id uuid, 
    ADD IF NOT EXISTS project_id uuid, 
    ADD IF NOT EXISTS attempt integer, 
    ADD IF NOT EXISTS sla_miss_delay integer, 
    ADD IF NOT EXISTS duration integer;


-- migrate the job_run references
UPDATE job_run AS jr SET job_id       = j.id FROM job       AS j WHERE jr.job_name        = j.name;
UPDATE job_run AS jr SET namespace_id = n.id FROM namespace AS n WHERE jr.namespace_name  = n.name;
UPDATE job_run AS jr SET project_id   = p.id FROM project   AS p WHERE jr.project_name    = p.name;


ALTER TABLE job_run 
    DROP COLUMN IF EXISTS job_name, 
    DROP COLUMN IF EXISTS namespace_name, 
    DROP COLUMN IF EXISTS project_name; 