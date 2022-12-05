-- rename id colum
ALTER TABLE task_run    RENAME COLUMN task_run_id   TO id;
ALTER TABLE sensor_run  RENAME COLUMN sensor_run_id TO id;
ALTER TABLE hook_run    RENAME COLUMN hook_run_id   TO id;

ALTER TABLE task_run 
    DROP COLUMN IF EXISTS attempt, 
    DROP COLUMN IF EXISTS job_run_attempt, 
    DROP COLUMN IF EXISTS duration;

ALTER TABLE sensor_run 
    DROP COLUMN IF EXISTS attempt, 
    DROP COLUMN IF EXISTS job_run_attempt, 
    DROP COLUMN IF EXISTS duration;

ALTER TABLE hook_run 
    DROP COLUMN IF EXISTS attempt, 
    DROP COLUMN IF EXISTS job_run_attempt, 
    DROP COLUMN IF EXISTS duration;

ALTER TABLE task_run    ADD IF NOT EXISTS name VARCHAR(220);
ALTER TABLE sensor_run  ADD IF NOT EXISTS name VARCHAR(220);
ALTER TABLE hook_run    ADD IF NOT EXISTS name VARCHAR(220);