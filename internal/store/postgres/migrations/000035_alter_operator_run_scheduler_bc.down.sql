ALTER TABLE task_run    RENAME COLUMN id TO task_run_id;
ALTER TABLE sensor_run  RENAME COLUMN id TO sensor_run_id;
ALTER TABLE hook_run    RENAME COLUMN id TO hook_run_id;


ALTER TABLE task_run 
   ADD COLUMN IF NOT EXISTS attempt, 
   ADD COLUMN IF NOT EXISTS job_run_attempt, 
   ADD COLUMN IF NOT EXISTS duration;

ALTER TABLE sensor_run 
   ADD COLUMN IF NOT EXISTS attempt, 
   ADD COLUMN IF NOT EXISTS job_run_attempt, 
   ADD COLUMN IF NOT EXISTS duration;

ALTER TABLE hook_run 
   ADD COLUMN IF NOT EXISTS attempt, 
   ADD COLUMN IF NOT EXISTS job_run_attempt, 
   ADD COLUMN IF NOT EXISTS duration;


ALTER TABLE task_run   DROP COLUMN IF EXISTS name;
ALTER TABLE sensor_run DROP COLUMN IF EXISTS name;
ALTER TABLE hook_run   DROP COLUMN IF EXISTS name;