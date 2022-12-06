ALTER TABLE job_run DROP CONSTRAINT IF EXISTS job_run_job_id_fkey;

ALTER TABLE job
RENAME TO job_old;

ALTER INDEX IF EXISTS job_name_idx RENAME TO job_old_name_idx;
ALTER INDEX IF EXISTS job_destination_idx RENAME TO job_old_destination_idx;
ALTER INDEX IF EXISTS job_namespace_id_idx RENAME TO job_old_namespace_id_idx;
ALTER INDEX IF EXISTS job_project_id_idx RENAME TO job_old_project_id_idx;
