ALTER TABLE job_old
RENAME TO job;

ALTER INDEX IF EXISTS job_old_name_idx RENAME TO job_name_idx;
ALTER INDEX IF EXISTS job_old_destination_idx RENAME TO job_destination_idx;
ALTER INDEX IF EXISTS job_old_namespace_id_idx RENAME TO job_namespace_id_idx;
ALTER INDEX IF EXISTS job_old_project_id_idx RENAME TO job_project_id_idx;
