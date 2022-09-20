DROP TABLE IF EXISTS namespace;

DROP INDEX IF EXISTS namespace_name_idx;
DROP INDEX IF EXISTS namespace_project_id_idx;

ALTER TABLE job DROP COLUMN IF EXISTS namespace_id;
DROP INDEX IF EXISTS job_namespace_id_idx;

ALTER TABLE resource DROP COLUMN IF EXISTS namespace_id;
DROP INDEX IF EXISTS resource_namespace_id_idx;
