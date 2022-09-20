DROP INDEX IF EXISTS namespace_name_idx;
CREATE INDEX IF NOT EXISTS namespace_name_idx ON namespace (name);

DROP INDEX IF EXISTS namespace_project_id_idx;
CREATE INDEX IF NOT EXISTS namespace_project_id_idx ON namespace (project_id);