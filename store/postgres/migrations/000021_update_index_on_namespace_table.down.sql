DROP INDEX IF EXISTS namespace_name_idx;
CREATE INDEX IF NOT EXISTS namespace_name_idx ON job (name);

DROP INDEX IF EXISTS namespace_project_id_idx;
CREATE INDEX IF NOT EXISTS namespace_project_id_idx ON job (project_id);