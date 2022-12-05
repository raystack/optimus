ALTER TABLE IF EXISTS backup
    RENAME TO backup_old;

CREATE TABLE IF NOT EXISTS backup (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    store VARCHAR(32) NOT NULL,
    project_name VARCHAR NOT NULL,
    namespace_name VARCHAR NOT NULL,

    description VARCHAR,
    resource_names TEXT ARRAY,
    config JSONB NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS backup_project_name_namespace_name_idx on backup(project_name, namespace_name);
