CREATE TABLE IF NOT EXISTS namespace (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID NOT NULL REFERENCES project (id),
    name VARCHAR(100) NOT NULL,
    config JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE,

    UNIQUE (project_id, name)
);

CREATE INDEX IF NOT EXISTS namespace_name_idx ON job (name);
CREATE INDEX IF NOT EXISTS namespace_project_id_idx ON job (project_id);

ALTER TABLE job ADD IF NOT EXISTS namespace_id UUID NOT NULL REFERENCES namespace (id);
CREATE INDEX IF NOT EXISTS job_namespace_id_idx ON job (namespace_id);

ALTER TABLE resource ADD IF NOT EXISTS namespace_id UUID NOT NULL REFERENCES namespace (id);
CREATE INDEX IF NOT EXISTS resource_namespace_id_idx ON resource (namespace_id);
