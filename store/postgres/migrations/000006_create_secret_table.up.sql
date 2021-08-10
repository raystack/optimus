CREATE TABLE IF NOT EXISTS secret (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID NOT NULL REFERENCES project (id),
    name VARCHAR(100) NOT NULL,
    value TEXT,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE,

    UNIQUE (project_id, name)
);
CREATE INDEX IF NOT EXISTS secret_name_idx ON secret (name);
CREATE INDEX IF NOT EXISTS secret_project_id_idx ON secret (project_id);