CREATE TABLE IF NOT EXISTS job (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID NOT NULL REFERENCES project (id),
    version INTEGER,
    name VARCHAR(220) NOT NULL,
    owner VARCHAR(100),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    interval VARCHAR(50),
    depends_on_past BOOLEAN,
    catch_up BOOLEAN,
    dependencies JSONB,

    task_name VARCHAR(200),
    task_config JSONB,
    window_size BIGINT,
    window_offset BIGINT,
    window_truncate_to VARCHAR(10),

    assets JSONB,
    hooks JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE,

    UNIQUE (project_id, name)
);
CREATE INDEX IF NOT EXISTS job_name_idx ON job (name);
CREATE INDEX IF NOT EXISTS job_project_id_idx ON job (project_id);