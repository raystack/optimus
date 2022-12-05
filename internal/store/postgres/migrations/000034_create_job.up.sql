CREATE TABLE IF NOT EXISTS job (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    name VARCHAR(220) NOT NULL,
    version INTEGER,
    owner VARCHAR(100),
    description TEXT,

    labels JSONB,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    interval VARCHAR(50),

    depends_on_past BOOLEAN,
    catch_up BOOLEAN,
    retry JSONB, -- from behavior
    alert JSONB, -- from behavior

    static_upstreams VARCHAR(220)[], -- from dependencies column
    http_upstreams JSONB, -- from external_dependencies column

    task_name VARCHAR(200),
    task_config JSONB,

    window_size VARCHAR(10),
    window_offset VARCHAR(10),
    window_truncate_to VARCHAR(10),

    assets JSONB,
    hooks JSONB,
    metadata JSONB,

    destination VARCHAR(300),
    sources VARCHAR(300)[],

    project_name VARCHAR(100) NOT NULL,
    namespace_name VARCHAR(100) NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE,

    UNIQUE (project_name, name)
);

CREATE INDEX IF NOT EXISTS job_name_idx ON job (name);
CREATE INDEX IF NOT EXISTS job_project_name_idx ON job (project_name);
CREATE INDEX IF NOT EXISTS job_namespace_name_idx ON job (namespace_name);
CREATE INDEX IF NOT EXISTS job_destination_idx ON job (destination);

ALTER TABLE job_run
ADD CONSTRAINT job_run_job_id_fkey
   FOREIGN KEY (job_id)
   REFERENCES job(id)
   ON DELETE CASCADE;
