ALTER TABLE IF EXISTS replay
    RENAME TO replay_old;

CREATE TABLE IF NOT EXISTS replay_request (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    project_name    VARCHAR NOT NULL,
    namespace_name  VARCHAR NOT NULL,
    job_name        VARCHAR NOT NULL,

    description VARCHAR,
    parallel    BOOLEAN,

    start_time  TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time    TIMESTAMP WITH TIME ZONE,

    job_config JSONB NOT NULL,

    status  VARCHAR(30) NOT NULL,
    message TEXT,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS replay_request_project_name_idx on replay_request(project_name);
CREATE INDEX IF NOT EXISTS replay_request_job_name_idx on replay_request(job_name);
