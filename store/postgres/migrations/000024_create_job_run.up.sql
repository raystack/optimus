CREATE TABLE IF NOT EXISTS job_run (
    job_run_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    job_id          UUID NOT NULL REFERENCES job(id),
    project_id      UUID NOT NULL REFERENCES project(id),
    namespace_id    UUID NOT NULL REFERENCES namespace(id),

    scheduled_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    start_time      TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time        TIMESTAMP WITH TIME ZONE,

    status          VARCHAR(30) NOT NULL,
    attempt         INT,
    sla_miss_delay  INT,
    duration        INT,
    sla_definition  INT,
    data            JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    UNIQUE (job_id, project_id, namespace_id, scheduled_at, attempt)
);
CREATE INDEX IF NOT EXISTS job_run_idx ON job_run(job_id, project_id, namespace_id, scheduled_at);