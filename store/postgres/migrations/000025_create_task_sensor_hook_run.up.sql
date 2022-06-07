CREATE TABLE IF NOT EXISTS task_run (
    task_run_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    job_run_id      UUID NOT NULL REFERENCES job_run(job_run_id),
    
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

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    UNIQUE ( task_run_id , job_id, project_id, namespace_id, scheduled_at)
);
CREATE INDEX IF NOT EXISTS task_run_idx ON task_run( task_run_id, job_id, project_id, namespace_id, scheduled_at);

CREATE TABLE IF NOT EXISTS hook_run (
    hook_run_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    job_run_id      UUID NOT NULL REFERENCES job_run(job_run_id),
    
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

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    UNIQUE (hook_run_id , job_id, project_id, namespace_id, scheduled_at)
);
CREATE INDEX IF NOT EXISTS hook_run_idx ON hook_run(hook_run_id, job_id, project_id, namespace_id, scheduled_at);

CREATE TABLE IF NOT EXISTS sensor_run (
    sensor_run_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    job_run_id      UUID NOT NULL REFERENCES job_run(job_run_id),
    
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

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    UNIQUE ( sensor_run_id, job_id, project_id, namespace_id, scheduled_at )
);
CREATE INDEX IF NOT EXISTS sensor_run_idx ON sensor_run( sensor_run_id, job_id, project_id, namespace_id, scheduled_at );