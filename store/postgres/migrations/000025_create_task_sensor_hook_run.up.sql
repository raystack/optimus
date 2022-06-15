CREATE TABLE IF NOT EXISTS task_run (
    task_run_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    job_run_id      UUID NOT NULL REFERENCES job_run(job_run_id),

    start_time      TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time        TIMESTAMP WITH TIME ZONE,

    status          VARCHAR(30) NOT NULL,
    attempt         INT,
    job_run_attempt INT,
    duration        INT,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS hook_run (
    hook_run_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    job_run_id      UUID NOT NULL REFERENCES job_run(job_run_id),

    start_time      TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time        TIMESTAMP WITH TIME ZONE,

    status          VARCHAR(30) NOT NULL,
    attempt         INT,
    job_run_attempt INT,
    duration        INT,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS sensor_run (
    sensor_run_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    job_run_id      UUID NOT NULL REFERENCES job_run(job_run_id),

    start_time      TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time        TIMESTAMP WITH TIME ZONE,

    status          VARCHAR(30) NOT NULL,
    attempt         INT,
    job_run_attempt INT,
    duration        INT,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);