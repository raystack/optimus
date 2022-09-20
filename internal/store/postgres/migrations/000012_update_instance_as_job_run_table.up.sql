CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS job_run (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- either job_id will be set [trigger: schedule]
    job_id UUID,
    -- or namespace & specification will be set [trigger: manual]
    namespace_id UUID,
    specification JSONB,

    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status varchar(30) NOT NULL,
    trigger varchar(30) NOT NULL,

    data JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS job_run_job_id_idx ON job_run (job_id);
CREATE INDEX IF NOT EXISTS job_run_namespace_id_idx ON job_run (namespace_id);
CREATE INDEX IF NOT EXISTS job_run_status_idx ON job_run (status);
CREATE INDEX IF NOT EXISTS job_run_trigger_idx ON job_run (trigger);

-- recreate instance table as its purpose has changed
DROP TABLE IF EXISTS instance;
CREATE TABLE IF NOT EXISTS instance (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_run_id UUID NOT NULL REFERENCES job_run (id) ON DELETE CASCADE,

    instance_name VARCHAR(150),
    instance_type VARCHAR(50),

    executed_at TIMESTAMP,
    status VARCHAR(30),

    data JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

