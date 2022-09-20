DROP TABLE IF EXISTS job_run;
DROP TABLE IF EXISTS instance;
CREATE TABLE IF NOT EXISTS instance (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id UUID NOT NULL REFERENCES job (id),
    scheduled_at TIMESTAMP NOT NULL,
    state VARCHAR(30),

    data JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE
);