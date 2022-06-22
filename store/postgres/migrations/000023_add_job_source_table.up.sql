CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS job_source (
    job_id UUID NOT NULL,
    project_id UUID NOT NULL REFERENCES project (id),
    resource_urn VARCHAR(300) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS job_source_job_id_idx ON job_source (job_id);