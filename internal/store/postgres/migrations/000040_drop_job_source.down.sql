CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS job_source (
    job_id UUID NOT NULL,
    project_id UUID NOT NULL REFERENCES project (id),
    resource_urn VARCHAR(300) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    unique(project_id, job_id, resource_urn)
);

CREATE INDEX IF NOT EXISTS job_source_job_id_idx ON job_source (job_id);
CREATE INDEX IF NOT EXISTS job_source_resource_urn_idx ON job_source (resource_urn);
