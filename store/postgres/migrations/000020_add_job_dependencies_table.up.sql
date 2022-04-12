CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS job_dependency (
    job_id UUID NOT NULL,
    project_id UUID NOT NULL REFERENCES project (id),
    dependent_job_id UUID NOT NULL,
    dependent_project_id UUID NOT NULL REFERENCES project (id),
    type VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS job_dependency_job_id_idx ON job_dependency (job_id);
CREATE INDEX IF NOT EXISTS job_dependency_project_id_idx ON job_dependency (project_id);