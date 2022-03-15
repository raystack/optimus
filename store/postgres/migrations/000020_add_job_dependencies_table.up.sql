CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS job_dependency (
    job_id UUID NOT NULL,
    project_id UUID NOT NULL,
    dependent_job_id UUID NOT NULL,
    dependent_project_id UUID NOT NULL,
    type VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);
