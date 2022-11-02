CREATE TABLE IF NOT EXISTS job_dependency (
    job_name VARCHAR(220) NOT NULL,
    project_name VARCHAR(100) NOT NULL,

    dependency_job_name VARCHAR(220),
    dependency_resource_urn VARCHAR(300),
    dependency_project_name VARCHAR(100),
    dependency_namespace_name VARCHAR(100),
    dependency_host VARCHAR(50),
    dependency_type VARCHAR(15) NOT NULL,
    dependency_state VARCHAR(15) NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE
    );
CREATE INDEX IF NOT EXISTS job_dependency_job_name_idx ON job_dependency (job_name);
CREATE INDEX IF NOT EXISTS job_dependency_dependency_job_name_idx ON job_dependency (dependency_job_name);
CREATE INDEX IF NOT EXISTS job_dependency_dependency_state_idx ON job_dependency (dependency_state);