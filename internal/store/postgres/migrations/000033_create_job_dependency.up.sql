CREATE TABLE IF NOT EXISTS job_dependency (
    job_name VARCHAR(220) NOT NULL,
    project_name VARCHAR(100) NOT NULL,

    dependency_job_name VARCHAR(220) NOT NULL,
    dependency_project_name VARCHAR(100) NOT NULL,
    dependency_namespace_name VARCHAR(100) NOT NULL,
    dependency_host VARCHAR(50),

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE
    );
CREATE INDEX IF NOT EXISTS job_dependency_job_name_idx ON job_dependency (job_name);
CREATE INDEX IF NOT EXISTS job_dependency_dependency_job_name_idx ON job_dependency (dependency_job_name);