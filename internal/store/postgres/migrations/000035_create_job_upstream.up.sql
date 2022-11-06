CREATE TABLE IF NOT EXISTS job_upstream (
    job_name VARCHAR(220) NOT NULL,
    project_name VARCHAR(100) NOT NULL,

    upstream_job_name VARCHAR(220),
    upstream_resource_urn VARCHAR(300),
    upstream_project_name VARCHAR(100),
    upstream_namespace_name VARCHAR(100),
    upstream_host VARCHAR(50),
    upstream_type VARCHAR(15) NOT NULL,
    upstream_state VARCHAR(15) NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS job_upstream_job_name_idx ON job_upstream (job_name);
CREATE INDEX IF NOT EXISTS job_upstream_upstream_job_name_idx ON job_upstream (upstream_job_name);
CREATE INDEX IF NOT EXISTS job_upstream_upstream_state_idx ON job_upstream (upstream_state);
