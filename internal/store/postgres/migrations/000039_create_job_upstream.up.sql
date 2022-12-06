CREATE TABLE IF NOT EXISTS job_upstream (
    job_id UUID NOT NULL,
    job_name VARCHAR(220) NOT NULL,
    project_name VARCHAR(100) NOT NULL,

    upstream_job_id UUID,
    upstream_job_name VARCHAR(220),
    upstream_resource_urn VARCHAR(300),
    upstream_project_name VARCHAR(100),
    upstream_namespace_name VARCHAR(100),
    upstream_task_name VARCHAR(200),
    upstream_host VARCHAR(50),
    upstream_type VARCHAR(15) NOT NULL,
    upstream_state VARCHAR(15) NOT NULL,
    upstream_external BOOLEAN,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,

    CONSTRAINT job_upstream_job_id_fkey
        FOREIGN KEY(job_id)
        REFERENCES job(id)
        ON DELETE CASCADE,

    CONSTRAINT job_upstream_upstream_job_id_fkey
        FOREIGN KEY(upstream_job_id)
        REFERENCES job(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS job_upstream_job_name_idx ON job_upstream (job_name);
CREATE INDEX IF NOT EXISTS job_upstream_upstream_job_name_idx ON job_upstream (upstream_job_name);
CREATE INDEX IF NOT EXISTS job_upstream_upstream_state_idx ON job_upstream (upstream_state);
