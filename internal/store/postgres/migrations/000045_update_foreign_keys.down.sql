ALTER TABLE resource DROP IF EXISTS id;

ALTER TABLE resource
    DROP CONSTRAINT IF EXISTS pk_resource;

ALTER TABLE resource
    DROP CONSTRAINT IF EXISTS fk_resource_namespace;

ALTER TABLE backup
    DROP CONSTRAINT IF EXISTS fk_backup_namespace;

ALTER TABLE job
    DROP CONSTRAINT IF EXISTS fk_job_namespace;

