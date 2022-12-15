ALTER TABLE resource
ADD CONSTRAINT pk_resource PRIMARY KEY (project_name, namespace_name, store, full_name);

ALTER TABLE resource
ADD CONSTRAINT fk_resource_namespace FOREIGN KEY (project_name, namespace_name) REFERENCES namespace (project_name, name);

ALTER TABLE backup
ADD CONSTRAINT fk_backup_namespace FOREIGN KEY (project_name, namespace_name) REFERENCES namespace (project_name, name);

ALTER TABLE job
ADD CONSTRAINT fk_job_namespace FOREIGN KEY (project_name, namespace_name) REFERENCES namespace (project_name, name);

