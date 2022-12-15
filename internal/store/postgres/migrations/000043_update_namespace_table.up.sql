ALTER TABLE IF EXISTS namespace
    RENAME TO namespace_old;

CREATE TABLE IF NOT EXISTS namespace (
     name VARCHAR(100) NOT NULL,
     id UUID NOT NULL UNIQUE DEFAULT uuid_generate_v4(),
     config JSONB,
     project_name VARCHAR(100) NOT NULL REFERENCES project (name),

     created_at TIMESTAMP WITH TIME ZONE NOT NULL,
     updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
     deleted_at TIMESTAMP WITH TIME ZONE,

     PRIMARY KEY (project_name, name)
);

INSERT INTO namespace (name, id, config, project_name, created_at, updated_at, deleted_at)
SELECT n.name, n.id, n.config, p.name, n.created_at, n.updated_at, n.deleted_at
FROM namespace_old n join project_old p ON n.project_id = p.id
WHERE n.deleted_at IS NULL;
