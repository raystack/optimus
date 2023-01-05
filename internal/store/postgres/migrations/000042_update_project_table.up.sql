ALTER TABLE IF EXISTS project
    RENAME TO project_old;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS project (
   name VARCHAR(100) PRIMARY KEY,
   id UUID NOT NULL UNIQUE DEFAULT uuid_generate_v4(),

   config JSONB,
   created_at TIMESTAMP WITH TIME ZONE NOT NULL,
   updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
   deleted_at TIMESTAMP WITH TIME ZONE
);

INSERT INTO project (name, id, config, created_at, updated_at, deleted_at)
SELECT name, id, config, created_at, updated_at, deleted_at
FROM project_old
WHERE deleted_at IS NULL;