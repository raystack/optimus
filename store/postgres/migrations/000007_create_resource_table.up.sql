CREATE TABLE IF NOT EXISTS resource (
   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
   project_id UUID NOT NULL REFERENCES project (id),
   datastore VARCHAR(100) NOT NULL,

   version INTEGER,
   name VARCHAR(250) NOT NULL,
   type VARCHAR(100) NOT NULL,

   spec BYTEA,
   assets JSONB,
   labels JSONB,

   created_at TIMESTAMP WITH TIME ZONE NOT NULL,
   updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
   deleted_at TIMESTAMP WITH TIME ZONE,

   UNIQUE (project_id, datastore, name)
);
CREATE INDEX IF NOT EXISTS resource_name_idx ON resource (name);
CREATE INDEX IF NOT EXISTS resource_project_id_idx ON resource (project_id);