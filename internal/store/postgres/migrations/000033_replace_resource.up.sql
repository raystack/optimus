ALTER TABLE IF EXISTS resource
    RENAME TO resource_old;

CREATE TABLE IF NOT EXISTS resource (
    full_name VARCHAR(256) NOT NULL,
    kind VARCHAR(32) NOT NULL,
    store VARCHAR(32) NOT NULL,

    project_name VARCHAR NOT NULL,
    namespace_name VARCHAR NOT NULL,

    metadata JSONB NOT NULL,
    spec JSONB NOT NULL,

    urn VARCHAR(1024) NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    status VARCHAR(32) NOT NULL,
    exist_in_store BOOLEAN NOT NULL,

    UNIQUE(project_name, namespace_name, store, full_name),
    UNIQUE(project_name, namespace_name, urn)
);

CREATE INDEX IF NOT EXISTS resource_project_name_namespace_name_idx on resource(project_name, namespace_name);
CREATE INDEX IF NOT EXISTS resource_full_name_idx on resource(full_name);
CREATE INDEX IF NOT EXISTS resource_urn_idx on resource(urn);
