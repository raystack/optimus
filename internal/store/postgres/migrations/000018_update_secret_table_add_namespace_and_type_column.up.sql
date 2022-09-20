ALTER TABLE secret ADD IF NOT EXISTS namespace_id UUID default null REFERENCES namespace (id);
ALTER TABLE secret ADD IF NOT EXISTS type VARCHAR(15);

CREATE INDEX IF NOT EXISTS secret_namespace_id_idx ON secret (namespace_id);
CREATE INDEX IF NOT EXISTS secret_type_idx ON secret (type);