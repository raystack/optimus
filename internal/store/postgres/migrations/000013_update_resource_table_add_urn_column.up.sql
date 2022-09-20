ALTER TABLE resource ADD IF NOT EXISTS urn VARCHAR(300);
CREATE INDEX IF NOT EXISTS resource_urn_idx ON resource (urn);
