ALTER TABLE job ADD IF NOT EXISTS destination VARCHAR(300);
CREATE INDEX IF NOT EXISTS job_destination_idx ON job (destination);