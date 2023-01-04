ALTER TABLE job

DROP COLUMN schedule,
ADD COLUMN start_date TIMESTAMP NOT NULL,
ADD COLUMN end_date TIMESTAMP,
ADD COLUMN interval VARCHAR(50),
ADD COLUMN depends_on_past BOOLEAN,
ADD COLUMN catch_up BOOLEAN,
ADD COLUMN retry JSONB,

DROP COLUMN window_spec,
ADD COLUMN window_size VARCHAR(10),
ADD COLUMN window_offset VARCHAR(10),
ADD COLUMN window_truncate_to VARCHAR(10);
