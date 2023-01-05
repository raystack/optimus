ALTER TABLE job

ADD COLUMN schedule JSONB,
DROP COLUMN start_date,
DROP COLUMN end_date,
DROP COLUMN interval,
DROP COLUMN depends_on_past,
DROP COLUMN catch_up,
DROP COLUMN retry,

ADD COLUMN window_spec JSONB,
DROP COLUMN window_size,
DROP COLUMN window_offset,
DROP COLUMN window_truncate_to;
