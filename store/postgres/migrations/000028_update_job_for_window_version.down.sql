ALTER TABLE job
    DROP COLUMN window_offset;
ALTER TABLE job
    DROP COLUMN window_size;
ALTER TABLE job
    RENAME COLUMN old_window_offset to window_offset;
ALTER TABLE job
    RENAME COLUMN old_window_size to window_size;
