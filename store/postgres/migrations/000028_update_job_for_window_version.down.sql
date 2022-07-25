ALTER TABLE job
    DROP COLUMN window_offset,
    DROP COLUMN window_size,
    RENAME COLUMN old_window_offset to window_offset;
    RENAME COLUMN old_window_size to window_size,
