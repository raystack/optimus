ALTER TABLE job
    RENAME COLUMN window_size to old_window_size,
    RENAME COLUMN window_offset to old_window_offset,
    ADD COLUMN window_size VARCHAR(10),
    ADD COLUMN window_offset VARCHAR(10);
