ALTER TABLE job
    RENAME COLUMN window_size to old_window_size;
ALTER TABLE job
    RENAME COLUMN window_offset to old_window_offset;
ALTER TABLE job
    ADD COLUMN window_size VARCHAR(10);
ALTER TABLE job
    ADD COLUMN window_offset VARCHAR(10);
