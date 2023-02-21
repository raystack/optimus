CREATE TABLE IF NOT EXISTS replay_run (
    replay_id UUID,

    scheduled_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    status          VARCHAR(30) NOT NULL,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    CONSTRAINT replay_run_replay_id_fkey
        FOREIGN KEY(replay_id)
        REFERENCES replay_request(id)
        ON DELETE CASCADE
);
