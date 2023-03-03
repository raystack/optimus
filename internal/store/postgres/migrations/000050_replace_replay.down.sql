DROP TABLE IF EXISTS replay_request;

ALTER TABLE IF EXISTS replay_old
    RENAME TO replay;
