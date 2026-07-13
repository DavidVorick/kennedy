DROP INDEX IF EXISTS one_unfinished_conversation;

ALTER TABLE conversations ADD COLUMN last_user_message_at TEXT;
ALTER TABLE conversations ADD COLUMN ended_at TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS one_ingress_in_progress
ON conversations((1)) WHERE phase = 'ingress_in_progress';

CREATE INDEX IF NOT EXISTS conversations_ingress_queue
ON conversations(phase, last_user_message_at, started_at);

PRAGMA user_version = 2;
