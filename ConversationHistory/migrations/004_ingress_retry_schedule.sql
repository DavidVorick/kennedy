BEGIN IMMEDIATE;

ALTER TABLE conversations ADD COLUMN ingress_next_attempt_at TEXT;

UPDATE conversations
SET phase = 'ingress_pending',
    ingress_next_attempt_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+15 seconds'),
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    version = version + 1
WHERE phase = 'ingress_in_progress'
  AND ingress_failure_count > 0;

DROP INDEX IF EXISTS conversations_ingress_queue;

CREATE INDEX conversations_ingress_queue
ON conversations(phase, ingress_next_attempt_at, last_user_message_at, started_at);

PRAGMA user_version = 4;

COMMIT;
