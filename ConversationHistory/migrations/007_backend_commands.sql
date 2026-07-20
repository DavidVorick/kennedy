BEGIN IMMEDIATE;

ALTER TABLE conversations ADD COLUMN start_request_id TEXT;

CREATE UNIQUE INDEX conversations_start_request
ON conversations(start_request_id)
WHERE start_request_id IS NOT NULL;

CREATE TABLE conversation_commands (
    id TEXT PRIMARY KEY NOT NULL,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK(sequence > 0),
    kind TEXT NOT NULL CHECK(kind IN ('message', 'retry', 'end', 'send-and-end')),
    payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'processing', 'complete')),
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0, 1)),
    outcome_json TEXT,
    created_at TEXT NOT NULL,
    processing_started_at TEXT,
    completed_at TEXT,
    UNIQUE(conversation_id, sequence)
);

CREATE INDEX conversation_commands_queue
ON conversation_commands(status, conversation_id, sequence);

PRAGMA user_version = 7;

COMMIT;
