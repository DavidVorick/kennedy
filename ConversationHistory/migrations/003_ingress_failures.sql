BEGIN IMMEDIATE;

DROP INDEX IF EXISTS one_ingress_in_progress;

ALTER TABLE conversations RENAME TO conversations_v2;

CREATE TABLE conversations (
    id TEXT PRIMARY KEY NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('active', 'ingress_pending', 'ingress_in_progress', 'ingress_failed', 'complete')),
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    state_json TEXT NOT NULL,
    provenance_id TEXT,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    last_user_message_at TEXT,
    ended_at TEXT,
    ingress_failure_count INTEGER NOT NULL DEFAULT 0 CHECK (ingress_failure_count >= 0),
    ingress_failures_json TEXT NOT NULL DEFAULT '[]'
);

INSERT INTO conversations (
    id, phase, started_at, updated_at, state_json, provenance_id, version,
    last_user_message_at, ended_at, ingress_failure_count, ingress_failures_json
)
SELECT
    id, phase, started_at, updated_at, state_json, provenance_id, version,
    last_user_message_at, ended_at, 0, '[]'
FROM conversations_v2;

DROP TABLE conversations_v2;

CREATE UNIQUE INDEX one_ingress_in_progress
ON conversations((1)) WHERE phase = 'ingress_in_progress';

CREATE INDEX conversations_ingress_queue
ON conversations(phase, last_user_message_at, started_at);

CREATE INDEX conversations_updated_at
ON conversations(updated_at DESC);

PRAGMA user_version = 3;

COMMIT;
