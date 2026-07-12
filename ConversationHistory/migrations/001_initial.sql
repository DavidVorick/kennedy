CREATE TABLE IF NOT EXISTS conversations (
    id TEXT PRIMARY KEY NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('active', 'ingress_pending', 'ingress_in_progress', 'complete')),
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    state_json TEXT NOT NULL,
    provenance_id TEXT,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_unfinished_conversation
ON conversations ((1))
WHERE phase <> 'complete';

CREATE INDEX IF NOT EXISTS conversations_updated_at
ON conversations (updated_at DESC);
