CREATE TABLE memory_ingress_jobs (
    id TEXT PRIMARY KEY NOT NULL,
    source_kind TEXT NOT NULL CHECK(source_kind IN ('conversation', 'audio')),
    source_id TEXT NOT NULL,
    source_created_at TEXT NOT NULL,
    source_position INTEGER NOT NULL DEFAULT 0 CHECK(source_position >= 0),
    phase TEXT NOT NULL CHECK(phase IN (
        'ingress_pending', 'ingress_in_progress', 'ingress_failed', 'complete'
    )),
    provenance_id TEXT,
    state_json TEXT NOT NULL DEFAULT '{}',
    version INTEGER NOT NULL CHECK(version > 0),
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count >= 0),
    failures_json TEXT NOT NULL DEFAULT '[]',
    next_attempt_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source_kind, source_id)
);

CREATE UNIQUE INDEX one_memory_ingress_in_progress
ON memory_ingress_jobs((1)) WHERE phase = 'ingress_in_progress';

CREATE INDEX memory_ingress_work_queue
ON memory_ingress_jobs(phase, next_attempt_at, source_created_at, source_position, id);

PRAGMA user_version = 1;
