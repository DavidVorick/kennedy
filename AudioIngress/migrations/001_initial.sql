CREATE TABLE audio_recordings (
    id TEXT PRIMARY KEY NOT NULL,
    sha256 TEXT NOT NULL UNIQUE CHECK(length(sha256) = 64),
    original_filename TEXT NOT NULL,
    content_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
    source_created_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    original_relative_path TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN (
        'uploaded', 'chunking', 'transcribing', 'reconciling',
        'ready_for_ingress', 'ingressing', 'ingress_failed', 'complete', 'failed'
    )),
    gemini_model TEXT NOT NULL,
    reconciliation_model TEXT NOT NULL,
    reconciliation_reasoning TEXT NOT NULL,
    final_transcript TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    next_attempt_at TEXT,
    last_error TEXT
);

CREATE TABLE audio_chunks (
    recording_id TEXT NOT NULL REFERENCES audio_recordings(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL CHECK(chunk_index >= 0),
    audio_start_ms INTEGER NOT NULL CHECK(audio_start_ms >= 0),
    audio_end_ms INTEGER NOT NULL CHECK(audio_end_ms > audio_start_ms),
    relative_path TEXT NOT NULL,
    transcript_json TEXT,
    PRIMARY KEY(recording_id, chunk_index)
);

CREATE TABLE audio_ingress_pieces (
    id TEXT PRIMARY KEY NOT NULL,
    recording_id TEXT NOT NULL REFERENCES audio_recordings(id) ON DELETE CASCADE,
    piece_index INTEGER NOT NULL CHECK(piece_index >= 0),
    transcript_text TEXT NOT NULL,
    estimated_tokens INTEGER NOT NULL CHECK(estimated_tokens > 0 AND estimated_tokens <= 50000),
    phase TEXT NOT NULL CHECK(phase IN (
        'ingress_pending', 'ingress_in_progress', 'ingress_failed', 'complete'
    )),
    provenance_id TEXT,
    state_json TEXT NOT NULL DEFAULT '{}',
    version INTEGER NOT NULL DEFAULT 1 CHECK(version > 0),
    ingress_failure_count INTEGER NOT NULL DEFAULT 0 CHECK(ingress_failure_count >= 0),
    ingress_failures_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(recording_id, piece_index)
);

CREATE INDEX audio_recordings_work_queue
ON audio_recordings(status, next_attempt_at, received_at);

CREATE INDEX audio_ingress_piece_queue
ON audio_ingress_pieces(phase, created_at, recording_id, piece_index);

CREATE UNIQUE INDEX one_audio_ingress_in_progress
ON audio_ingress_pieces((1)) WHERE phase = 'ingress_in_progress';

PRAGMA user_version = 1;
