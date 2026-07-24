BEGIN IMMEDIATE;

ALTER TABLE audio_ingress_pieces ADD COLUMN next_attempt_at TEXT;

DROP INDEX audio_ingress_piece_queue;
CREATE INDEX audio_ingress_piece_queue
ON audio_ingress_pieces(phase, next_attempt_at, recording_id, piece_index, id);

UPDATE audio_ingress_pieces
SET next_attempt_at = (
    SELECT audio_recordings.next_attempt_at
    FROM audio_recordings
    WHERE audio_recordings.id = audio_ingress_pieces.recording_id
)
WHERE phase = 'ingress_pending'
  AND next_attempt_at IS NULL;

CREATE TABLE audio_ingress_migrations (
    name TEXT PRIMARY KEY NOT NULL,
    completed_at TEXT NOT NULL,
    details TEXT NOT NULL
);

PRAGMA user_version = 5;

COMMIT;
