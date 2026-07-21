BEGIN IMMEDIATE;

UPDATE audio_recordings
SET status = 'uploaded',
    transcription_status_json = NULL,
    attempt_count = 0,
    next_attempt_at = NULL,
    last_error = NULL,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE status = 'failed'
  AND last_error LIKE '%WAV audio ended before the planned interval%';

PRAGMA user_version = 4;

COMMIT;
