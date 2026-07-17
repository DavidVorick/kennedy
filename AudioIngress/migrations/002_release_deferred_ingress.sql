BEGIN IMMEDIATE;

UPDATE audio_recordings
SET next_attempt_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+15 seconds'),
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE next_attempt_at IS NOT NULL
  AND id IN (
      SELECT recording_id
      FROM audio_ingress_pieces
      WHERE phase = 'ingress_in_progress'
  );

UPDATE audio_ingress_pieces
SET phase = 'ingress_pending',
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    version = version + 1
WHERE phase = 'ingress_in_progress'
  AND recording_id IN (
      SELECT id FROM audio_recordings WHERE next_attempt_at IS NOT NULL
  );

PRAGMA user_version = 2;

COMMIT;
