BEGIN IMMEDIATE;

ALTER TABLE audio_recordings ADD COLUMN transcription_status_json TEXT;

PRAGMA user_version = 3;

COMMIT;
