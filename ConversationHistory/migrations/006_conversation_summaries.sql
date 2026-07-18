BEGIN IMMEDIATE;

ALTER TABLE conversations ADD COLUMN summary_state_json TEXT;

PRAGMA user_version = 6;

COMMIT;
