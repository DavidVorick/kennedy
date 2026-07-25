-- The Rust migration driver guards the two SQLite ALTER TABLE operations with
-- PRAGMA table_info so this remains idempotent for databases already opened by
-- the 0.1.4 generated-source implementation:
--   telegram_events.revision_update_id INTEGER
--   telegram_group_ingress.completion_reason TEXT
CREATE INDEX IF NOT EXISTS telegram_events_source_message
ON telegram_events(chat_id, message_id, session_kind);
