CREATE TABLE IF NOT EXISTS authorized_users (
    bootstrap_username TEXT PRIMARY KEY,
    telegram_user_id INTEGER UNIQUE,
    username TEXT,
    display_name TEXT,
    chat_id INTEGER,
    current_conversation_id TEXT,
    paired_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS telegram_events (
    id TEXT PRIMARY KEY,
    update_id INTEGER NOT NULL UNIQUE,
    message_id INTEGER NOT NULL,
    telegram_user_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    username TEXT,
    display_name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('text', 'voice', 'document', 'reset')),
    text TEXT,
    voice_bytes BLOB,
    mime_type TEXT,
    file_name TEXT,
    duration_seconds INTEGER,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'complete')),
    conversation_id TEXT,
    processing_started_at TEXT,
    transcription TEXT,
    transcription_model TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    completion_reason TEXT
);

CREATE INDEX IF NOT EXISTS telegram_events_work_queue
ON telegram_events(status, update_id);

CREATE INDEX IF NOT EXISTS telegram_events_user_queue
ON telegram_events(telegram_user_id, status, update_id);
