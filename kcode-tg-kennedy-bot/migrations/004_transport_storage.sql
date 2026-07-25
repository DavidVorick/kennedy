CREATE TABLE IF NOT EXISTS telegram_private_sessions (
    telegram_user_id INTEGER PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    current_conversation_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS telegram_groups (
    group_id TEXT PRIMARY KEY,
    current_chat_id INTEGER NOT NULL UNIQUE,
    title TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'quarantined'
        CHECK(state IN ('quarantined', 'allowed')),
    roster_complete INTEGER NOT NULL DEFAULT 0 CHECK(roster_complete IN (0, 1)),
    quarantine_reason TEXT,
    last_invocation_message_id INTEGER,
    background_cursor_message_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS telegram_group_chat_ids (
    chat_id INTEGER PRIMARY KEY,
    group_id TEXT NOT NULL REFERENCES telegram_groups(group_id) ON DELETE CASCADE,
    first_seen_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS telegram_group_chat_ids_group
ON telegram_group_chat_ids(group_id, chat_id);

CREATE TABLE IF NOT EXISTS telegram_group_members (
    group_id TEXT NOT NULL REFERENCES telegram_groups(group_id) ON DELETE CASCADE,
    telegram_user_id INTEGER NOT NULL,
    username TEXT,
    display_name TEXT NOT NULL,
    membership TEXT NOT NULL
        CHECK(membership IN ('member', 'administrator', 'creator', 'left', 'kicked')),
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(group_id, telegram_user_id)
);

CREATE INDEX IF NOT EXISTS telegram_group_members_active
ON telegram_group_members(group_id, membership, telegram_user_id);

CREATE TABLE IF NOT EXISTS telegram_group_sessions (
    group_id TEXT NOT NULL REFERENCES telegram_groups(group_id) ON DELETE CASCADE,
    telegram_user_id INTEGER NOT NULL,
    current_conversation_id TEXT,
    updated_at TEXT NOT NULL,
    last_context_message_id INTEGER NOT NULL DEFAULT 0,
    last_invocation_message_id INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(group_id, telegram_user_id)
);

CREATE INDEX IF NOT EXISTS telegram_group_sessions_conversation
ON telegram_group_sessions(current_conversation_id);

CREATE TABLE IF NOT EXISTS telegram_group_resets (
    conversation_id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL REFERENCES telegram_groups(group_id) ON DELETE CASCADE,
    telegram_user_id INTEGER NOT NULL,
    last_context_message_id INTEGER NOT NULL DEFAULT 0,
    through_message_id INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS telegram_group_resets_group
ON telegram_group_resets(group_id, telegram_user_id, created_at);

CREATE TABLE IF NOT EXISTS telegram_storage_migrations (
    name TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL
);
