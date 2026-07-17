PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS whitelist_entries (
    handle TEXT PRIMARY KEY,
    telegram_user_id INTEGER UNIQUE,
    current_username TEXT,
    display_name TEXT,
    root_node_id TEXT NOT NULL UNIQUE CHECK(length(root_node_id) = 40),
    root_ready INTEGER NOT NULL DEFAULT 0 CHECK(root_ready IN (0, 1)),
    can_add_users INTEGER NOT NULL DEFAULT 0 CHECK(can_add_users IN (0, 1)),
    added_by_telegram_user_id INTEGER,
    whitelisted_at TEXT NOT NULL,
    resolved_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS observed_identities (
    telegram_user_id INTEGER PRIMARY KEY,
    current_username TEXT,
    display_name TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS observed_identities_username
ON observed_identities(current_username);

CREATE TABLE IF NOT EXISTS telegram_groups (
    chat_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    root_node_id TEXT NOT NULL CHECK(length(root_node_id) = 40),
    root_ready INTEGER NOT NULL DEFAULT 0 CHECK(root_ready IN (0, 1)),
    state TEXT NOT NULL DEFAULT 'validating'
        CHECK(state IN ('validating', 'allowed', 'blacklisted')),
    blacklist_reason TEXT,
    blacklisted_at TEXT,
    last_invocation_message_id INTEGER,
    background_cursor_message_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS telegram_group_aliases (
    old_chat_id INTEGER PRIMARY KEY,
    current_chat_id INTEGER NOT NULL REFERENCES telegram_groups(chat_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS telegram_group_members (
    chat_id INTEGER NOT NULL REFERENCES telegram_groups(chat_id) ON DELETE CASCADE,
    telegram_user_id INTEGER NOT NULL,
    username TEXT,
    display_name TEXT NOT NULL,
    membership TEXT NOT NULL CHECK(membership IN ('member', 'administrator', 'creator', 'left', 'kicked')),
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(chat_id, telegram_user_id)
);

CREATE INDEX IF NOT EXISTS telegram_group_members_active
ON telegram_group_members(chat_id, membership, telegram_user_id);

PRAGMA user_version = 2;
