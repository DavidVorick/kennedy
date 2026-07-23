CREATE TABLE IF NOT EXISTS whitelist_entries (
    handle TEXT PRIMARY KEY,
    telegram_user_id INTEGER UNIQUE,
    current_username TEXT,
    display_name TEXT,
    root_node_id TEXT UNIQUE CHECK(root_node_id IS NULL OR length(root_node_id) = 8),
    root_ready INTEGER NOT NULL DEFAULT 0 CHECK(root_ready IN (0, 1)),
    can_add_users INTEGER NOT NULL DEFAULT 0 CHECK(can_add_users IN (0, 1)),
    added_by_telegram_user_id INTEGER,
    whitelisted_at TEXT NOT NULL,
    resolved_at TEXT,
    updated_at TEXT NOT NULL,
    CHECK(root_ready = 0 OR root_node_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS whitelist_entries_username
ON whitelist_entries(current_username);

CREATE TABLE IF NOT EXISTS observed_identities (
    telegram_user_id INTEGER PRIMARY KEY,
    current_username TEXT,
    display_name TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS observed_identities_username
ON observed_identities(current_username);

CREATE TABLE IF NOT EXISTS telegram_group_roots (
    group_id TEXT PRIMARY KEY,
    root_node_id TEXT CHECK(root_node_id IS NULL OR length(root_node_id) = 8),
    root_ready INTEGER NOT NULL DEFAULT 0 CHECK(root_ready IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(root_node_id),
    CHECK(root_ready = 0 OR root_node_id IS NOT NULL)
);

-- Telegram uses this transport-only pseudo-user for anonymous group authorship.
-- It must never become a Kennedy identity or whitelist candidate.
DELETE FROM observed_identities
WHERE telegram_user_id = 1087968824
   OR lower(COALESCE(current_username, '')) = 'groupanonymousbot';
