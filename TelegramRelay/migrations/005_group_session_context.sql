CREATE TABLE IF NOT EXISTS telegram_group_session_resets (
    conversation_id TEXT PRIMARY KEY,
    group_root_node_id TEXT NOT NULL CHECK(length(group_root_node_id) = 40),
    telegram_user_id INTEGER NOT NULL,
    last_context_message_id INTEGER NOT NULL DEFAULT 0,
    through_message_id INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS telegram_group_session_resets_group
ON telegram_group_session_resets(group_root_node_id, telegram_user_id, created_at);
