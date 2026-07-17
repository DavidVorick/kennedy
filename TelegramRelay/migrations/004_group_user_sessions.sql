CREATE TABLE IF NOT EXISTS telegram_group_user_sessions (
    group_root_node_id TEXT NOT NULL CHECK(length(group_root_node_id) = 40),
    telegram_user_id INTEGER NOT NULL,
    current_conversation_id TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(group_root_node_id, telegram_user_id)
);

CREATE INDEX IF NOT EXISTS telegram_group_user_sessions_conversation
ON telegram_group_user_sessions(current_conversation_id);
