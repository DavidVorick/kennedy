CREATE TABLE IF NOT EXISTS telegram_group_messages (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    update_id INTEGER NOT NULL,
    telegram_user_id INTEGER,
    username TEXT,
    display_name TEXT NOT NULL,
    text TEXT NOT NULL,
    reply_to_message_id INTEGER,
    sent_by_kennedy INTEGER NOT NULL DEFAULT 0 CHECK(sent_by_kennedy IN (0, 1)),
    created_at TEXT NOT NULL,
    PRIMARY KEY(chat_id, message_id)
);

CREATE INDEX IF NOT EXISTS telegram_group_messages_order
ON telegram_group_messages(chat_id, message_id);

CREATE TABLE IF NOT EXISTS telegram_group_ingress (
    id TEXT PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    first_message_id INTEGER NOT NULL,
    last_message_id INTEGER NOT NULL,
    messages_json TEXT NOT NULL,
    participants_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'processing', 'complete')),
    created_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE(chat_id, first_message_id, last_message_id)
);

CREATE INDEX IF NOT EXISTS telegram_group_ingress_queue
ON telegram_group_ingress(status, created_at);
