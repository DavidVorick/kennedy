CREATE TABLE IF NOT EXISTS telegram_polling_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    next_update_id INTEGER NOT NULL CHECK(next_update_id >= 0),
    updated_at TEXT NOT NULL
);
