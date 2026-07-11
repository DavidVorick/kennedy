PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS data_provenance_nodes (
    id BLOB PRIMARY KEY CHECK(length(id) = 20),
    data TEXT NOT NULL,
    source TEXT NOT NULL CHECK(length(trim(source)) BETWEEN 1 AND 200),
    source_created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS knowledge_nodes (
    id BLOB PRIMARY KEY CHECK(length(id) = 20),
    short_name TEXT NOT NULL CHECK(length(trim(short_name)) BETWEEN 4 AND 50),
    short_description TEXT NOT NULL CHECK(length(trim(short_description)) <= 200),
    long_description TEXT NOT NULL,
    history_head_id BLOB REFERENCES data_history_nodes(id) ON DELETE RESTRICT,
    is_user_root INTEGER NOT NULL DEFAULT 0 CHECK(is_user_root IN (0, 1))
);

CREATE UNIQUE INDEX IF NOT EXISTS one_user_root
ON knowledge_nodes(is_user_root) WHERE is_user_root = 1;

CREATE TABLE IF NOT EXISTS data_history_nodes (
    id BLOB PRIMARY KEY CHECK(length(id) = 20),
    knowledge_node_id BLOB NOT NULL REFERENCES knowledge_nodes(id) ON DELETE RESTRICT,
    previous_history_id BLOB REFERENCES data_history_nodes(id) ON DELETE RESTRICT,
    provenance_id BLOB NOT NULL REFERENCES data_provenance_nodes(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS knowledge_connections (
    source_node_id BLOB NOT NULL REFERENCES knowledge_nodes(id) ON DELETE RESTRICT,
    target_node_id BLOB NOT NULL REFERENCES knowledge_nodes(id) ON DELETE RESTRICT,
    tier TEXT NOT NULL CHECK(tier IN ('active', 'fanout')),
    activation_order INTEGER NOT NULL,
    PRIMARY KEY(source_node_id, target_node_id),
    CHECK(source_node_id != target_node_id)
);

CREATE INDEX IF NOT EXISTS connection_order
ON knowledge_connections(source_node_id, tier, activation_order DESC);
