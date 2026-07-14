CREATE TABLE IF NOT EXISTS kmap_roots (
    role TEXT PRIMARY KEY CHECK(role IN ('user', 'kennedy')),
    knowledge_node_id BLOB NOT NULL UNIQUE REFERENCES knowledge_nodes(id) ON DELETE RESTRICT
);
