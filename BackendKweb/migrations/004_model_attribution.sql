CREATE TABLE IF NOT EXISTS knowledge_node_model_attribution (
    knowledge_node_id BLOB PRIMARY KEY REFERENCES knowledge_nodes(id) ON DELETE RESTRICT,
    last_modified_by TEXT NOT NULL CHECK(length(trim(last_modified_by)) BETWEEN 1 AND 200)
);

INSERT OR IGNORE INTO knowledge_node_model_attribution(knowledge_node_id,last_modified_by)
SELECT id,'legacy-unknown' FROM knowledge_nodes;
