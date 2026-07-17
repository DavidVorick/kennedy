ALTER TABLE knowledge_nodes
ADD COLUMN owner_root_node_id BLOB
    REFERENCES knowledge_nodes(id) ON DELETE RESTRICT
    CHECK(owner_root_node_id IS NULL OR length(owner_root_node_id) = 20);

CREATE INDEX IF NOT EXISTS knowledge_nodes_by_owner
ON knowledge_nodes(owner_root_node_id);
