CREATE TABLE IF NOT EXISTS provenance_idempotency (
    idempotency_key TEXT PRIMARY KEY NOT NULL CHECK(length(trim(idempotency_key)) BETWEEN 1 AND 200),
    provenance_id BLOB NOT NULL UNIQUE REFERENCES data_provenance_nodes(id) ON DELETE RESTRICT
);
