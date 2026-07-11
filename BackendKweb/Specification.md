# Kweb Rust Backend Specification

## 1. Scope

The kweb backend is a Rust HTTP service that owns the SQLite database, serves the static frontend, and exposes durable kweb APIs to the frontend. It stores knowledge nodes, data provenance nodes, and data history nodes. It implements kweb graph mutation rules, but it does not manage UI sessions, LLM conversations, context windows, short identifiers, or chat transcripts.

## 2. Responsibilities

- Store all kweb data in SQLite.
- Create and migrate the SQLite schema.
- Bootstrap the hardcoded user node for David Vorick when no history exists.
- Provide atomic knowledge-node mutations.
- Preserve immutable data provenance records.
- Preserve append-only data history linked lists.
- Apply active/fanout connection rules when nodes are connected.
- Provide APIs for frontend memory exploration and frontend-orchestrated agent tools.

## 3. Runtime Configuration

The backend must accept configuration from CLI flags and environment variables. CLI flags take precedence.

| Setting | CLI flag | Environment | Default |
| --- | --- | --- | --- |
| Bind host | `--host` | `KWEB_HOST` | `127.0.0.1` |
| Bind port | `--port` | `KWEB_PORT` | `4321` |
| SQLite path | `--database` | `KWEB_DATABASE` | `./kennedy.sqlite3` |
| Static frontend dir | `--static-dir` | `KWEB_STATIC_DIR` | `./Frontend/public` |
| Active connection limit | `--active-connection-limit` | `KWEB_ACTIVE_CONNECTION_LIMIT` | `12` |
| Fanout connection limit | `--fanout-connection-limit` | `KWEB_FANOUT_CONNECTION_LIMIT` | `60` |

Hardcoded numbers from the user specification must be constants or config-derived values, not scattered literals.

## 4. SQLite Data Model

### 4.1 Encoding Rules

- Durable node identifiers are 20 random bytes stored as `BLOB` and exposed as lowercase 40-character hex strings.
- JSON arrays may be used for ordered connection lists in v1, but updates must be transactional.
- Text is UTF-8.
- Timestamps are UTC RFC 3339 in API responses.

### 4.2 Tables

#### `knowledge_nodes`

| Column | Type | Constraints | Description |
| --- | --- | --- | --- |
| `id` | `BLOB` | primary key, 20 bytes | Durable knowledge-node ID |
| `short_name` | `TEXT` | not null, length 4-50 | Human-readable compact name |
| `short_description` | `TEXT` | not null, length 0-200 | One-line description |
| `long_description` | `TEXT` | not null, max 1000 words | Detailed memory summary |
| `active_connections` | `TEXT` | not null JSON array | Ordered list of connected knowledge-node IDs |
| `fanout_connections` | `TEXT` | not null JSON array | Ordered list of fanout knowledge-node IDs |
| `history_head_id` | `BLOB` | nullable FK to `data_history_nodes.id` | Latest data-history entry |
| `created_at` | `TEXT` | not null | Creation timestamp |
| `updated_at` | `TEXT` | not null | Last mutation timestamp |

Connection order is most-recently-active first. Active connections are capped at 12 by default. Fanout connections should be capped at 60 by design, but v1 may temporarily exceed this limit to match the user specification.

#### `data_provenance_nodes`

| Column | Type | Constraints | Description |
| --- | --- | --- | --- |
| `id` | `BLOB` | primary key, 20 bytes | Durable provenance ID |
| `source_type` | `TEXT` | not null | Source category, such as `conversation`, `manual`, `email`, `telegram`, `meeting_recording` |
| `source_ref` | `TEXT` | nullable | External source locator or human label |
| `data` | `TEXT` | not null | Raw source data |
| `data_sha256` | `TEXT` | not null | SHA-256 hex digest of `data` |
| `created_at_source` | `TEXT` | not null | When the source data was created |
| `ingested_at` | `TEXT` | not null | When the backend ingested the source |
| `metadata_json` | `TEXT` | not null JSON object | Additional source metadata |

Data provenance rows are immutable after insertion.

#### `data_history_nodes`

| Column | Type | Constraints | Description |
| --- | --- | --- | --- |
| `id` | `BLOB` | primary key, 20 bytes | Durable history ID |
| `knowledge_node_id` | `BLOB` | not null FK | Knowledge node whose history this belongs to |
| `previous_history_id` | `BLOB` | nullable FK to same table | Previous linked-list entry |
| `provenance_id` | `BLOB` | not null FK | Source provenance for this update |
| `change_summary` | `TEXT` | not null | Short summary of the update |
| `snapshot_json` | `TEXT` | not null JSON object | Knowledge-node fields after the update |
| `created_at` | `TEXT` | not null | History-entry timestamp |

Data history rows are append-only. Updating a knowledge node creates a new history row and points `knowledge_nodes.history_head_id` at it in the same transaction.

## 5. Validation Rules

- `short_name`: 4-50 Unicode scalar values after trimming leading/trailing whitespace.
- `short_description`: 0-200 Unicode scalar values after trimming.
- `long_description`: at most 1000 words, using whitespace tokenization for v1.
- Knowledge IDs in connection lists must refer to existing knowledge nodes.
- A node must not list itself as an active or fanout connection.
- Duplicate connection IDs are removed while preserving first occurrence.
- `CreateNode` and `UpdateNode` require a provenance ID supplied by the frontend.
- The backend accepts only durable node IDs. Any short IDs used in prompts or UI state are translated by the frontend before API calls.

## 6. API Reference

All endpoints are relative to the kweb backend base URL.

### 6.1 Health

#### `GET /health`

Returns service health.

Response `200`:

```json
{
  "service": "kweb-backend",
  "status": "ok",
  "database": "ok",
  "version": "0.1.0"
}
```

### 6.2 Static Frontend

#### `GET /`

Serves the frontend `index.html`.

#### `GET /assets/*`

Serves static CSS, JavaScript, prompt text files, and image assets.

### 6.3 Bootstrap and User

#### `POST /api/bootstrap`

Ensures the initial David Vorick root node exists. Safe to call multiple times.

Request:

```json
{}
```

Response `200`:

```json
{
  "user": {
    "name": "David Vorick",
    "root_node_id": "0123456789abcdef0123456789abcdef01234567"
  },
  "created": true
}
```

#### `GET /api/user`

Returns the current hardcoded user.

Response `200`:

```json
{
  "name": "David Vorick",
  "root_node_id": "0123456789abcdef0123456789abcdef01234567"
}
```

### 6.4 Knowledge Nodes

#### `GET /api/nodes/{node_id}`

Returns durable node details for frontend context loading and memory explorer views.

Response `200`:

```json
{
  "id": "0123456789abcdef0123456789abcdef01234567",
  "short_name": "David Vorick",
  "short_description": "Root user node.",
  "long_description": "Minimal bootstrap information.",
  "active_connections": [
    {
      "id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "short_name": "Example Memory",
      "short_description": "A concise summary."
    }
  ],
  "fanout_connections": [],
  "history_head_id": "cccccccccccccccccccccccccccccccccccccccc",
  "created_at": "2026-07-11T00:00:00Z",
  "updated_at": "2026-07-11T00:00:00Z"
}
```

#### `POST /api/nodes/load`

Returns a node plus its active connections. This is the backend primitive the frontend uses to implement the agent-facing `LoadNode` tool.

Request:

```json
{
  "node_id": "0123456789abcdef0123456789abcdef01234567"
}
```

Response `200`:

```json
{
  "requested_node": {
    "id": "0123456789abcdef0123456789abcdef01234567",
    "short_name": "David Vorick",
    "short_description": "Root user node.",
    "long_description": "Minimal bootstrap information.",
    "active_connections": [],
    "fanout_connections": []
  },
  "active_connection_nodes": []
}
```

#### `POST /api/nodes`

Creates a knowledge node using an existing provenance record. This is the backend primitive the frontend uses to implement the agent-facing `CreateNode` tool during history ingress.

Request:

```json
{
  "provenance_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "parent_node_ids": ["0123456789abcdef0123456789abcdef01234567"],
  "short_name": "New Memory",
  "short_description": "A concise summary.",
  "long_description": "Detailed description.",
  "change_summary": "Created from ingress source."
}
```

Response `201`:

```json
{
  "node": {
    "id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "short_name": "New Memory",
    "short_description": "A concise summary.",
    "long_description": "Detailed description.",
    "active_connections": [],
    "fanout_connections": []
  },
  "history_id": "cccccccccccccccccccccccccccccccccccccccc"
}
```

The backend creates the knowledge node, creates its first data-history node, and connects the new node with its parent nodes within one transaction.

#### `PATCH /api/nodes/{node_id}`

Updates mutable fields for a knowledge node using an existing provenance record. This is the backend primitive the frontend uses to implement the agent-facing `UpdateNode` tool during history ingress.

Request:

```json
{
  "provenance_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "new_short_name": "Updated Memory",
  "new_short_description": "Updated concise summary.",
  "new_long_description": "Updated detailed description.",
  "change_summary": "Updated from ingress source."
}
```

Response `200`:

```json
{
  "node": {
    "id": "dddddddddddddddddddddddddddddddddddddddd",
    "short_name": "Updated Memory",
    "short_description": "Updated concise summary.",
    "long_description": "Updated detailed description.",
    "active_connections": [],
    "fanout_connections": []
  },
  "history_id": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
}
```

### 6.5 Connections

#### `POST /api/connections/connect-nodes`

Connects a group of knowledge nodes according to kweb active/fanout rules. This is the backend primitive the frontend uses to implement the agent-facing `ConnectNodes` tool.

Request:

```json
{
  "node_ids": [
    "0123456789abcdef0123456789abcdef01234567",
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  ]
}
```

Response `200`:

```json
{
  "updated_nodes": [
    {
      "id": "0123456789abcdef0123456789abcdef01234567",
      "active_connections": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
      "demoted_to_fanout": []
    }
  ]
}
```

For each ordered pair in the provided set, the backend promotes the other nodes into the node's active connections. If active connections exceed the configured limit, least-recently-active entries are demoted to fanout connections. Demotions are one-way and need not be mirrored.

### 6.6 Provenance

#### `POST /api/provenance`

Creates an immutable provenance node. The frontend calls this when a conversation or other source is ready for history ingress.

Request:

```json
{
  "source_type": "conversation",
  "source_ref": "frontend-conversation-id",
  "data": "User: ...\nKennedy: ...",
  "created_at_source": "2026-07-11T00:00:00Z",
  "metadata": {}
}
```

Response `201`:

```json
{
  "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "data_sha256": "sha256hex",
  "ingested_at": "2026-07-11T00:00:00Z"
}
```

#### `GET /api/provenance/{provenance_id}`

Returns provenance details.

Response `200`:

```json
{
  "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "source_type": "conversation",
  "source_ref": "frontend-conversation-id",
  "data": "User: ...\nKennedy: ...",
  "data_sha256": "sha256hex",
  "created_at_source": "2026-07-11T00:00:00Z",
  "ingested_at": "2026-07-11T00:00:00Z",
  "metadata": {}
}
```

### 6.7 History

#### `GET /api/nodes/{node_id}/history`

Returns history entries newest first.

Response `200`:

```json
{
  "node_id": "0123456789abcdef0123456789abcdef01234567",
  "history": [
    {
      "history_id": "cccccccccccccccccccccccccccccccccccccccc",
      "previous_history_id": null,
      "provenance_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "change_summary": "Initial bootstrap node.",
      "created_at": "2026-07-11T00:00:00Z"
    }
  ]
}
```

## 7. Transaction Semantics

Each mutation API call is atomic. If any validation, database write, or invariant update fails, the backend rolls back the entire call and returns an error envelope.

`POST /api/nodes`, `PATCH /api/nodes/{node_id}`, `POST /api/connections/connect-nodes`, and `POST /api/provenance` must use write transactions.

## 8. Concurrency

- Allow concurrent reads for memory explorer and context-loading endpoints.
- Serialize writes that affect the same node's connection lists or history head.
- Knowledge-node write conflicts must return `409` or retry internally with a bounded retry policy.
- The backend must never expose partially updated connection lists.

## 9. Observability

The backend should log:

- service startup configuration excluding secrets,
- migrations applied,
- provenance creation,
- node creation/update calls,
- connection updates,
- validation failures,
- database errors.

Logs must not include full provenance data by default because conversations may contain private information.
