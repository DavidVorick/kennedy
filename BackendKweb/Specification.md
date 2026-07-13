# Kweb Backend Specification

## 1. Scope

The Kweb backend is a Rust HTTP service that owns all durable Kennedy memory.
It stores the kweb in SQLite, enforces graph and history invariants, exposes a
JSON API to the frontend, and serves the frontend's static files and system
prompt manuals.

It is an independent library backend hosted by the `kennedy-server` binary.
It has no dependency on and receives no state or handles from the intelligence
or conversation history backends.

It does not know about LLM providers, chatends, short identifiers, frontend
sessions, or agent call budgets.

## 2. Runtime Configuration

Configuration values must have code defaults and may be overridden by command
line flags.

| Setting | Flag | Default |
| --- | --- | --- |
| Bind address | `--kweb-bind` | `127.0.0.1:4321` |
| SQLite file | `--kweb-database` | `./kennedy.sqlite3` |
| Frontend directory | `--frontend-dir` | `./Frontend/public` |
| System prompts directory | `--system-prompts-dir` | `./Frontend/SystemPrompts` |
| Active connection limit | `--active-limit` | `12` |
| Fanout connection target | `--fanout-limit` | `60` |

The fanout value is exposed as configuration even though overflow is not
enforced in the MVP.

## 3. SQLite Model

The backend runs schema migrations at startup and configures every SQLite
connection with foreign keys enabled, WAL journaling, and a five-second busy
timeout. All identifiers are generated with the operating system's secure
random source, stored as 20-byte SQLite `BLOB`s, and encoded as 40 lowercase
hexadecimal characters at the API boundary.

### 3.1 Knowledge Nodes

`knowledge_nodes` contains:

| Column | Meaning |
| --- | --- |
| `id` | Primary key, exactly 20 bytes |
| `short_name` | Trimmed text, 4–50 characters |
| `short_description` | Trimmed text, 0–200 characters |
| `long_description` | Text, at most 1000 whitespace-delimited words |
| `history_head_id` | Nullable reference to the newest data history node |
| `is_user_root` | Internal marker for the single MVP user root |

Exactly one knowledge node has `is_user_root = true`.

### 3.2 Data Provenance Nodes

`data_provenance_nodes` contains:

| Column | Meaning |
| --- | --- |
| `id` | Primary key, exactly 20 bytes |
| `data` | Complete source material |
| `source` | Source type or human-readable source description |
| `source_created_at` | RFC 3339 timestamp supplied by the caller |

Provenance rows are immutable. The backend exposes no update or delete API for
them.

### 3.3 Data History Nodes

`data_history_nodes` contains:

| Column | Meaning |
| --- | --- |
| `id` | Primary key, exactly 20 bytes |
| `knowledge_node_id` | Knowledge node whose history contains this entry |
| `previous_history_id` | Previous entry for that knowledge node, or null |
| `provenance_id` | Provenance responsible for the create or update |

History rows are append-only. A knowledge node's `history_head_id` points at
its newest entry; following `previous_history_id` reaches older entries.

### 3.4 Directed Connections

`knowledge_connections` is a normalized implementation of the active and
fanout lists on knowledge nodes. It contains:

| Column | Meaning |
| --- | --- |
| `source_node_id` | Node containing the outgoing connection |
| `target_node_id` | Destination node |
| `tier` | `active` or `fanout` |
| `activation_order` | Monotonically increasing integer used for recency |

The primary key is `(source_node_id, target_node_id)`. Self-connections are
forbidden. A connection exists in only one tier. Connection rows are supporting
structure, not an additional durable node type.

Schema constraints enforce 20-byte IDs, valid connection tiers, non-self
connections, required foreign keys, and a single user root. Foreign-key delete
actions are restrictive; the MVP exposes no deletion path.

## 4. Bootstrap

After migrations, the backend checks for the user root. If none exists, it
creates, in one transaction:

1. a bootstrap provenance node,
2. the minimal `David Vorick` knowledge node,
3. its first history node pointing to the bootstrap provenance node,
4. the knowledge node's history-head reference.

Bootstrap is therefore complete before the HTTP listener begins accepting
requests.

## 5. Graph Rules

`ConnectNodes` receives a set of at least two distinct knowledge-node IDs. For
every ordered pair `(a, b)` where `a != b`, the backend:

1. creates the directed connection if it does not exist,
2. promotes it to active if it is fanout,
3. assigns it the newest activation order.

After promoting the pairs, each affected source node is processed separately.
If it has more active connections than the configured limit, its oldest active
connections are demoted to fanout until it is within the limit. A demotion from
`a` to `b` does not change the connection from `b` to `a`.

Fanout connections are not pruned in the MVP, even when they exceed the
configured target.

## 6. HTTP and Static Files

The backend serves:

- `GET /` and frontend assets from the configured frontend directory,
- `GET /system-prompts/{filename}` from the configured system-prompts
  directory,
- JSON APIs under `/api/v1`.

The system-prompt route serves only the three configured manual files and does
not permit arbitrary filesystem paths.

## 7. JSON Shapes

### 7.1 Connection Summary

```json
{
  "id": "0123456789abcdef0123456789abcdef01234567",
  "short_name": "Example Node",
  "short_description": "Short description."
}
```

### 7.2 Knowledge Node

```json
{
  "id": "0123456789abcdef0123456789abcdef01234567",
  "short_name": "Example Node",
  "short_description": "Short description.",
  "long_description": "Long description.",
  "active_connections": [],
  "fanout_connections": [],
  "history_head_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
```

Connection arrays contain connection summaries in descending activation order.

### 7.3 Error

All API errors use the shared envelope from `TechnicalDesign.md`. The backend
uses these status codes consistently:

| Status | Use |
| --- | --- |
| `400` | Invalid JSON, identifier, field length, or request shape |
| `404` | Referenced node or provenance does not exist |
| `409` | The requested mutation conflicts with a Kweb invariant |
| `500` | Unexpected database or server failure |

## 8. API

### 8.1 Health

#### `GET /health`

```json
{
  "service": "kweb",
  "status": "ok"
}
```

Returns `503` if the database cannot be queried.

### 8.2 User Root

#### `GET /api/v1/user`

```json
{
  "name": "David Vorick",
  "root_node_id": "0123456789abcdef0123456789abcdef01234567"
}
```

### 8.3 Read a Knowledge Node

#### `GET /api/v1/nodes/{node_id}`

Returns one knowledge node in the shape defined above. This endpoint powers the
memory explorer.

### 8.4 Load Context for a Knowledge Node

#### `GET /api/v1/nodes/{node_id}/context`

Returns the requested node and the full knowledge node for each of its current
active connections:

```json
{
  "requested_node": {},
  "active_connection_nodes": []
}
```

All objects use the knowledge-node shape. The requested node appears only in
`requested_node`; duplicate active destinations are impossible.

### 8.5 Create a Provenance Node

#### `POST /api/v1/provenance`

The loopback service accepts provenance request bodies up to 128 MiB so a
complete structured Chatend archive, including future inline media payloads,
can be retained without truncation.

Request:

```json
{
  "data": "{\"format\":\"kennedy-chatend\",\"version\":1,\"messages\":[...]}",
  "source": "conversation",
  "source_created_at": "2026-07-11T00:00:00Z",
  "idempotency_key": "conversation:550e8400-e29b-41d4-a716-446655440000"
}
```

`idempotency_key` is optional and contains 1–200 characters when supplied. The
first request creates the provenance and returns `201`. Repeating the key
returns the original ID with `200` without comparing or rewriting its data.

Response `201`:

```json
{
  "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
```

### 8.6 Read a Provenance Node

#### `GET /api/v1/provenance/{provenance_id}`

```json
{
  "id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "data": "User: ...\nKennedy: ...",
  "source": "conversation",
  "source_created_at": "2026-07-11T00:00:00Z"
}
```

### 8.7 Create a Knowledge Node

#### `POST /api/v1/nodes`

Request:

```json
{
  "provenance_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "parent_node_ids": [
    "0123456789abcdef0123456789abcdef01234567"
  ],
  "short_name": "New Memory",
  "short_description": "Short description.",
  "long_description": "Long description."
}
```

The parent list must be non-empty and contain distinct existing nodes. In one
transaction the backend creates the knowledge node, creates its first history
node, updates its history head, and applies `ConnectNodes` to the new node and
all parents.

Response `201`:

```json
{
  "node": {},
  "history_node_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
}
```

### 8.8 Update a Knowledge Node

#### `PUT /api/v1/nodes/{node_id}`

Request:

```json
{
  "provenance_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "short_name": "Updated Memory",
  "short_description": "Updated description.",
  "long_description": "Updated long description."
}
```

All three mutable text fields are replaced. In one transaction the backend
creates a history node pointing to the supplied provenance and previous history
head, updates the knowledge node, and moves its history head to the new entry.

Response `200` uses the same shape as knowledge-node creation.

### 8.9 Connect Knowledge Nodes

#### `POST /api/v1/connections`

Request:

```json
{
  "node_ids": [
    "0123456789abcdef0123456789abcdef01234567",
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  ]
}
```

The IDs must be distinct and refer to existing nodes. The backend applies the
graph rules in one transaction.

Response:

```json
{
  "nodes": [{}, {}]
}
```

`nodes` contains the affected knowledge nodes after promotion and demotion.

### 8.10 Read Knowledge History

#### `GET /api/v1/nodes/{node_id}/history`

Returns newest first by following the linked list from the current head:

```json
{
  "node_id": "0123456789abcdef0123456789abcdef01234567",
  "history": [
    {
      "id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "previous_history_id": null,
      "provenance_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  ]
}
```

## 9. Transaction Requirements

The following are single SQLite write transactions:

- bootstrap,
- knowledge-node creation and parent connection,
- knowledge-node update and history append,
- connection promotion and demotion,
- provenance creation.

Any failure rolls back the entire operation. Reads never return an intermediate
state from a mutation.

## 10. Implementation Requirements

- Use parameterized SQL for every value.
- Validate all referenced IDs before mutating.
- Preserve connection ordering in every API response.
- Do not log provenance data or full knowledge descriptions.
- Do not follow symlinks or `..` components in static-file routes.
- Keep migrations in source control and apply them in order.
- Include unit tests for validation and graph promotion/demotion, plus
  transaction tests for create and update history behavior.
