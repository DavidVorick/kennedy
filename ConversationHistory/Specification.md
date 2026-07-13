# Conversation History Backend Specification

## 1. Scope

The conversation history backend is a logically independent Rust API that owns
durable browser conversation checkpoints and the obligation to complete
history ingress before another conversation begins. It shares a deployment
binary with the Kweb and intelligence backends but has its own listener, router,
state, SQLite database, and crate. It imports neither backend and calls neither
backend.

## 2. Data Model

Each conversation record contains:

- UUID conversation ID,
- phase: `active`, `ingress_pending`, `ingress_in_progress`, or `complete`,
- conversation start and last-update timestamps,
- opaque JSON frontend state,
- optional opaque Kweb provenance ID,
- monotonically increasing version for optimistic concurrency.

SQLite permits at most one record whose phase is not `complete`. Completed
records remain available as conversation history.

The frontend state currently contains the clean transcript, start timestamp,
directly loaded durable Kweb IDs, and whether a user query still needs a model
response. The backend does not interpret those fields.

## 3. State Machine

```text
active -> ingress_pending -> ingress_in_progress -> complete
```

- Checkpoints may update only an `active` conversation.
- Requesting a new conversation atomically checkpoints the final state and
  changes `active` to `ingress_pending`.
- The frontend creates or retrieves an idempotent Kweb provenance node, then
  records its opaque ID while changing the phase to `ingress_in_progress`.
- Only successful history ingress changes the record to `complete`.
- A new conversation cannot be created while any prior record is unfinished.
- Startup retrieves the unfinished record. Active work is restored; pending or
  in-progress ingress is retried before a new durable conversation is created.
  The frontend may display an unpersisted draft composer during that retry.

Every mutation supplies `expected_version`. Stale browser tabs receive
`409 state_conflict` rather than overwriting newer state. Starting and
completing ingress are idempotent when the requested terminal state has already
been reached.

## 4. API

- `GET /health`
- `GET /api/v1/conversations`
- `POST /api/v1/conversations`
- `GET /api/v1/conversations/current`
- `GET /api/v1/conversations/{id}`
- `PUT /api/v1/conversations/{id}/checkpoint`
- `POST /api/v1/conversations/{id}/request-ingress`
- `POST /api/v1/conversations/{id}/ingress-started`
- `POST /api/v1/conversations/{id}/ingress-completed`

`current` returns `{ "conversation": null }` when no unfinished record exists.
The list endpoint returns every durable record, including its opaque state, so
the frontend can render conversation history; the record endpoint retrieves one
complete record and its full saved transcript state.
Create accepts `started_at` plus opaque `state`. Checkpoint and
`request-ingress` accept `expected_version` plus `state`. `ingress-started`
accepts `expected_version` plus `provenance_id`; `ingress-completed` accepts
`expected_version`.

All successful mutations return the complete updated record.

## 5. Deployment and Isolation

The default listener is `127.0.0.1:4323` and the default database is
`kennedy-conversations.sqlite3`. Only the frontend origin is allowed by CORS.
The single `kennedy-server` executable starts this service alongside the other
backend libraries, but the top-level binary passes no backend state or handles
between them.

## 6. Verification

Tests cover the phase transition, optimistic-version conflicts, and the
one-unfinished-conversation database invariant. Frontend tests verify that a
pending query is checkpointed before generation and can resume after restore.
