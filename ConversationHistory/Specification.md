# Conversation History Backend Specification

## 1. Scope

The conversation history backend is a logically independent Rust API that owns
durable browser and Telegram conversation checkpoints and the queue of conversations that
must undergo history ingress. It shares a deployment binary with the Kweb and
intelligence backends but has its own listener, router, state, SQLite database,
and crate. It imports neither backend and calls neither backend.

## 2. Data Model

Each conversation record contains:

- UUID conversation ID,
- phase: `active`, `ingress_pending`, `ingress_in_progress`, or `complete`,
- conversation start, last-update, last-user-message, and optional end times,
- opaque JSON frontend state,
- optional opaque Kweb provenance ID,
- monotonically increasing version for optimistic concurrency.

SQLite permits any number of `active` and `ingress_pending` records but at most
one `ingress_in_progress` record. Completed records remain available as
conversation history. A migration removes the older one-unfinished-record
index; legacy rows receive null activity/end times and remain valid.

A current startup also drops the obsolete singleton index idempotently even
when `user_version` is already 2. This repairs databases touched by an early v2
build that could recreate the v1 index after recording the v2 migration.

The frontend state contains recovery metadata and versioned, opaque JSON
archives for both the complete conversation Chatend and its history-ingress
Chatend. The backend interprets `pendingTurn` and the top-level/archive
`sessionType` when deciding whether an idle conversation is safe to close
automatically. Telegram sessions never idle-close; `/reset` explicitly closes
them. For legacy safety during
unstarted-record cleanup, it also recognizes user-role entries in the stored
conversation transcript before deleting a record whose activity timestamp is
null.

## 3. State Machine and Queue

```text
active -> ingress_pending -> ingress_in_progress -> complete
```

- Checkpoints may update only an `active` conversation.
- A checkpoint marked `user_activity` records the server's current time as the
  last user-message time. In the same transaction, other active conversations
  idle for more than 24 hours become `ingress_pending`, except records whose
  opaque state says Kennedy still owes a response (`pendingTurn: true`).
  Records whose state identifies a Telegram session are also exempt.
- Explicitly ending a conversation checkpoints its final state and changes
  `active` to `ingress_pending` immediately.
- The oldest queued conversation is selected by last user activity, falling
  back to its start time. An existing `ingress_in_progress` record always wins.
- The frontend creates or retrieves idempotent Kweb provenance, records its ID,
  and changes the selected record to `ingress_in_progress`.
- Only successful history ingress changes the record to `complete`.
- New and existing active conversations are independent of this queue.
- On frontend startup, records that have neither recorded user activity nor a
  user message in their stored conversation transcript are permanently
  discarded, regardless of phase. This also removes an untouched placeholder
  that was ended or processed without ever becoming a real conversation.
  Every record containing a user message is ineligible. Telegram records are
  also ineligible because they can be created and bound to a relay event just
  before the first durable user-message checkpoint.

Every mutation supplies `expected_version`. Stale browser tabs receive
`409 state_conflict` rather than overwriting newer state. The unique in-progress
index serializes memory updates even when several conversations close together.

## 4. API

- `GET /health`
- `GET|POST /api/v1/conversations`
- `GET /api/v1/conversations/current` (most recently updated active record,
  retained as a compatibility convenience)
- `GET /api/v1/conversations/ingress/next`
- `DELETE /api/v1/conversations/unstarted`
- `GET /api/v1/conversations/{id}`
- `PUT /api/v1/conversations/{id}/checkpoint`
- `POST /api/v1/conversations/{id}/request-ingress`
- `POST /api/v1/conversations/{id}/ingress-started`
- `PUT /api/v1/conversations/{id}/ingress-checkpoint`
- `POST /api/v1/conversations/{id}/ingress-completed`

Create accepts `started_at` plus opaque `state`. Checkpoint accepts
`expected_version`, `state`, and optional `user_activity`. The ingress queue
endpoint returns `{ "conversation": null }` when empty. All successful
record mutations return the complete updated record. Unstarted cleanup is
idempotent and returns the count and IDs of discarded records.

## 5. Deployment and Isolation

The default listener is `127.0.0.1:4323` and the default database is
`kennedy-conversations.sqlite3`. SQLite uses WAL mode and a busy timeout, so
Kmap reads can continue while the separate Kweb database is being updated.
Only the frontend origin is allowed by CORS. The server accepts request bodies
up to 128 MiB for structured Chatend archives and future inline media.

## 6. Verification

Tests cover optimistic-version conflicts, multiple active conversations,
repair of a v2 database containing the legacy singleton index, the
single-ingress-worker invariant, 24-hour expiry plus pending-response and
Telegram-session protection,
structured archives, safe unstarted-record cleanup, and phase-restricted ingress
checkpoints.
