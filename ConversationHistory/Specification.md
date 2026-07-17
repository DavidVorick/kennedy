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
- phase: `active`, `ingress_pending`, `ingress_in_progress`, `ingress_failed`,
  or `complete`,
- conversation start, last-update, last-user-message, and optional end times,
- opaque JSON frontend state,
- optional opaque Kweb provenance ID,
- monotonically increasing version for optimistic concurrency.
- durable history-ingress failure count and a concise JSON log of all attempts
  (at most five), including timestamp, stage, code/message, model-round count,
  and measured context occupancy when available,
- optional next-attempt time for a deferred automatic ingress retry.

SQLite permits any number of ordinary `active` and `ingress_pending` records
but at most one `ingress_in_progress` record. The create path also atomically
rejects a second active `free-time` record with
`free_time_already_active`; this server-side guard covers separate browser
contexts in addition to the frontend's Web Lock. Completed and failed records
remain available as conversation history. A migration removes the older
one-unfinished-record index; legacy rows receive null activity/end times and
remain valid.

A current startup also drops the obsolete singleton index idempotently even
when `user_version` is already 2. This repairs databases touched by an early v2
build that could recreate the v1 index after recording the v2 migration.

The frontend state contains recovery metadata and versioned, opaque JSON
archives for both the complete conversation Chatend and its history-ingress
Chatend. It may include the direct root IDs, referenced group-participant root
IDs, and dynamic channel/group context; these remain opaque to this backend.
The backend interprets `pendingTurn` and the top-level/archive `sessionType`
when deciding whether an idle conversation is safe to close automatically.
All session types beginning with `telegram`, plus autonomous `free-time`
sessions, are protected from idle closure and unstarted-record cleanup.
Private Telegram sessions and persistent `(group root, Telegram user)` sessions
close through `/reset`; background group batches are explicitly queued by the
frontend as soon as their independent archive is ready. For legacy safety during
unstarted-record cleanup, it also recognizes user-role entries in the stored
conversation transcript before deleting a record whose activity timestamp is
null.

## 3. State Machine and Queue

```text
active -> ingress_pending -> ingress_in_progress -> complete
             |               |
             +---------------+-> ingress_failed (fifth failure)
                                      |
                                      +-> ingress_pending (explicit retry)
```

- Checkpoints may update only an `active` conversation.
- A checkpoint marked `user_activity` records the server's current time as the
  last user-message time. In the same transaction, other active conversations
  idle for more than 24 hours become `ingress_pending`, except records whose
  opaque state says Kennedy still owes a response (`pendingTurn: true`).
  Records whose state identifies any Telegram or free-time session are also
  exempt.
- Explicitly ending a conversation checkpoints its final state and changes
  `active` to `ingress_pending` immediately.
- The oldest eligible queued conversation is selected by last user activity,
  falling back to its start time. An actively claimed `ingress_in_progress`
  record wins; deferred records are skipped until their next-attempt time.
- The frontend creates or retrieves idempotent Kweb provenance, records its ID,
  and changes the selected record to `ingress_in_progress`.
- Only successful history ingress changes the record to `complete`.
- The frontend records a failed ingress attempt atomically. Attempts one
  through four return the record to `ingress_pending`, release the
  single-worker claim, and defer that record for 15 seconds so other eligible
  conversations or audio pieces can proceed. Attempt five changes it to
  terminal `ingress_failed` and excludes it from future queue selection.
  A provider input-size rejection is terminal on its first attempt because the
  unchanged checkpoint cannot make it smaller. Failed records remain queryable
  with their diagnostic logs.
- New and existing active conversations are independent of this queue.
- A terminal failed record can be explicitly retried. The frontend supplies a
  fresh opaque state with the failed history-ingress checkpoint removed, the
  backend resets the consecutive-attempt count, preserves the diagnostic log
  and provenance ID, and returns the record to `ingress_pending`.
- Any record can be explicitly purged with its expected version. Purge deletes
  the row permanently without moving it through `ingress_pending`, so an active
  or queued conversation cannot later be selected for history ingress. It is a
  destructive escape hatch for stuck sessions, not a state-machine phase.
- On frontend startup, records that have neither recorded user activity nor a
  user message in their stored conversation transcript are permanently
  discarded, regardless of phase. This also removes an untouched placeholder
  that was ended or processed without ever becoming a real conversation.
  Every record containing a user message is ineligible. Telegram records,
  including new group sessions/background batches, are also ineligible
  because they can be created and bound or queued just before their first
  durable checkpoint.

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
- `DELETE /api/v1/conversations/{id}`
- `PUT /api/v1/conversations/{id}/checkpoint`
- `POST /api/v1/conversations/{id}/request-ingress`
- `POST /api/v1/conversations/{id}/ingress-started`
- `PUT /api/v1/conversations/{id}/ingress-checkpoint`
- `POST /api/v1/conversations/{id}/ingress-completed`
- `POST /api/v1/conversations/{id}/ingress-failure`
- `POST /api/v1/conversations/{id}/retry-ingress`

Create accepts `started_at` plus opaque `state`. Checkpoint accepts
`expected_version`, `state`, and optional `user_activity`. The ingress queue
endpoint returns `{ "conversation": null }` when empty. Successful
state-machine mutations return the complete updated record. Purge returns the
deleted ID. Unstarted cleanup is idempotent and returns the count and IDs of
discarded records.
The failure endpoint accepts `expected_version`, stage, optional error code,
message, round count, and optional context usage. It normalizes and bounds
diagnostic text before atomically incrementing the attempt count.
Retry accepts `expected_version` plus replacement opaque `state`.
Purge accepts `expected_version`, deletes the complete conversation record in
any phase, and returns its ID. A stale expected version returns
`409 state_conflict` rather than deleting newer work.

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
checkpoints, plus deferred retry claim release, terminal fifth-failure
behavior, and queue advancement.
