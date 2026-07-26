# kcode Session History Specification

## Purpose

`kcode-session-history` is Kennedy's local session-history library. It owns
active session history, lifecycle and command records, pending session objects,
and the completed-session index. The package and Rust crate are named
`kcode-session-history` and `kcode_session_history`; the old
ConversationHistory name is retired. The runtime does not use a conversation
SQLite database.

The library exposes two primary handles. `SessionHistory` is the long-lived
store and catalog. `Session` is an opaque handle to one active session. A
caller creates a `Session` with a library-assigned ID or reopens one through
`SessionHistory` using typed metadata; storage paths never cross the library
boundary. There is no `SessionLease`.

An in-progress session has one authoritative append-only `.session-log` file
plus a separate `.session-control` file for lifecycle and browser commands.
After the session and its history-ingress phase commit, local Session History
retains a structured completion receipt.

## Storage

The defaults are:

- in-progress transcripts: `data/sessions/in-progress/{uuid}.session-log`;
- in-progress control state:
  `data/sessions/in-progress/{uuid}.session-control`;
- pending objects:
  `data/sessions/in-progress/{uuid}-{event-position}.pending-object`;
- completed ID list: `data/session-history.txt`.

New completed-list entries are JSON-lines receipts containing the transaction
ID when available, canonical archive object ID, pending-node mappings, and
pending-object mappings, in commit order. Historical one-object-ID lines
remain readable.
Appending a receipt already represented by its archive ID is a successful
no-op. Every append is flushed and synchronized. Clients load the immutable
archive from Kweb when needed.

The `.session-log` format and its recovery rules are owned by
`kcode-session-log`.
Session History writes only lifecycle and command records to
`.session-control`; it does not add storage-specific sidebands to the
transcript. Transcript entries, box events, tool events, and pending objects
come from `kcode-session-log`. The library rebuilds Chatend from that ordered
log, and the browser projection falls back to the same events rather than
requiring a durable presentation snapshot. Successful completion deletes the
control file.

At startup, Session History compacts active control journals. It retains the
latest lifecycle record and the latest state of every command, removes
superseded records and any historical presentation snapshots, writes the
replacement atomically, and leaves `.session-log` and pending-object files
unchanged.

## Lifecycle

The lifecycle projection uses these phases:

```text
active -> ingress_pending -> ingress_in_progress -> complete
             ^                    |  \
             |                    |   -> ingress_failed
             +--------------------+

ingress_failed --manual retry--> ingress_pending
```

An `active` session accepts ordered commands and staged browser objects.
Ending the source session changes it to `ingress_pending`. The single global
Kweb writer lane claims it as `ingress_in_progress`. Source conversation and
history ingress remain parts of the same session log. A successful Kweb commit
supplies a completion receipt; Session History synchronizes that receipt and
then removes the session log, pending objects, and control file.

Retryable failures return the record to `ingress_pending`, release the writer
lane, and set a roughly 15-second `ingress_next_attempt_at`. The scheduler
chooses claimed work first and otherwise the oldest due work across conversation
and audio ingress. The fifth failed attempt, or an explicitly non-retryable
input error, moves the record to terminal `ingress_failed`; manual retry grants
a fresh attempt budget from the preserved checkpoint. The total consecutive
failure count is retained while only the five latest concise diagnostics are
embedded in lifecycle records. Startup requeues a process-interrupted
`ingress_in_progress` record without discarding its checkpoint.

If history ingress reaches its full context ceiling, Kennedy commits the work
completed so far instead of reserving another output margin.

There is no purge operation for the new model. An in-progress session must be
ended and committed. A completed session is immutable Kweb data.

## Commands

Browser commands are sideband records with:

- UUID command ID;
- session ID and monotonic sequence;
- kind and JSON payload;
- `pending`, `processing`, or `complete` status;
- cancellation state, outcome, timestamps, and idempotency ID.

Only the earliest unfinished command for a session is claimable. Different
read-only sessions may run concurrently. Object upload is rejected while a
command is pending or processing so a second journal handle cannot race the
session controller's shared temporary-ID allocator.

Start and command idempotency IDs are validated. Replaying the same ID returns
the existing record.

## KennedyServer HTTP adapter

The library has no Axum dependency and exposes no routes, multipart types,
HTTP statuses, or response bodies. KennedyServer adapts the typed library API
to the browser's compatibility routes. The live route prefix remains
`/api/v1/conversations`; that transport name does not change the Session
History domain:

- `GET /api/v1/conversations/health`
- `GET /api/v1/conversations/summaries`
- `GET /api/v1/conversations/{id}`
- `POST /api/v1/conversations/start`
- `GET /api/v1/conversation-commands`
- `POST /api/v1/conversations/{id}/commands`
- `POST /api/v1/conversations/{id}/stop`
- `POST /api/v1/conversations/{id}/objects`
- `GET /api/v1/conversations/{id}/objects/{pending_id}`
- `POST /api/v1/conversations/{id}/retry-ingress`

Completed summary records intentionally contain only permanent identifiers and
their structured commit receipt, not a second transcript copy. The frontend
follows the archive ID through
`GET /api/v1/session-history/{object_id}` to classify and display the archive.

KennedyServer's object POST accepts exactly one multipart `file` and gives its
owned bytes and metadata to the library. The library writes one durable
pending-object file, appends the corresponding event, and returns the
event-position-derived temporary `pending:N` ID. KennedyServer's object GET
adds transport headers to the verified bytes and metadata returned by the
library.

## Isolation and cutover

Session History owns no graph policy, HTTP policy, or Kweb transaction commit.
KennedyServer's orchestration worker owns those actions. KennedyServer depends
on the typed API and does not depend directly on `kcode-session-log`, open
session storage paths, or send path-shaped JSON calls into the library. The old
conversation SQLite database is an offline archive and has no runtime loader.

At the 2026-07-23 cutover, all unfinished legacy sessions were exported one per
text file, the full legacy conversation database was moved under
`data/archive/`, and legacy conversation rows were copied out of the still-live
mixed memory-ingress database before deletion.

## Verification

Tests cover coordinated transcript/control creation, durable commands,
structured completion receipts, pending objects, control-journal compaction,
immutable completed-history behavior, and the typed non-HTTP boundary.
