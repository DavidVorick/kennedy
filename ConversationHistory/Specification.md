# Session History Specification

## Purpose

Session History is the local lifecycle and discovery service for Kennedy
sessions. The Rust crate retains its historical package name
`kennedy-conversation-history`, but Conversation History is no longer the
application-domain name and the runtime no longer uses a conversation SQLite
database.

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
come from `kcode-session-log`. KennedyServer rebuilds Chatend from that ordered
log, and the browser falls back to the same events rather than requiring a
durable presentation snapshot. Successful completion deletes the control file.

At startup, Session History compacts active control journals. It retains the
latest lifecycle record and the latest state of every command, removes
superseded records and any historical presentation snapshots, writes the
replacement atomically, and leaves `.session-log` and pending-object files
unchanged.

## Lifecycle

The lifecycle projection uses these phases:

```text
active -> ingress_pending -> ingress_in_progress -> complete
```

An `active` session accepts ordered commands and staged browser objects.
Ending the source session changes it to `ingress_pending`. The single global
Kweb writer lane claims it as `ingress_in_progress`. Source conversation and
history ingress remain parts of the same session log. A successful Kweb commit
supplies a completion receipt; Session History synchronizes that receipt and
then removes the session log, pending objects, and control file.

Failures may return the record to `ingress_pending` with bounded diagnostics.
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

## Browser API

The compatibility route prefix remains `/api/v1/conversations`; that transport
name does not change the Session History domain:

- `GET /api/v1/conversations/health`
- `GET /api/v1/conversations/summaries`
- `GET /api/v1/conversations/{id}`
- `POST /api/v1/conversations/start`
- `GET /api/v1/conversation-commands`
- `POST /api/v1/conversations/{id}/commands`
- `POST /api/v1/conversations/{id}/stop`
- `POST /api/v1/conversations/{id}/objects`
- `POST /api/v1/conversations/{id}/retry-ingress`

Completed summary records intentionally contain only their permanent Kweb
object ID. The frontend follows that ID through
`GET /api/v1/session-history/{object_id}` to classify and display the archive.

The object route accepts exactly one multipart `file`. It writes one durable
pending-object file, appends the corresponding event, and returns the
event-position-derived temporary `pending:N` ID.

## Isolation and cutover

Session History owns no graph policy and does not commit Kweb transactions.
The orchestration worker owns those actions. The old conversation SQLite
database is an offline archive and has no runtime loader.

At the 2026-07-23 cutover, all unfinished legacy sessions were exported one per
text file, the full legacy conversation database was moved under
`data/archive/`, and legacy conversation rows were copied out of the still-live
mixed memory-ingress database before deletion.

## Verification

Tests cover coordinated transcript/control creation, durable commands,
structured completion receipts, pending objects, control-journal compaction,
and immutable completed-history behavior.
