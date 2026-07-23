# Session History Specification

## Purpose

Session History is the local lifecycle and discovery service for Kennedy
sessions. The Rust crate retains its historical package name
`kennedy-conversation-history`, but Conversation History is no longer the
application-domain name and the runtime no longer uses a conversation SQLite
database.

An in-progress session has one authoritative append-only `.chatend` file.
After the session and its history-ingress phase commit, local Session History
retains only the canonical Kweb object ID of the immutable session archive.

## Storage

The defaults are:

- in-progress journals: `data/sessions/in-progress/{uuid}.chatend`;
- completed ID list: `data/session-history.txt`.

The completed list contains one eight-character canonical object ID per line,
in commit order. Appending a duplicate ID is a successful no-op. Every append
is flushed and fsynced. Session details are not duplicated in a local index;
clients load the immutable archive from Kweb when needed.

The `.chatend` format and its recovery rules are owned by
`kennedy-chatend`. Session History writes lifecycle and command sideband frames
into the same file. It never rewrites the journal.

## Lifecycle

The lifecycle projection uses these phases:

```text
active -> ingress_pending -> ingress_in_progress -> complete
```

An `active` session accepts ordered commands and staged browser objects.
Ending the source session changes it to `ingress_pending`. The single global
Kweb writer lane claims it as `ingress_in_progress`. Source conversation and
history ingress remain parts of the same Chatend journal. A successful Kweb
commit supplies the permanent session object ID; Session History appends that
ID to `session-history.txt` and then removes the local journal.

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

The object route accepts exactly one multipart `file`. It writes the raw bytes
into the session journal and returns a temporary `pending:N` ID. Individual
objects and the aggregate staged object payload are limited to 32 GiB.

## Isolation and cutover

Session History owns no graph policy and does not commit Kweb transactions.
The orchestration worker owns those actions. The old conversation SQLite
database is an offline archive and has no runtime loader.

At the 2026-07-23 cutover, all unfinished legacy sessions were exported one per
text file, the full legacy conversation database was moved under
`data/archive/`, and legacy conversation rows were copied out of the still-live
mixed memory-ingress database before deletion.

## Verification

Tests cover one-journal managed creation, durable command sidebands,
idempotency, the completed object-ID-only list, upload exclusion while commands
run, and immutable completed-history behavior.
