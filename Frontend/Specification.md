# Frontend Specification

## Scope

Kennedy's frontend is browser-native HTML, CSS, and JavaScript. It has no
Node.js runtime dependency, bundler, package manager, TypeScript build, or
backend execution authority. `kennedy-server` serves the files and owns every
live Chatend.

The browser:

- selects and observes Session History records;
- submits start, message, retry, end, stop, and ingress-retry commands;
- captures attachments and voice input;
- uploads original objects into an idle in-progress journal;
- renders Chatend boxes, Kweb state, audio work, and Telegram state;
- keeps unsent draft and capture state in browser memory.

It does not compose provider prompts, execute tools, mutate Kweb directly, or
manage box representations on the user's behalf.

## Addresses

The main origin, normally `http://127.0.0.1:4321`, serves static files and all
Kennedy-owned APIs. The published Telegram transport remains on loopback port
4324. `app.js` derives Kennedy API bases from `window.location.origin`.

## Session History

The UI calls the compatibility `/api/v1/conversations` routes through a client
named `SessionHistoryAPI`.

In-progress summaries come from the local `.session-log` transcript and
Session History control journal. Completed
summaries contain only a Kweb object ID. The browser loads the corresponding
immutable archive from `/api/v1/session-history/{object_id}` before classifying
it as conversation, self-time, Telegram, or audio history. Hydrated records
never regress to a later summary response of the same or older version.

The completed archive is a three-field session header and an ordered array of
role/text events. The browser reconstructs display messages from that stable
event order; persisted box projections and active/retired snapshots are not
part of the archive.

One session log spans the source and history-ingress phases. The browser uses
the durable `source_terminated` and `history_ingress_started` context events to
produce non-overlapping conversation, transition, and ingress views. Session
History control state supplies lifecycle, failure, recovery, and commit status;
it is not an alternative source of display events.

Completed records are read-only. There is no purge control.

## Chatend display

Every provider-visible item is represented by a box. The diagnostic view
renders current boxes and exposes:

- stable box ID, equal to its creation event ID;
- owner and name;
- canonical revision and occurrence history;
- hydrated, dehydrated, or summarized representation;
- stale state when a compact representation predates the canonical revision;
- referenced pending or canonical objects;
- continuation markers that explain chronological movement.

The user can inspect these facts but cannot alter boxes. Kennedy alone calls
`DehydrateBox`, `SummarizeBox`, and `HydrateBox`.

System prompts are ordinary persisted boxes. The provider-level `call_ktool`
function remains registered independently of whether Kennedy has dehydrated
the box explaining it.

## Composer and objects

The browser stages attachments before queueing a message:

1. extract user-facing document or audio metadata as needed;
2. upload the original `File` or `Blob` as multipart data;
3. receive a shared temporary ID such as `pending:47`;
4. place that ID in the queued message metadata.

The browser does not base64-encode the object into JSON. The maximum individual
and aggregate staged object payload is 32 GiB. Uploads are sequential and the
composer is disabled while the session processes a command.

At final commit, the backend reads staged bytes and supplies them to
`kcode-kweb-db`. This deliberately performs extra disk I/O in V1; a zero-copy
prepared-object API is deferred.

## Context limits

Ordinary source sessions operate within 70% of the model's effective context
window. A user message, tool call or result, or Kennedy response that would
cross that limit is rejected and displayed as a system message. If the context
after that message is above 75%, the source is force-ended and queued for
history ingress. Ingress first fits boxes at or below a 75% target, dehydrating
formerly protected boxes largest-first if necessary, and may then use 100%.
If every box is dehydrated and the initial projection is still above 75%, the
session commits without running ingress.

The UI displays backend-projected usage. It does not estimate a different
policy.

## Self time and concurrency

Self time uses the same Chatend model. Kennedy may leave one handoff message
for the next self-time session. There is no ResetContext operation.

The backend allows read-only sessions in parallel. One global write lane
serializes source-history ingress, self time, audio ingress, and other Kweb
writes. Browser busy state reflects the relevant durable command rather than a
global read lock.

## Files

```text
Frontend/
  Specification.md
  SystemPrompts/
  public/
    index.html
    css/styles.css
    js/
      api.js
      app.js
      chatend_format.js
      human_format.js
      memory_explorer.js
      render.js
      session_log_view.js
      self_time.js
  tests/mvp.test.mjs
```

`node --test Frontend/tests/*.test.mjs` is a development verification command;
Node is not needed in production.

## Safety

Rendering uses DOM text nodes rather than HTML injection. API errors are shown
as text. The frontend never receives credential-vault secrets or a Kweb writer
private key. Canonical node and object IDs are displayed directly; fake short
node IDs are gone.
