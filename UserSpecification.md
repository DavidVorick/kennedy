# Kennedy and Kweb Specification

## Product

Kennedy is a local-first assistant that uses a durable knowledge web, or Kweb,
as long-term memory. The user talks to Kennedy through the browser or Telegram.
Kennedy may inspect memory in parallel, but all writes are gathered into
explicit transactional sessions.

## Kweb

The Kweb is a directed graph of nodes. A node contains:

- short name;
- short and long descriptions;
- owner (`self`, `unowned`, or another node);
- ordered fixed connections;
- ordered recent connections;
- immutable object references.

Connections are one-way. Fixed and recent connections remain distinct. The
database stores complete arrays and does not impose an application-level count
limit.

Node and object IDs are canonical eight-character URL-safe Base64 strings in
disjoint domains. Kennedy and users see these real IDs. Session-local fake
short node IDs do not exist.

`kcode-kweb-db` 1.0 owns persistence. Current nodes, histories, objects,
transaction packages, append-only log, state, and WAL use canonical,
checksummed binary formats. The transaction log is not loaded wholesale into
memory and the Kweb is not rebuilt at startup.

## Roots and identities

Kennedy's root, the primary user's root, whitelisted Telegram user roots, and
Telegram group roots are application data stored in the user directory by
canonical node ID. They are not responsibilities of the Kweb library.

Trusted users share one Borg-like graph. Kweb contains no per-node access
control system.

## Sessions

A logical session contains the source activity and its later history ingress.
Examples are:

- browser conversation;
- private or group Telegram conversation;
- self time;
- audio history ingress;
- group background ingress.

Every session produces exactly one immutable session archive object and, when
it has write effects, exactly one Kweb transaction. Source conversation and
history ingress are not separate permanent records.

## Boxes

Every item Kennedy can see in context is a box:

- system prompts;
- user and Kennedy messages;
- Kweb nodes;
- tool calls and results;
- controller notices;
- document/audio material;
- history-event inspections.

A box has a stable ID equal to the event that created it. Later canonical
updates or representation changes create new events without changing the box
ID. Continuation markers explain this movement in the active context.

A box is:

- **hydrated** when canonical content is in context;
- **dehydrated** when content is absent from context;
- **summarized** when Kennedy's replacement text is in context;
- **stale** when a dehydrated or summarized representation predates the latest
  canonical revision.

Kennedy normally decides when to dehydrate, summarize, or hydrate. The
controller does not invent diffs for stale boxes. History-ingress preparation
retains Kennedy-authored summaries and automatically fits the remaining
context to its model-relative budget.

The retired terms “blanched” and “dirty” are not used.

## Tools

The provider always has the `call_ktool` bridge. Actual tool definitions and
operating instructions are system-prompt boxes.

Read tools include:

- `LoadNode`;
- box and history hydration/dehydration/summarization;
- web search and fetch;
- managed Rust library tools.

Write tools stage:

- node creation and complete node updates;
- fixed/recent connection changes;
- object attachment;
- fanout consolidation and other Kweb graph operations.

There is no ResetContext and no fixed LoadNode count. Dehydrating a Kweb node
box does not unload the node's state.

Loaded Kweb boxes use one globally deduplicated layout. Full directly loaded
nodes come first, full fixed connections second, and full active connections
third. Active means the first eight recent connections. A full node shows its
ID, name, summary, long description, and identifier-only fixed, active, and
fanout lists. Per-loaded-node fanout boxes and the three second-layer aggregate
boxes add summaries or names only for nodes not already represented earlier.

A manual `LoadNode` invocation remains visible in its normal JSON call box.
Every Ktool result is raw text. Ordinary result boxes contain that text
directly, without a JSON envelope or pretty-printing. Managed Rust library
`create` and `open` establish or refresh one stateful complete-source box per
library. A successful `write` advances that same box's canonical revision
without changing its box ID or adding a generic result box. Its active tool-call
box records only the library name and file count; complete write arguments
remain in the durable invocation event but are not projected into later
provider input. Failed writes leave the library box unchanged and return a
short ordinary error. A summarized or dehydrated library box keeps that
representation and becomes stale when its canonical source changes. Other
managed Rust operations retain ordinary result behavior.

The successful `LoadNode` result is not copied into a generic result box. Instead,
the native tool response contains exactly the Kweb boxes that the load created
or revised, using their current Chatend-rendered representation and Kweb
layout order. This lets the already-running provider turn see the state it
just loaded without duplicating it. A no-change reload and a failed load
return short plain text. The journal retains only a compact completion receipt
for the operation.

## Temporary IDs and staged objects

The ordered session transcript is the identity space. A pending node or object
is named `pending:N`, where N is reconstructed from the allocating event's
array position. Event IDs and pending IDs are not serialized as separate
session-log fields.

Original object bytes are staged in one file per object before the matching
`pending-object` event is appended. A later Kweb commit reads those bytes and
obtains canonical object IDs. The Session History completion receipt records
the pending-to-canonical mapping.

Kweb owns its transaction and object limits. The `kcode-session-log` package does not
interpret or duplicate those limits.

## Durability

An in-progress session has one append-only, checksummed `.session-log` file.
Its durable logical value is a three-field header followed by an ordered array
of `{role, text}` events. Every complete append is synchronized. Recovery
discards an incomplete trailing frame but rejects durable checksum-valid
structural failures.

A pending object file contains a fixed header, filename, media type, byte
length, byte checksum, and object bytes. It is synchronized and atomically
renamed before its event is appended. Opening a session removes object files
that have no transcript event. Sealing verifies referenced objects, writes a
durable footer, and makes the log immutable.

Lifecycle and browser commands are stored in a separate Session History
control journal. Chatend representation state, context policy, and Kweb plans
belong to KennedyServer. Final transaction receipts belong to Kmap and Session
History.

## Context budgets

A normal source session may use 70% of the model's effective context window.
A user message, tool call or result, or Kennedy response that would exceed
that limit is rejected and replaced by a system capacity message. If context
after that message exceeds 75%, the controller force-ends the source and
queues history ingress.

History ingress uses 75% only as its initial fitting target and may use the
model's full effective context window once running. Exceeding the full window
force-commits the useful work completed so far.

## History ingress

History ingress begins from the same journal. The controller:

- preserves the complete event history;
- retains Kennedy-authored summaries;
- hydrates all other source boxes when the resulting context fits;
- otherwise reduces eligible boxes from largest to smallest until the context
  is at or below 75% of the ingress model's context window;
- initially protects the system prompt, conversation messages, full directly
  loaded/fixed/active Kweb nodes, and short tool invocations, then dehydrates
  those protected boxes largest-first if they alone cannot fit at or below 75%;
- if a fully dehydrated projection is still above 75%, commits the
  staged session transaction without invoking Kennedy;
- programmatically summarizes a tool invocation longer than 1,000 characters
  only if the largest-first fitting pass reaches it;
- loads Kennedy's and the user's root nodes;
- revalidates nodes after obtaining the global writer lane;
- posts compact update occurrences for changed nodes;
- keeps canonical content available for later hydration.

Kennedy may inspect any event or box, then builds the final Kweb plan.

## Session History

In-progress sessions appear through Session History. Completed local history is
only a text file of permanent session archive object IDs. Selecting one loads
the complete immutable object from Kweb.

There is no local completed-session metadata index, no purge control, and no
legacy loader.

## Concurrency

Read-only sessions run in parallel. One global V1 writer lane serializes all
Kweb-writing sessions. A conversation can begin read-only and later acquire
the lane; it must revalidate loaded nodes at that point.

Self time acquires the lane before loading. Kennedy may leave one message for
the next self-time session. More advanced session handoff and swarm concurrency
are future work.

## Legacy cutover

Legacy node IDs were already migrated independently and are not translated by
session-log. The completed `.chatend` conversion retained its original files
in the migration archive; the live directory now accepts only current
session-log and control-journal files.

At cutover:

- every unfinished legacy session is exported as one text file;
- the complete legacy conversation database and its provenance are archived;
- legacy conversation work is removed from the live mixed ingress queue only
  after an archive copy exists;
- no runtime cutover command, compatibility store, or legacy loader remains.

The archives can be moved off-machine and manually ingressed if desired.
