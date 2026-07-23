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
The successful result is not copied into a generic JSON result box. Instead,
the native tool response contains exactly the Kweb boxes that the load created
or revised, using their current Chatend-rendered representation and Kweb
layout order. This lets the already-running provider turn see the state it
just loaded without duplicating it. A no-change reload and a failed load
return short plain text. The journal retains only a compact completion receipt
for the operation.

## Temporary IDs and staged objects

The Chatend journal uses one monotonic temporary identity space. A pending node
or object is named `pending:N`, where N is the allocation event. Box and
pending IDs cannot overlap at the integer level.

Original object bytes are staged in the append-only journal before provider or
Kweb work is acknowledged. A later Kweb commit reads those bytes and obtains
canonical object IDs. The final archive may retain pending IDs as its internal
references; the commit result separately returns the canonical IDs needed by
live application state.

V1 permits up to 32 GiB for one object and 32 GiB aggregate object payload in a
transaction. Larger streaming objects are future work.

## Durability

An in-progress session is one append-only, checksummed file. JSON is used for
evolving Chatend state; object data is raw binary. Each update is appended as
one checksummed frame. Related events may share one transition frame and
checksum. Recovery replays the valid prefix in place and discards the first
incomplete or checksum-invalid frame together with everything after it.

Journal writes use ordinary append I/O without explicit flush or `fsync`.
Process termination does not discard data already accepted by the kernel, but
the journal makes no stronger power-loss durability claim. Checksum-valid
structural or semantic failures remain errors.

V1 deliberately accepts one narrow exception: the current Kweb library commits
a locally built transaction inside `finalize`, so Kennedy cannot prepare and
persist the exact signed package before submitting it. A crash after Kweb
commit but before the local completion event has an unknown outcome. A
prepared-package API is deferred and this window must not be represented as
exact-once behavior.

## Context budgets

A normal source session may use 70% of the model's effective context window.
History ingress may use 75% of the context window of the model it calls.

If a hydration or tool result would exceed capacity, Kennedy receives an
error. If repeated failures push an ordinary source above 72%, the controller
force-ends it and queues history ingress. If history ingress fills 75% and
cannot continue, it commits the useful work completed so far.

## History ingress

History ingress begins from the same journal. The controller:

- preserves the complete event history;
- retains Kennedy-authored summaries;
- hydrates all other source boxes when the resulting context fits;
- otherwise reduces eligible boxes from largest to smallest until the context
  is below 75% of the ingress model's context window;
- protects the system prompt, conversation messages, full directly
  loaded/fixed/active Kweb nodes, and short tool invocations from automatic
  dehydration;
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
Chatend. Legacy conversation records are not imported into the new session
model.

At cutover:

- every unfinished legacy session is exported as one text file;
- the complete legacy conversation database and its provenance are archived;
- legacy conversation work is removed from the live mixed ingress queue only
  after an archive copy exists;
- no runtime compatibility store or loader remains.

The archives can be moved off-machine and manually ingressed if desired.
