# Kennedy Technical Design

## 1. Authority

`UserSpecification.md` defines product behavior. This document defines the
implemented architecture. The final Chatend decisions are recorded in
`chatend-overhaul/chatend-overhaul-clarifications.md` and the discussion review.
Those documents supersede the earlier draft files in that directory.

## 2. Runtime components

Kennedy is one Rust server with deliberately separated library domains:

1. `kcode-kweb-db` 1.0 owns the transactional Kweb, canonical nodes,
   immutable objects, signed transactions, binary disk formats, append-only
   transaction log, current-node files, WAL, and duplicate/dependency rules.
2. `kennedy-chatend` owns the append-only session journal, event/box state
   machine, temporary IDs, staged raw objects, replay, and provider projection.
3. `kennedy-conversation-history` implements the Session History lifecycle and
   browser adapter. Its package name is historical; it owns no conversation
   SQLite database.
4. `kennedy-server` owns orchestration, provider calls, tool execution, graph
   policy, the one Kweb writer lane, Kweb HTTP routes, roots, the credential
   vault, and static assets.
5. `kennedy-audio-ingress` and `kcode-audio-transcribe` own durable audio
   intake and transcription.
6. `kennedy-memory-ingress` retains prepared audio work. Conversational
   history ingress now lives directly in the Chatend journal.
7. `kcode-tg-kennedy-bot` owns Telegram transport and its durable event stream.
8. The browser frontend is an observer and command client only.

Kennedy-owned routers share the main loopback listener. The Telegram crate
retains its own loopback listener.

## 3. Kweb adapter

Kennedy uses real canonical eight-character node and object IDs across its
public boundary. The application user directory, not Kweb, maps roles and
Telegram identities to root IDs.

Read APIs remain conventional node/object queries. A completed write session
is adapted into one `kcode-kweb-db::Transaction`:

1. create all staged objects;
2. create the immutable Chatend archive object;
3. resolve staged node creates in dependency order;
4. apply complete node updates;
5. finalize exactly once.

`SessionNodeData` contains complete fixed and recent connection arrays. The
library imposes no application connection-count policy. Kennedy graph tools
own any future policy.

The session archive object is attached to every affected node. The final result
maps `pending:N` identifiers to canonical IDs. Pending IDs never cross into
durable Kweb storage.

The local transaction builder currently allocates random object/node IDs and
sets its signed commit timestamp during `finalize`. Because V1 deliberately
deferred a prepared `TransactionPackage`, there is a small crash window after
Kweb commit and before the Chatend journal records the result. The journal and
full semantic plan are fsynced first, but reconstructing after this particular
crash does not guarantee the same signed transaction. This limitation is
accepted and explicitly not described as exact-once recovery.

## 4. Chatend journal

Each source session and its history-ingress continuation share one file:

```text
data/sessions/in-progress/{session-uuid}.chatend
```

The file begins with `KCHAT01\n`. Each frame contains:

- one-byte frame kind;
- canonical little-endian payload length;
- SHA-256 payload checksum;
- payload.

Transition and sideband frames contain JSON because the Chatend model is
expected to evolve rapidly. Staged object frames contain a short JSON metadata
prefix followed by raw bytes. The journal is append-only and fsynced before an
operation is acknowledged. On open, an incomplete final frame is truncated;
checksum or structural failure in a complete frame is fatal.

The process maintains only the replayed current state, current transaction
plan, provider work, and staged object metadata in memory. Raw staged bytes are
read from disk only for final commit.

## 5. Event and box model

One monotonic integer namespace allocates event IDs, box IDs, and pending
resource IDs. A box ID is its creation event ID. A pending resource allocated
at event 47 is exposed as `pending:47`.

A box has:

- owner and stable name;
- canonical content and canonical revision event;
- occurrence history;
- hydrated, dehydrated, or summarized visible representation;
- stale state when its representation is based on an older canonical revision;
- optional object references and metadata;
- retirement state.

Canonical content is never destroyed by dehydration or summarization.
Representation changes append events. Continuation markers in the projected
context show where a stable box moved over time.

Tool-backed state uses named append-only slots. Updating a slot advances its
canonical box revision without observing Kennedy's summary as tool input.
Retired slots cannot be silently reused.

## 6. Provider loop

Every inference is a new provider request built from the current Chatend
projection. There is no hidden provider thread history and no ResetContext.
The exact provider input and provider usage receipt are appended to the journal.

The provider always receives one dynamic function, `call_ktool`. System prompt
boxes explain tool names and contracts. Dehydrating those instructions does
not unregister the function.

Every Kennedy message, user message, tool call, tool result, loaded Kweb node,
system prompt, controller notice, and history inspection is a box. Multiple
tool calls in one model response each get independent call/result boxes.

## 7. Kweb context and writes

`LoadNode` has no fixed call or node-count limit. Loaded nodes occupy stateful
Kweb tool slots and can be represented compactly without unloading them.
Repeated loads refresh canonical content.

Kweb write tools mutate only a durable `KwebPlan` in the journal. Staged
creates and updates immediately update Kweb boxes, providing read-your-writes
semantics. The plan is not applied until the session owns the writer lane and
finishes.

When a read-only session becomes a writer, the controller reloads its Kweb
nodes. Changed canonical nodes produce system update occurrences. Summarized
or dehydrated representations remain compact and become stale; no generated
diff is added.

## 8. Session lifecycle and context ceilings

Ordinary source sessions have a hard working ceiling of floor(70% of the
effective context window). History ingress has floor(100%). At the source
72% emergency boundary, the controller terminates the source and queues the
same journal for history ingress.

History ingress:

1. records source termination;
2. fully dehydrates ordinary source boxes;
3. hydrates the current system prompt and Kennedy/user root nodes;
4. revalidates loaded Kweb nodes;
5. lets Kennedy selectively hydrate source material;
6. stages all Kweb effects;
7. commits one Kweb transaction and one session archive object.

If ingress reaches 100% and cannot progress, it commits the work completed so
far. There is no extra output reserve.

## 9. Session History

Lifecycle and ordered browser commands are sidebands in the `.chatend` file.
After commit, Session History appends only the archive object ID to
`data/session-history.txt`, fsyncs it, and removes the journal.

The frontend fetches completed detail from Kweb on demand. There is no local
metadata database, completed archive duplication, purge endpoint, or legacy
loader.

## 10. Concurrency

Read-only sessions run concurrently. A single async mutex is the V1 Kweb writer
lane for:

- conversational and Telegram history ingress;
- self time;
- audio ingress;
- other backend-owned Kweb writes.

A session revalidates state after acquiring the lane. Self time acquires the
lane before loading. More granular concurrency is intentionally deferred.

## 11. Objects and limits

Chatend and Kweb both enforce 32 GiB for one object and for aggregate object
payload in one transaction. The implementation avoids cloning the complete
payload in Kennedy's adapter, but the V1 boundary writes the object once into
the Chatend journal and again into Kweb. Streaming and zero-copy handoff are
future work.

Internal Kweb disk encodings are canonical binary and checksummed. Chatend's
evolving control events are checksummed JSON frames. HTTP/provider boundaries
use ordinary typed JSON and multipart data; internal binary formats are not
exposed as API encodings.

## 12. Credentials and backup

The Kweb writer signing key lives in the passphrase-encrypted credential vault.
Kennedy never receives it. The server passes it directly to `kcode-kweb-db`
after a human unlocks the vault.

Backup format 10 captures:

- the complete Kweb root;
- active Chatend journals;
- `session-history.txt`;
- runtime SQLite services;
- audio media;
- the encrypted vault when present.

Legacy conversation files live only under `data/archive/` and are not runtime
persistence.

## 13. Known V1 boundaries

- exact prepared-transaction replay is deferred;
- zero-copy staged object handoff is deferred;
- the global writer lane is intentionally coarse;
- gossip integration is future work;
- mutually dependent new nodes cannot be created in one session through the
  current random-ID transaction builder without a future reservation API;
- legacy sessions are offline archives, not loadable compatibility records.
