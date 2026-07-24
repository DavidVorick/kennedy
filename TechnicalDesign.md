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
2. `kcode-session-log` 0.2.1 owns append-ordered role/text history, pending-object
   files, recovery, integrity validation, sealing, and exact cleanup.
3. `kennedy-conversation-history` implements the Session History lifecycle and
   browser adapter. Its package name is historical; it owns no conversation
   SQLite database.
4. `kennedy-server` owns the reconstructed Chatend, box and representation
   state, context limits, orchestration, provider calls, tool execution, graph
   policy, the one Kweb writer lane, Kweb HTTP routes, roots, the credential
   vault, and static assets.
5. `kennedy-audio-ingress` and `kcode-audio-transcribe` own durable audio
   intake and transcription. The AudioIngress database also owns prepared
   transcript pieces and their complete memory-ingress queue lifecycle.
6. Conversational history ingress uses the same session log as its source
   session; KennedyServer serializes it with audio work through the global
   Kweb writer lane.
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
2. create the immutable session-log archive object;
3. reserve every pending node ID;
4. resolve circular pending-node and pending-object references;
5. create the reserved nodes;
6. apply complete node updates;
7. finalize the transaction.

`SessionNodeData` contains complete fixed and recent connection arrays. The
library imposes no application connection-count policy. Kennedy graph tools
own any future policy.

The session archive object is attached to every affected node. The final result
maps `pending:N` identifiers to canonical IDs. Pending IDs never cross into
durable Kweb storage.

The Kmap adapter stores a request hash and prepared result keyed by session ID
before Kweb finalization. Replaying an identical request returns the recorded
result. If final receipt recording was interrupted, visibility of the prepared
archive object proves the atomic Kweb transaction committed and prevents a
duplicate. The current Kweb API cannot recover that object's creator
transaction ID, so that recovery path returns a null transaction ID with the
complete archive, node, and object mappings. Reusing a session ID with
different transaction input is rejected.

## 4. Session log

Each source session and its history-ingress continuation share one transcript:

```text
data/sessions/in-progress/{session-uuid}.session-log
```

Its logical value is:

```text
header { formatVersion, sessionId, createdAt }
events [ { role, text }, ... ]
```

The format version is the string `0.2.1`. Event array position is stable
identity. The package serializes no event ID, box ID, pending ID, correlation
ID, representation state, context limit, Kweb plan, lifecycle record, command
record, or generic metadata.

The physical file is framed and checksummed. Every append is synchronized.
Opening truncates only an incomplete trailing frame and rejects durable
checksum or structure failures. Sealing verifies referenced objects, appends a
durable footer, and prevents further events. Tool completion is Kennedy
semantics and is validated by KennedyServer before sealing.

One pending object is stored in
`{session-uuid}-{event-position}.pending-object`. The complete object file is
synchronized and renamed into place before its transcript event is appended.
On open, unreferenced temporary and final object files are removed. A
referenced missing or invalid object is corruption.

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

Every Kennedy message, user message, tool invocation, ordinary tool result,
loaded Kweb node, system prompt, controller notice, and history inspection is
a box. Every Ktool returns raw text, and the native response forwards that
string unchanged. An ordinary result box stores the same text directly,
without a JSON envelope, Serde rendering, diagnostic wrapper, or
pretty-printing. Its normal Chatend header is added only when Chatend projects
the retained box in a later context. Multiple tool calls in one model response
are recorded independently. Managed Rust library results use the same path as
every other ordinary result, so an opened library and all of its files occupy
one result box. `LoadNode` is the deliberate result exception: its invocation
remains a box, but it updates the shared Kweb boxes and returns the exact newly
created or revised box renderings directly to the in-flight provider turn. It
does not create a second generic result box.

## 7. Kweb context and writes

`LoadNode` has no fixed call or node-count limit. Loaded nodes occupy stateful
Kweb tool slots and can be represented compactly without unloading them.
Repeated loads refresh canonical content.

The provider request is already in flight when a manual load updates Chatend,
so the current turn cannot see those journal mutations merely because they
were persisted. The native tool response therefore contains the changed Kweb
boxes in current Kweb layout order and in the same text format used by the
next Chatend projection. This respects each box's current hydrated,
summarized, or dehydrated representation. An unchanged repeat load returns a
short plain-text acknowledgement. Load failures are also plain text. A compact
structured completion receipt remains in the journal but is not projected as
a box.

The Kweb slots have an explicit persistent display layout. Directly loaded
nodes appear first with complete node text, followed by unique fixed nodes and
then unique active nodes (the first eight recent connections). Complete node
text contains the ID, name, summary, long description, and identifier-only
fixed/active/fanout lists. Each directly loaded node then has one fanout box
containing only previously unseen fanout IDs and summaries. Three final boxes
contain, with one global deduplication pass, fixed/active neighbor summaries of
fixed nodes, fixed/active neighbor summaries of active nodes, and remaining
fanout IDs/names of all fixed and active nodes. Tool layout events preserve
this order independently of box creation chronology and across restart.

Kweb write tools mutate only a durable `KwebPlan` in the journal. Staged
creates and updates immediately update Kweb boxes, providing read-your-writes
semantics. The plan is not applied until the session owns the writer lane and
finishes.

When a read-only session becomes a writer, the controller reloads its Kweb
nodes. Changed canonical nodes produce system update occurrences. Summarized
or dehydrated representations remain compact and become stale; no generated
diff is added.

## 8. Session lifecycle and context ceilings

Ordinary source sessions admit a user message, tool call or result, or Kennedy
response only when its exact projected Chatend remains at or below floor(70%
of the effective context window). A rejected addition is replaced by a bounded
system capacity message. If the projection after that message is above
floor(75%), the controller terminates the source and queues the same journal
for history ingress.

History ingress:

1. records source termination;
2. installs the current ingress prompt and model context-window metadata;
3. revalidates loaded Kweb nodes;
4. retains all current Kennedy-authored summaries and tentatively hydrates
   everything else;
5. if needed, fits context at or below the 75% initial target by reducing
   eligible boxes from largest to smallest, then protected conversation messages,
   full direct/fixed/active Kweb nodes, the system prompt, and short tool
   invocations from largest to smallest;
6. if every box is dehydrated and the projection is still above the target,
   commits immediately without invoking Kennedy;
7. lets Kennedy selectively hydrate source material while using up to 100% of
   the effective context window;
8. stages all Kweb effects;
9. commits one Kweb transaction and one session archive object.

If running ingress exceeds the full effective context window, it commits the
work completed so far.

A terminal prose response during ingress is retained, then followed by a
private controller message explaining that ingress is a solo session and
showing the exact native `call_ktool` arguments for `EndSession`. Reaching the
existing agent-loop round limit during ingress force-commits the staged
transaction rather than returning it to the retry queue.

## 9. Session History

Lifecycle and ordered browser commands live in a separate
`{session-uuid}.session-control` file. After commit, Session History appends a
structured receipt to `data/session-history.txt`, synchronizes it, and removes
the session log, pending objects, and control file.

The receipt includes the transaction ID when available, archive object ID,
pending-node mappings, and pending-object mappings. The frontend fetches the immutable
header/event archive from Kweb and rebuilds its display. There is no completed
archive duplication or purge endpoint. Startup migrates legacy `.chatend`
files and retains their originals under `data/sessions/legacy-chatend-migration`.

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

Kweb enforces its object and transaction payload limits. `kcode-session-log` does
not own application or database limits. The current boundary writes each
pending object once into its session sidecar and again into Kweb at commit.
Streaming and zero-copy handoff are future work.

Internal Kweb disk encodings are canonical binary and checksummed. Session-log
headers and events use checksummed frames; pending objects have a fixed header,
declared lengths, and a byte checksum. HTTP/provider boundaries use ordinary
typed JSON and multipart data; internal binary formats are not exposed as API
encodings.

## 12. Credentials and backup

The Kweb writer signing key lives in the passphrase-encrypted credential vault.
Kennedy never receives it. The server passes it directly to `kcode-kweb-db`
after a human unlocks the vault.

Backup format 11 captures:

- the complete Kweb root;
- active session logs, pending-object files, and control journals;
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
