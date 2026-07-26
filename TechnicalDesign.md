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
3. `kcode-session-history` 0.1.0 is a published standalone library. It is the
   only consumer of `kcode-session-log` and owns opaque active `Session`
   handles, reconstructed Chatend and box state, lifecycle and command
   journals, pending objects, and completion receipts. It has no Axum or HTTP
   API.
4. `kcode-dev-tools` owns the shared session-scoped Ktool service, leases,
   source snapshots, and backend calls for managed Rust libraries, Web
   libraries, and Rust binaries. `kcode-rust-bins` keeps editable source and
   immutable local executable publications in separate roots.
5. `kcode-intelligence-router` owns all Gemini, OpenAI, and Codex model calls,
   exact-model routing, cancellation, error normalization, and per-user
   per-call usage receipts. It is a typed in-process library with no HTTP API.
6. `kennedy-server` owns context-policy decisions, orchestration, Ktool
   authorization and Chatend integration, graph policy, the one Kweb writer
   lane, all browser HTTP adapters, roots, the credential vault, and the tiny
   root UI loader page. It adapts canonical Kweb objects for Rust-binary call
   inputs and outputs and serves immutable Web publications.
7. `kcode-audio-ingress` owns durable audio intake, chunking, transcript
   workflow, and automatic whole-job recovery. It delegates every model call
   through typed callbacks backed by `kcode-intelligence-router`. KennedyServer
   owns the Axum adapter and a separate prepared-transcript memory-ingress
   queue; those downstream concepts are absent from the standalone library.
8. Conversational history ingress uses the same session log as its source
   session; KennedyServer serializes it with audio work through the global
   Kweb writer lane.
9. `kcode-tg-kennedy-bot` owns Telegram transport and its durable event stream.
10. The browser frontend is the managed `kcode-kennedy-ui` Web library and is
    an observer and command client only. The managed `kcode-kui-loader`
    library selects its floating patch line.

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

The provider always receives one dynamic function, `call_ktool`. The system
prompt explains the Kmap bootstrap, critical Kmap/context navigation tools, and
writable Kmap mutations when the mode permits them. Other tool names and
contracts are discovered through Kmap manuals. Dehydrating prompt or manual
boxes does not unregister the function.

Every Kennedy message, user message, tool invocation, ordinary tool result,
loaded Kweb node, system prompt, controller notice, and history inspection is
a box. Every Ktool returns raw text, and the native response forwards that
string unchanged. An ordinary result box stores the same text directly,
without a JSON envelope, Serde rendering, diagnostic wrapper, or
pretty-printing. Its normal Chatend header is added only when Chatend projects
the retained box in a later context. Multiple tool calls in one model response
are recorded independently. New invocation and completion events share one
durable invocation ID, so parallel or out-of-order completion is unambiguous.
When a process interruption leaves an invocation without a completion, session
recovery appends an explicit failed completion before sealing; legacy journals
without invocation IDs retain their historical LIFO compatibility.

Managed Rust and Web library `create` and `open` operations install one
stateful complete-source box per name and library kind only after a
non-mutating projection preview accepts the returned snapshot. An
over-capacity result leaves Chatend unchanged and returns the bounded capacity
error. Successful writes revise that box in place and suppress a generic
result copy. The durable invocation event retains the exact complete write
arguments, while its active call box contains only bounded identifying
metadata so a later provider request sees one complete source. Failed writes
do not revise the source box. Kennedy-authored summaries and dehydration
survive canonical source updates and become stale under the ordinary box
rules. `LoadNode` is the other deliberate result exception: its
invocation remains a box, but it updates the shared Kweb boxes
and returns the exact newly created or revised box renderings directly to the
in-flight provider turn. It does not create a second generic result box.
`EmitObject` is another deliberate exception: it validates one canonical Kweb
object and creates a durable object-bearing Kennedy message box. That message
is the adapter outbox and may terminally answer a conversational turn without
assistant prose; no generic result box duplicates it.

Published Web-library trees are immutable. `/lib/<name>/<selector>` resolves
Cargo-compatible SemVer requirements and redirects to the highest matching
exact publication's manifest entry; file routes preserve the requested
relative path. Exact files are immutable-cacheable and floating redirects are
uncached. `/module/<name>/v<exact>/<file>` remains an alias for the dependency
URLs used by the library's Chromium checker.

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
archive duplication or purge endpoint. The live directory accepts only current
`.session-log` and `.session-control` files; completed cutover inputs remain
offline under `data/archive/` and `data/sessions/legacy-chatend-migration/`.

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

Accepted browser and Telegram bytes first enter that pending sidecar. The
owning user message box references `pending:N`; extraction, transcription, or
annotation is supplementary and never replaces the original object. Browser
recordings and current Telegram voice messages are not eagerly transcribed.
The transport stages the original audio and leaves both tool choice and prompt
to Kennedy. Older Telegram group voice context is likewise not silently
pre-transcribed. At history-ingress
completion, Kmap wraps each file in a versioned binary envelope containing
filename, MIME type, source transport kind, and exact bytes. It creates all
objects first, then replaces exact known pending-object tokens in the immutable
archive and staged node descriptive text before creating/updating nodes and
finalizing the one transaction.

Canonical object HTTP reads decode that envelope, while active-session reads
serve the pending sidecar directly. Transcript projections retain object IDs
and bounded attachment metadata. The browser selects inline
image/video/audio/PDF presentation or a download card. Telegram sends native
media through the additive relay `/media` multipart route and uses the
existing `/file` route only for deliberate generic-document delivery. Native
delivery failures remain native failures rather than triggering a semantic
fallback. The final response part alone completes the relay event.

Three model-callable read tools consume only active-session pending objects.
An additional `StageTelegramGroupMedia(messageId)` read tool resolves retained
media metadata from the session's current bounded Telegram group context,
validates the exact group/message identity, fetches the already-local relay
bytes, enforces the 20 MiB limit, and stages them idempotently. This keeps raw
bytes out of model context and avoids eagerly duplicating every group
attachment into every active participant session. A group message need not
invoke Kennedy for its retained media to be eligible; invocation controls turn
creation only. Static Telegram behavior is isolated in Telegram-only
system-prompt layers. The bounded message history is a separate controller box
rendered as natural-language prose; exact structured group data remains
internal for authorization and media lookup.
`TranscribeAudio` checks the 20 MiB limit and supported audio MIME types, then
sends the exact original audio, exact selected supported model, and Kennedy's
required 4,000-character-bounded prompt to OpenAI or Gemini. The call joins
the active parent operation's cancellation scope.
`AnnotateMedia` reads the exact sidecar bytes after checking the 20 MiB
enrichment limit, exact-model/media matrix, and Kennedy-authored
4,000-character prompt. Intelligence maps the exact model to OpenAI
single-image analysis, a fresh ephemeral tool-free Codex image turn, or
Gemini multimodal inference. All calls join the active parent operation's
cancellation scope.
Codex's media-bearing provider-input event is consumed without journaling it.
`ExtractDocumentText` passes PDF, DOC, or DOCX bytes to the in-memory document
extractor. All paths render provenance plus returned text as an ordinary
Ktool result, leaving the original pending object unchanged.

Internal Kweb disk encodings are canonical binary and checksummed. Session-log
headers and events use checksummed frames; pending objects have a fixed header,
declared lengths, and a byte checksum. HTTP/provider boundaries use ordinary
typed JSON and multipart data; internal binary formats are not exposed as API
encodings.

## 12. Credentials and backup

The Kweb writer signing key lives in the passphrase-encrypted credential vault.
Kennedy never receives it. The server passes it directly to `kcode-kweb-db`
after a human unlocks the vault.

All Kennedy-owned runtime paths default beneath the repository-local `data/`
tree, including managed Rust/Web libraries, Rust-binary source, and immutable
Rust-binary executable publications. `scripts/backup` is an offline,
format-agnostic backup: after verifying that Kennedy is stopped, it archives
the complete tree without interpreting SQLite, Kweb, Session History, audio,
vault, recovery, or legacy formats. A small metadata member records the source
commit and dirty status. Recovery uses those opaque bytes with the matching
source version; format inspection and migration happen at recovery time.

Managed Web source and immutable Web publications use distinct roots.
Editable source lives beneath `data/kcode/kcode-web-libs/`; publications live
beneath
`data/kcode/kcode-web-libs-published/module/<name>/v<version>/`. The
publication root is created lazily on first use. Rust-binary publications use
the distinct `data/kcode/kcode-rust-bin-artifacts` root.

Backup archives are written outside `data/` to prevent recursive inclusion.
Codex authentication and original vnote source media are replaceable external
inputs rather than Kennedy-owned runtime persistence.

## 13. Known V1 boundaries

- exact prepared-transaction replay is deferred;
- zero-copy staged object handoff is deferred;
- the global writer lane is intentionally coarse;
- gossip integration is future work;
- mutually dependent new nodes cannot be created in one session through the
  current random-ID transaction builder without a future reservation API;
- legacy sessions are offline archives, not loadable compatibility records.
