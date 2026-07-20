# Frontend Specification

## 1. Scope

The frontend is a browser-native HTML, CSS, and JavaScript application. It has
no Node.js dependency, package manager, bundler, TypeScript, or compile step.
The main Rust server serves it on localhost beside the Kmap and intelligence
HTTP routes.

The frontend owns Kennedy's UI only. It observes backend-owned durable records,
submits explicit start/message/retry/end/stop and ingress-retry requests, keeps
unsent drafts and browser capture state, and renders the memory explorer. The
native Rust `kennedy-server` orchestrator owns clean transcripts,
Chatends, context glue, prompt composition, agent/tool loops, checkpoints,
recovery, Telegram, self time, graph policy, and ingress coordination.

## 2. Source Layout

```text
Frontend/
  Specification.md
  SystemPrompts/
    KennedyIdentity.txt
    ConversationSession.txt
    SelfTimeSession.txt
    HistoryIngressSession.txt
    AudioIngressSession.txt
    KmapBasics.txt
    ReadTools.txt
    WriteTools.txt
  public/
    index.html
    css/
      styles.css
    js/
      api.js
      app.js
      chatend_format.js
      human_format.js
      memory_explorer.js
      render.js
      self_time.js
KennedyServer/
  src/
    intelligence/
      mod.rs
      defaults.rs
    orchestration.rs
    orchestration/
      context.rs
      http.rs
      prompts.rs
      session.rs
      worker.rs
```

Files under `public/js` are browser-native presentation modules. `app.js` wires
UI events to durable backend commands and polls view state. The native Rust
modules under `KennedyServer/src/orchestration` own live sessions, tool
execution, Telegram handling, self time, and the serialized ingress queue. The
server neither executes browser modules nor launches a JavaScript runtime.

## 3. Backend Addresses

The frontend defaults to:

- Main static origin and all Kennedy-owned APIs: `http://127.0.0.1:4321`
- Published Telegram relay API: `http://127.0.0.1:4324`

The values are defined once in `app.js` so alternate local ports do not require
changes throughout the codebase.

## 4. Application State

Live application state is held in JavaScript memory:

```js
{
  activeView: "conversation", // conversation | self-time | telegram | audio | memory
  selectedConversationId: null,
  selectedByView: { conversation: null, "self-time": null, telegram: null },
  historyRecords: [],
  drafts: new Map(),       // durable ID -> unsent composer text
  voiceDrafts: new Map(),  // durable ID -> original audio + transcription metadata
  attachmentDrafts: new Map(),
  audioDetails: new Map(),
  explorer: {
    currentNodeId: null,
    back: [],
    forward: []
  }
}
```

Each backend `ConversationSession` owns its clean transcript, complete Chatend, Kweb
context and short-ID maps, LoadNode counter, tool log, usage, continuation,
start time, pending-turn flag, busy state, configured model and reasoning effort,
their combined model-attribution value, its own direct root IDs, and any
unloaded group-participant root references. Root selection is durable session
state rather than one application-global user root; group-session records
store user, group, and Kennedy roots in that order.
Self-time sessions (durably typed as `free-time`) additionally own persisted run/deadline metadata and a
run-level mutation provenance ID.

The frontend does not use local storage, IndexedDB, cookies, or service-worker
caches for persistence. The backend checkpoints an opaque recovery snapshot to
the conversation history API. The versioned snapshot contains recovery fields
plus a lossless JSON recovery archive: composed system prompt, retained
content, structured messages, structured Kmap snapshot and durable-ID
diagnostics, tool log and counters, usage telemetry, and a media collection
containing original voice recordings plus future serializable media or
attachment references. This JSON is storage format, not Chatend text and never
becomes a generation prompt. Provider response IDs and credentials are
transport details and are not archived.

The archive never projects message `content` to text. Arrays and objects are
preserved recursively, so future multimodal content blocks survive storage
even before every inspector has a renderer for that media type. Active records
reconstruct the canonical Chatend from the archived messages on a fresh Codex
thread. Legacy
transcript-only snapshots remain readable and recover through the old rebuild
path.

The recovery archive also contains an inspector-only Full History. Immediately
before every successful `ResetContext` rebuild, the backend snapshots the
outgoing structured messages, Kmap snapshot, and usage state as one completed
context segment. These segments are durable UI history only: they are never
restored into the active Chatend, formatted into generation input, or included
when history ingress extracts the archived conversation's model-readable final
Chatend.

It loads durable summaries for the conversation-history sidebar and hydrates
the selected record. The browser keeps only an independent in-memory composer
draft per web conversation. The user may switch freely among active sessions
or create a new one while Kennedy is working elsewhere. Closed records are
read-only. Draft text is not durable until submitted as a backend command.

## 5. Chatend Model

The Chatend is the canonical human-readable application prompt supplied to
Kennedy. The Full inspector and generation path call the same formatter over
the same current message list. Consequently the Full view shows the text sent
for a fresh Codex thread exactly—role labels, separators, and content included;
there is no hidden application-side JSON envelope or differently formatted
prompt. This exactness is scoped to application-controlled plaintext. Codex or
its upstream provider may add forced system content or structured tool metadata
downstream; the application minimizes everything its environment exposes and
does not claim those inaccessible layers appear in the inspector. Provider
thread IDs and runtime protocol data are not Chatend content.
It contains:

- the composed system prompt,
- the clean conversation transcript,
- the current Kweb context,
- transparent text tool requests and readable tool results that remain in
  context.

Canonical formatting labels system messages `System context`, user messages
`David`, and assistant messages `Kennedy` unless a message supplies a more
specific visible role. Nonempty messages are separated by
`────────────────────────`. The backend submits the entire formatted Chatend
when starting a Codex thread. Later requests use `previous_response_id` as the
Codex thread ID and submit the canonically formatted newly appended suffix;
the preceding Chatend is already in that provider thread. System instructions
are prose sections, Kmap context is YAML-like text, tool requests are ordinary
assistant text, and local tool results are readable memory updates.
If the backend rejects a continuation because its thread predates the current
verified prompt boundary, the backend clears that continuation and immediately
replays the complete visible Chatend into a fresh thread.

The clean transcript is maintained separately for the uncluttered conversation
panel. Conversation provenance stores the complete versioned recovery archive
rather than only that transcript. Before upload, the client copies the archive,
moves every top-level media `dataUrl` into a multipart artifact that preserves
its filename and content type, and replaces it with
`provenanceArtifactIndex`. It never mutates the live recovery archive. The
Kweb client retries one ambiguous multipart failure with the same FormData and
`IdempotencyId`, so the server receives identical bytes. During history ingress the backend parses
the archive and canonically formats its `messages` array as the `Archived
Chatend`; the archive object itself, media data URLs, counters, usage,
diagnostics, and other recovery fields are not sent to Kennedy.

### 5.1 Context Rebuild

The backend can rebuild a Chatend from:

1. the active system-prompt manuals,
2. session content that must survive reset—the clean conversation transcript
   for conversation sessions or provenance data for history ingress—plus any
   notes to self retained by earlier resets,
3. one compact history of every successful ResetContext call,
   grouped by retained node-name set and annotated with the shared context-load
   budget position at the latest reset,
4. the current reset's optional note to self,
5. freshly materialized Kweb context, with mandatory roots before explicitly
   requested nodes and active-connection expansions after direct loads.

`ResetContext` first resolves all supplied short identifiers to durable IDs.
It then clears loaded Kweb data while preserving the session's short-ID
assignments and allocation counter, reloads every session root in its declared
order followed by the supplied nodes, and rebuilds the chatend. When a
`selfMessage` is supplied, the backend retains it as an assistant-role note
immediately before the new Kweb context. Before that note, it places the compact
ResetContext history. Node names keep that history readable; repeated node sets
are collapsed into counted lines. Previous
Kweb context and other tool activity are omitted. The clean transcript or
provenance input and notes from prior resets remain.
During an active tool loop, the rebuilt chatend ends with the assistant's
visible ResetContext request and a compact success result. The result does not
repeat the newly loaded nodes: they already appear once in the rebuilt Kmap
context, immediately after the optional note to self. Reset abandons the old
`previous_response_id` thread and submits the rebuilt chatend as a fresh
request. Because the Full inspector is formatted from that same rebuilt message
list, removed nodes and tool results disappear from both the inspector and
Kennedy's next fresh-thread prompt.

### 5.2 Continuation, caching, and context growth

Conversation and ingress retain separate deterministic cache-key fields in the
frontend/backend contract, but the Codex runtime manages caching through its
threads and does not consume those keys. Append-only generations continue with
the latest Codex thread ID, keeping unchanged prefixes eligible for Codex cache
reads.

The backend aggregates provider-reported input, output, reasoning, cache-read,
and cache-write tokens. Codex reports cumulative thread usage, so continuation
rounds are differenced before per-call and session totals are updated. Current
context occupancy is the latest successful model request's input plus output
tokens; explicit `last_input_tokens` and `last_output_tokens` are preferred,
with the already-differenced per-call usage used as the fallback. Remaining
capacity uses the effective context window that Codex advertises for the
selected model. These figures are informative and never trigger compaction,
truncation, or an automatic reset. Every Codex invocation suppresses automatic
compaction; only an explicit ResetContext tool request rebuilds context.

Every generation request ends with exactly one terse context clue:
`context window usage: {used-or-unknown} / {advertised-effective-limit}`.
The numbers use thousands separators and no token labels, percentages,
remaining-token prose, or other explanation. `unknown` appears only before the
session has received any successful LLM usage report. Starting a fresh provider
thread, restoring a session, or calling `ResetContext` preserves the most recent
measurement until the next successful LLM response replaces it. The Full
inspector uses the same formatter and line. The measurement therefore does not
include messages, tool results, or loaded nodes appended since that response.

The backend also measures wall-clock latency at the orchestration boundary. Each LLM
response is followed by one compact timing line, every tool result includes one
duration line, and each completed turn ends with one line containing total time
and combined LLM/tool time. It does not repeat the calls in a summary step list.
These entries are persisted in the Chatend for later model turns.

## 6. Context Glue

The Kmap API returns complete nodes whose fixed and recent connections are
ordered durable-ID arrays, plus an additive name-and-description summary for
every unique referenced node. The backend joins those summaries onto all
fixed and recent references, treats the first eight recent IDs as active, the
remainder as fanout, fetches active node bodies as needed, and converts every
node exposed to Kennedy into an in-context node:

```json
{
  "identifier": 3,
  "shortName": "Example Node",
  "shortDescription": "Short description.",
  "longDescription": "Long description.",
  "lastModifiedBy": "gpt-5.6-sol-xhigh",
  "lastModifiedAt": "2026-07-18T00:00:00Z",
  "ownerIdentifier": 1,
  "fixedConnections": [
    {"identifier": 5, "shortName": "Pinned Memory", "slot": 1}
  ],
  "activeConnections": [
    {"identifier": 4, "shortName": "Related Node"}
  ],
  "fanoutConnections": []
}
```

`ownerIdentifier` is a root-node short identifier, or the literal `unowned`
when a database row deliberately has no owner.

Short identifiers are positive integers allocated in first-seen order. A
durable node receives one short identifier per session, even if it appears in
multiple LoadNode results or is unloaded by ResetContext. The complete mapping
and next allocation value are checkpointed with the session; resetting context
does not recycle or reassign an identifier. Durable IDs are never included in
LLM-visible tool results or prompt context.

`lastModifiedBy` is backend-owned metadata identifying the model and thinking
mode responsible for the node's latest mutation. It is shown in readable Kmap
context and the memory UI. It is not editable by Kennedy.

`fullNodeIds` distinguishes nodes whose complete long description is in context
from nodes that appear only as connection summaries. Summary-only nodes still
receive short identifiers so Kennedy can load them, but they are not treated as
fully materialized knowledge.

A directly loaded node is one for which the backend executed the LoadNode
operation. Active connections returned alongside it are in context and receive
short identifiers, but do not count toward the ten-directly-loaded-node limit.

The structured snapshot above remains available to frontend rendering and
recovery, but the canonical model-readable Kmap text is a compact projection:

1. Directly loaded nodes retain short and long descriptions. Their task,
   active, and fanout edges contain identifiers only.
2. Full active-connection nodes are emitted once globally with their names and
   long descriptions; short descriptions are omitted. Their task, active, and
   fanout edges also contain identifiers only.
3. Non-full fanout nodes directly referenced by any directly loaded node are
   emitted once with identifier, name, and short description.
4. Non-full fanout nodes referenced only by full active-connection nodes are
   emitted once with identifier and name. Nodes already represented in full or
   through a direct-node connection are not repeated in this fourth tier.

The same projection formats LoadNode results. The context glue derives which
fanout references were already visible from the current graph, omits repeated
references, and emits a richer direct-fanout summary if a name-only indirect
fanout later becomes a direct fanout.

Every declared root is directly loaded at session start, survives every reset,
and counts toward the shared limit. Web and private Telegram sessions declare
the user root followed by Kennedy's root. Group invocations insert the group
root between them; background group ingress declares the group root followed by
Kennedy's root. In a fresh or reset context, roots are loaded first in their
declared order.

## 7. Prompt Composition

The backend fetches composable prompt assets from
`/system-prompts/{filename}`. It assembles every session in this order:

1. `KennedyIdentity.txt`,
2. exactly one of `ConversationSession.txt`, `SelfTimeSession.txt`,
   `HistoryIngressSession.txt`, or `AudioIngressSession.txt`,
3. `KmapBasics.txt`,
4. `ReadTools.txt`, containing the Kmap and web read-only tools,
5. `WriteTools.txt` only for history ingress, audio ingress, and self time,
6. `CodexHarness.txt` only when the backend-selected provider kind is `codex`,
7. a dynamic runtime section with the configured model and thinking mode.

The backend selects its inference provider from the intelligence backend's
provider metadata before composing the prompt, so this condition uses the same
provider choice sent with generation requests rather than inferring transport
from a model name. The Codex-only layer concisely explains that Kennedy runs in
an outer harness which catches her ordinary-text tool calls even if the inner
Codex wrapper claims its APIs or tools are limited.

The selected session asset contains only session purpose, mutability, and the
context-loading budget. `KmapBasics.txt` is the single source for identifier
lifetime, automatic roots, the exclusive text tool-call protocol, and the fact
that additional tools and documentation may be found in the Kmap. Each tool
contract likewise has one source asset. The composer adds short readable
headings and channel/source details but does not add XML wrappers, JSON
serialization, or duplicate behavioral instructions. Kmap usage strategy and
learned judgment belong in Kennedy's graph rather than static prompts.

Runtime values come from intelligence-provider metadata, so restored sessions
receive the identity that will actually execute them.

## 8. Agent Tools

All baseline tool names and argument shapes live in the composable tool assets;
the request protocol lives once in `KmapBasics.txt`. No provider-native
function or custom-tool definitions are sent.

For every mutating Kmap request, the tool executor automatically adds
`model_attribution` using the active configured model and reasoning effort.
This transport field never appears in Kennedy's tool schema, request envelope,
or model-controlled arguments.

Kennedy requests tools using an ordinary assistant response:

```text
KENNEDY_TOOL_CALLS
{"calls":[{"name":"LoadNode","arguments":{"identifier":3}}]}
```

The first envelope must contain the marker and an object with one non-empty
`calls` array. Each call has exactly `name` and object-valued `arguments`.
Multiple calls in that array are allowed and execute sequentially before the
next generation request. `ResetContext` and `EndTurn` must each be the only
call in their envelope.
The marker must be the first response text; Markdown fences, commentary, and
status text before it remain invalid. After the first valid JSON object's
closing brace, the backend truncates every trailing character without reading
or preserving it. This includes commentary, final-answer text, and later
`KENNEDY_TOOL_CALLS` blocks, so only the first valid envelope is considered.
Malformed first envelopes and text before the marker still produce readable
protocol feedback so Kennedy can retry correctly.

### 8.1 `LoadNode`

Text-protocol arguments:

```json
{
  "identifier": 3
}
```

Execution:

1. Consume one call from the shared LoadNode/ResetContext context-loading
   budget and reject the call if that budget is exhausted.
2. Reject the call if ten nodes are already directly loaded.
3. Resolve the short identifier.
4. Fetch `GET /api/v1/kmap/nodes/{durable_id}`, treat the first eight recent
   IDs as active, and fetch those full node bodies.
5. Add the requested durable ID to the directly loaded set.
6. Mark the requested node and returned active-connection nodes as full.
7. Assign short IDs to every newly seen node or connection summary.
8. Convert newly full payloads to the compact role-based projection and return
   them as the tool result. Omit active-connection nodes that were already full
   before the call. Deduplicate direct and indirect fanout references against
   the current context. If the requested node was already full through an
   earlier active expansion, report its identifier without repeating its full
   body; the call still makes it directly loaded, upgrades newly direct fanout
   summaries where needed, and returns any newly full active connections.

Every model-requested LoadNode or ResetContext invocation consumes one call from
the shared session or turn budget, including failed calls after basic argument
validation. Loads performed internally while starting or resetting a context
do not consume additional calls. Conversation turns receive 20 shared calls;
history-ingress sessions receive 50.

### 8.2 `ResetContext`

Text-protocol arguments:

```json
{
  "identifiers": [3, 8],
  "selfMessage": "Ideas I need after this reset."
}
```

The backend session resolves the identifiers, clears loaded node material while
preserving every existing session-local identifier assignment, then reloads all
roots in their declared order followed by the supplied nodes in their given
order. No root may appear in the argument list, and the resulting direct-load
set must not exceed ten nodes. A private/web session has two roots and therefore
accepts at most eight IDs; a group invocation has three roots and accepts at
most seven.

`selfMessage` is optional. When present, it must be a non-empty string of at
most 400,000 Unicode characters. A successful reset adds it to retained session
content as an assistant-role note to self. The rebuilt order is prior retained
history (including prior reset notes), compact ResetContext history, the latest
note, the mandatory roots, the explicitly requested nodes, and then any
active-connection expansions.

Reset consumes one call from and preserves the current shared context-loading
counter. A successful reset records the supplied nodes' names, the counter
position, and limit. The Chatend keeps every successful entry and groups
identical name sets irrespective of argument order. Its tool
result is only a compact acknowledgment because the rebuilt Chatend already
contains the complete newly loaded Kweb context.

### 8.3 `ConnectNodes`

Available only during history ingress. Live conversation execution rejects it
even if an older prompt or malformed model response requests it.

LLM schema:

```json
{
  "identifiers": [2, 3, 8]
}
```

Every identifier must refer to a node whose full payload is in context. This
includes directly loaded nodes, their full active-connection expansions, and
full nodes returned by create or update operations; it excludes summary-only
connection references. The backend resolves the IDs. For each node in order
it prepends every peer to that node's recent-ID array and sends the complete
replacement node to `PUT /api/v1/kmap/nodes/{durable_id}`. Updated nodes
refresh matching context records. The tool result reports their in-context
shapes. The backend adds current model attribution to each request; it is not
part of Kennedy's arguments. It also generates a distinct random 16-byte
idempotency identifier for every node update and reuses the request unchanged
for an ambiguous network retry. Individual calls are therefore idempotent;
the multi-node sequence remains intentionally non-atomic and is not a single
composite operation.

### 8.4 `ConsolidateFanout`

Available only during history ingress.

```json
{
  "parentIdentifier": 2,
  "aggregatorIdentifier": 3,
  "fanoutIdentifiers": [8, 9]
}
```

The parent and aggregator must be full nodes. The moved identifiers may be
known fanout summaries. The backend verifies that the aggregator and moved
IDs are currently after the first eight parent recent IDs, removes the moved
IDs from the parent, and appends them to the aggregator. It persists the parent
and aggregator through two sequential complete-node updates. Final list
position determines whether each renders as active or fanout. No Kmap
consolidate API exists.

### 8.5 `SetFixedConnection`

Available only during history ingress.

```json
{
  "parentIdentifier": 2,
  "childIdentifier": 3,
  "slot": 1
}
```

The parent and child must be full nodes and slot is 1, 2, or 3. The string
`blank` in `childIdentifier` clears the selected position. The backend edits
the parent's fixed-ID array and persists that parent with
`PUT /api/v1/kmap/nodes/{durable_id}`. Fixed connections are arbitrary
Kennedy-controlled placements with no priority or task meaning. Because
storage is an ID array rather than a sparse slot map, occupied positions remain
contiguous; assigning a position beyond the next available one is rejected.

### 8.6 `CreateNode`

Available only during history ingress.

```json
{
  "parentIdentifiers": [2, 3],
  "ownerIdentifier": 1,
  "shortName": "New Memory",
  "shortDescription": "Short description.",
  "longDescription": "Long description."
}
```

The backend resolves the parents and root owner and calls
`POST /api/v1/kmap/nodes`, supplying the current provenance ID and every parent
as the new node's recent array. It then sequentially prepends the new ID to
every parent's recent array with ordinary node updates. The created node is
assigned a short identifier and marked as full before it is returned to
Kennedy. Creation does not make the node directly loaded unless a later
LoadNode call loads it. The backend supplies current model attribution to
every mutation and refreshes the affected full records.

### 8.7 `UpdateNode`

Available only during history ingress.

```json
{
  "identifier": 3,
  "ownerIdentifier": 1,
  "newShortName": "Updated Memory",
  "newShortDescription": "Updated description.",
  "newLongDescription": "Updated long description."
}
```

The backend resolves the node and root owner and calls
`PUT /api/v1/kmap/nodes/{durable_id}` with the current provenance ID, requested
fields, and the node's complete existing fixed/recent arrays. It refreshes the
in-context representation and returns the updated node. The request also
receives the backend's current model attribution automatically.

### 8.8 `WebSearch`

Available during live conversation, history ingress, and audio ingress.

```json
{
  "question": "What are the strongest current brunch recommendations in El Salvador, and what evidence supports them?",
  "mode": "balanced"
}
```

The model controls the natural-language question and an exact `quality`,
`balanced`, or `fast` mode. `balanced` is the ordinary default; `fast` is for
simple latency-sensitive lookups where reduced research quality is acceptable;
`quality` is for difficult, high-stakes, cross-source, or conflict-resolution
research. Geographic, language, freshness, and source requirements belong in
the question. Concrete provider, model, reasoning effort, search context,
fixed timeout, and query expansion remain intelligence-layer policy. The caller
cannot shorten or extend the profile deadline, and returned sources are not
reduced to a caller-selected count.
The backend calls `POST /api/v1/web/search` with the active provider/model and
returns the normalized research answer and source URLs as a readable Web tool
result. This research request is not part of the conversation's provider
continuation chain.

### 8.9 `WebFetch`

Available during live conversation, history ingress, and audio ingress.

```json
{
  "url": "https://example.com/article"
}
```

The backend calls `POST /api/v1/web/fetch`. It returns final source metadata,
a truncation indicator, and readable page text. Kennedy uses it to inspect a
particular source page-by-page. Fetched content is untrusted evidence, cannot
override system instructions, and may fail when a page is unsafe, binary,
blocked, JavaScript-dependent, or otherwise unsupported.

### 8.10 `ToolCheck`

`ToolCheck` accepts `{}` in every Kennedy execution mode and returns the exact
readable confirmation `Tool calls are working.` without changing session
state. Each newly created or restored session that lacks visible evidence runs
this tool through the real executor, then retains its assistant request and
successful result in the active Chatend. The exchange therefore survives
`ResetContext`. Kennedy may repeat the same real call at any time.

### 8.11 `EndTurn`

`EndTurn` is the only normal model-controlled way to complete a turn and must
appear alone in its tool envelope. Ordinary prose and empty provider responses
remain inside the active loop and receive a durable controller message. In a
browser, private Telegram, or Telegram-group conversation, Kennedy must first
provide the complete normal user response; `EndTurn({})` then releases that
response for durable completion and waits for the next user message. Calling it
before prose fails with `missing_turn_response`. The candidate prose is stored
with the pending Chatend, so a reload between prose and `EndTurn` resumes with
the same response instead of asking Kennedy to regenerate it.

In history and audio ingress, `EndTurn({})` ends the one-turn session and is
checkpointed before the loop returns its control sentinel. In self time it
accepts `{}` or `{"message":"A message for the next self-time session."}`; the
optional non-empty message is limited to 400,000 characters and is forwarded
only if another clean-slate slice can open. With less than five minutes left,
the self-time run ends and no message is forwarded. The old session-specific
end-tool names are not exposed or accepted.

At startup, the backend asks both durable ingress services to release records
carrying the one-time historical repair marker. A release failure is shown as a
repair-specific warning but does not make ordinary conversation history or
audio browsing unavailable; the marked records remain safely quarantined until
the corrected service is reachable.
Every corrected conversation or audio claim also supplies
`completion_protocol: "end-turn-v1"`, allowing the backend to reject
an older backend build before it can consume newly released repair work.

### 8.12 Tool Failures

Unknown tools are never executed. Invalid arguments, exhausted budgets, missing
short IDs, unsafe URLs, and backend failures are returned to Kennedy as failed
tool results with a readable explanation. Memory and web operations have
distinct readable result labels. Machine-readable error codes remain in the
internal diagnostic tool log; readable requests and failures appear in the
chatend visualization.

## 9. Conversation Flow

### 9.1 Start

Browser startup is feature-isolated and read-only except for an explicit
request to create the initial web conversation.

1. Check Kweb and load the two root identifiers. If it succeeds, initialize the
   memory explorer.
2. Check conversation history and load its sidebar records independently.
3. Check audio ingress and load its recording history independently.
4. Check Telegram and intelligence only to expose feature status and local
   browser media capabilities.
5. Select the most recently updated active web record, or submit an idempotent
   `conversation` start intent if none exists.
6. Poll summaries and hydrate the selected conversation/audio record so the UI
   follows backend checkpoints.

Backend startup independently checks prompt/model dependencies, releases
repair claims, removes truly abandoned legacy placeholders, restores every
active session, polls Telegram, and starts its Kmap-writer scheduler. Those
operations continue with no browser open.

### 9.2 User Turn

1. The frontend submits an idempotent `message` command containing text and
   prepared media metadata, then returns to polling durable state.
2. Conversation History exposes that command only when every earlier command
   for the same conversation is complete; heads for other conversations remain
   independently claimable.
3. The backend appends the user message, resets the per-turn LoadNode counter,
   and checkpoints the pending query with `user_activity: true`. If this fails,
   it does not contact the LLM.
4. The backend submits the canonical plaintext for newly appended messages, the previous
   response ID when available, a stable session-type cache key, and the
   configured model to the intelligence backend. The first request sends the
   complete formatted Chatend.
5. If Kennedy emits a tool envelope, the backend appends the visible assistant text, executes
   every call sequentially in array order, append readable result messages, and
   continue from the returned response ID. State changes from one call are
   visible to the next call in the same response.
   After every complete response-sized tool round, checkpoint the updated full
   recovery archive before requesting another model response.
6. Continue until Kennedy returns final text.
7. The backend appends final text to the clean transcript and Chatend,
   checkpoints the completed turn, and completes the durable command before
   accepting the next command for that conversation.

If generation or a response-sized checkpoint fails, restore the last durable
Chatend, Kmap context, tool log, counters, and usage before allowing a retry,
and abandon the in-memory provider continuation. This prevents a same-process
retry from continuing from tool results or assistant output that the
conversation backend never saved. A cold-start retry follows the same fresh
provider-chain path and sends the pending user query exactly once.

A conversation permits at most 20 model-requested LoadNode calls per user turn.
The UI derives busy/stop/retry state from the durable checkpoint for the entire
tool loop. A stop request marks the processing message/retry command for
cancellation; the backend owns the actual cancellation.

### 9.3 End

Starting a new conversation simply creates another durable active record and
selects it; it does not end or block any existing live conversation. When the
user explicitly ends the selected conversation, the frontend queues an `end`
command. End Conversation remains available while a message is queued,
running, stopped, or failed; retrying an unanswered query is a separate action.
Queueing `end` atomically requests cancellation of every earlier unfinished
message/retry command for that conversation, and the backend then:

1. atomically checkpoint its final state and transition the
   history record from `active` to `ingress_pending`,
2. remove it from the set of continuable sessions but keep the closed record
   selected so its queued, running, and completed ingress state remains visible;
   do not select or create a replacement conversation automatically,
3. serialize the complete versioned recovery archive, including structured
   system, memory, tool, and media-capable message content,
4. let the serialized ingress worker create Kweb provenance using source
   `conversation` and the conversation start time, uploading archived
   media through `POST /api/v1/kmap/provenance-with-artifacts`,
5. store the returned opaque provenance ID while transitioning the history
   record to `ingress_in_progress`,
6. run history ingress using that provenance ID in the background and
   checkpoint its complete Chatend after every tool round,
7. only after success transition the history record to `complete`, then claim
   the next queued record.

`Send & end` uses the same close path, but first appends and checkpoints one
final user message (including prepared attachments) without setting a pending
turn or contacting the intelligence backend. If the close transition fails,
the final message remains durable and the live record remains recoverable; on
success it is visible to history ingress.

An ingress attempt failure is recorded through the conversation-history
backend with its stage, normalized code/message, round count, and measured
context occupancy. The outer worker retries the same logical session at most
four times. The fifth total failure transitions the record to
`ingress_failed`, excludes it from queue selection, releases the serialized
worker for the next record, and leaves its complete diagnostic log visible on
the conversation. These failures do not disable live conversation submission.
A backend `MemoryIngressCoordinator` owns queue selection and the complete provenance,
claim, checkpoint, failure, cancellation, and completion sagas for both
conversation and audio sources. It resumes any already-in-progress source
first, otherwise compares source timestamps, and runs one item at a time. A
process-wide Kmap-writer gate and each backend's unique in-progress invariant
prevent any second Kmap-writing workflow from overlapping it.

An active conversation also moves to `ingress_pending` when it has been idle
for more than 24 hours and the user successfully sends a message in a different
conversation. Merely viewing or typing does not trigger expiry, and Kennedy's
pending response is never timed out.

The selected history row also exposes a confirmed `Purge` escape hatch. It is
available even when an `active` record cannot be reconstructed into a live
session. Purge cancels this tab's current conversation or ingress operation,
then version-checks and permanently deletes the backend record without first
requesting ingress. A deleted record cannot be selected by the history-ingress
queue. Before every conversation-ingress Kmap mutation, the worker also checks
that the record still exists in `ingress_in_progress`, which stops a worker in
another tab from continuing after deletion. Purging an already running or
completed ingress cannot roll back Kmap mutations that happened before purge,
and the confirmation says so.

### 9.4 Autonomous Self Time

The dedicated Self Time category tab has a start panel that accepts 0.1 through
10,080 minutes, defaults to 30, and accepts an optional custom prompt up to
20,000 characters. On the first click it immediately disables the prompt,
duration, and button, labels the button `Starting…`, and shares one in-page
start promise across any repeated click handlers. It submits an idempotent
`free-time` intent containing duration and the trimmed custom prompt. The
backend creates the Conversation History run metadata, absolute deadline,
slice index, and run-level Kmap provenance ID. Conversation History rejects a
second active `free-time` record. The initial autonomous instruction is
checkpointed as a pending turn before generation begins. Its first user-role
message says only which self-time session opened and how much of the shared run
remains. The optional launch prompt follows as its own user-role message in
every clean-slate session, as does a one-session handoff supplied by the
previous session. The composer is hidden for these records, while their
transcript, Chatend, tool traffic, countdown, and history phases remain
inspectable. The live status says `One run · slice N`; history rows use the
launch prompt plus the session number, falling back to `Self time · session N`
when no prompt was supplied.

The backend's single Kmap-writer gate owns the complete run; browser tabs do
not execute or lock it. Only a successful `EndTurn`, the shared deadline, or
its hard-stop grace
finalizes the current record directly into read-only `complete` history without
submitting its Chatend to history ingress: the live self-time session already
performs Kmap memory work with its run-level provenance. An ordinary final
answer remains inside the active Chatend; the controller appends and checkpoints
a user-role continuation notice with the current remaining time, then immediately
runs another model round. A Codex completion with no assistant answer receives a
different continuation notice asking for a concrete answer or Kennedy tool call
and is retried the same way. Neither condition creates a new clean-slate record.

After an explicit end-tool call, if at least five minutes remain before the
persisted deadline, the controller increments the slice index and creates a new
record with a fresh Chatend, context, continuation, counters, and the unchanged
run deadline/provenance. An optional end-tool message is durably promoted to
that next record and removed before any later rollover. With less than five
minutes left, the run ends and no message is forwarded. Backend startup
restores an active pending slice; other provider or checkpoint
failures retry the same durable turn.

Before every model round and immediately after every model response, the
session compares wall-clock time with the deadline. At or below three minutes
it appends and checkpoints one retained `Self time timer` user message. At the
deadline it appends the expiry message, rejects further ordinary tools and
Kmap mutations, and marks the next model request as the final wrap-up round.
Generation and search requests preserve the intelligence backend's
provider/profile timeout, including the longer quality-search allowance, but
the backend supplies the shorter remaining deadline plus two-minute grace
when necessary. A backend timer also aborts and cancels the operation at that
hard stop.
Finalization clears `pendingTurn` and checkpoints the complete slice archive
without waking the normal memory-ingress coordinator.

Conversation History list responses are reconciled with the local cache by
monotonically increasing record version. A delayed list response therefore
cannot overwrite a checkpoint response with an older active-record version;
after a checkpoint conflict the latest server record is adopted before retry.

## 10. History-Ingress Flow

History ingress has a new chatend and context. It does not reuse the ended
conversation's Kweb tool history.

1. Fetch `GET /api/v1/kmap/provenance/{provenance_id}` from the main server.
2. Compose history-ingress instructions.
3. parse the provenance's durable recovery archive and place the canonically
   formatted `messages` text into retained session content under `Archived
   Chatend`; do not place the serialized archive or its non-message fields into
   model context,
4. load the direct roots preserved by the archived session,
5. set the session LoadNode counter to zero,
6. generate with the ingress prompt describing the shared read tools and
   ingress mutation tools,
7. execute tools until Kennedy returns final text,
8. append live requests, results, and completion after the clean transcript in
   the same scroll container,
9. mark the conversation history record complete.

The provenance response contains ordered artifact metadata rather than media
bytes. When a UI needs the original, it fetches the metadata's encoded relative
path below `GET /api/v1/kmap/provenance-artifacts/` and receives the immutable
file as a streamed response. Ordinary history-ingress text does not fetch media.

Prompt composition and every history-ingress mutation use the selected model
and provider-reported reasoning effort. The combined attribution format is
`{model}-{reasoning_effort}`, for example `gpt-5.6-sol-xhigh`. The value is
derived ephemerally by the backend and sent only to the Kweb mutation API; it
is not Chatend state and is never exposed as a model-controlled tool argument.

At most 50 model-requested LoadNode/ResetContext calls are allowed across the
whole ingress session. The agent loop also permits at most 100 model rounds
across the whole logical ingress session. The current round count is
checkpointed before each call and restored after retry; ResetContext starts a
fresh Codex thread but does not reset either session-wide limit. Zero
CreateNode or UpdateNode calls is valid.

Provider, checkpoint, provenance, and completion errors count toward the same
five-attempt outer-session failure allowance. Each retry restores the last
durable ingress archive. Conversation retries are scheduled by the backend
worker after the history service's short durable defer; audio retries use the
recording's durable 15-second next-attempt timestamp. A failed job releases its
durable claim before waiting, allowing the shared worker to select another
eligible conversation or recording. Ingress uses the normal Codex generation
timeout. An input-size rejection stops that record immediately for manual
intervention instead of repeating an identical oversized request five times.

### 10.1 Audio-ingress pieces

The backend's single Kmap-writer gate polls both conversation and audio queues
and also serializes self time and Telegram root provisioning. A claimed item
from either queue completes before the next Kmap-mutating item starts. An
in-progress item has priority; otherwise the oldest source time wins, and audio
pieces with the same source time remain ordered by their persisted piece index.

For audio, the backend creates `audio-vnote` provenance with the
recording-start `source_created_at`; the recording hash and piece index remain
inside the provenance content. The
retained content is the Sol-produced final transcript piece, not Gemini JSON or
audio bytes. Its heading repeats recording time, hash, filename, and piece
position. Prompt composition selects `AudioIngressSession.txt`, then adds the
same Kmap basics, read tools, and write tools used by other sessions. The
audio-provenance context explicitly tells Kennedy that `Created` is historical
recording time. The audio backend stores the same complete ingress Chatend checkpoints,
versions, and five-consecutive-attempt failure history as conversation ingress.
Failed attempts become eligible again after one, five, fifteen, and sixty
minutes. It marks the recording complete only after every chronological piece
completes.

## 11. User Interface

### 11.1 Conversation Panel

The left panel contains only:

- user and Kennedy messages,
- multiline input,
- Send, Send & end, and End Conversation controls,
- a Stop Kennedy control while a response is in flight,
- a Retry Saved Query state when a durable user turn needs to resume,
- per-conversation busy status,
- inline history-ingress activity after a closed conversation,
- visible errors.

Tool calls and internal context never appear in the clean transcript.

A separate left sidebar lists all durable conversations. Each entry derives a
short title from its first user message, shows its phase/date and a clear live
or closed indicator. Selecting a live entry restores its draft and continuable
session; selecting a closed entry opens its clean transcript read-only. New
always creates and selects another durable live conversation.

The selected entry includes a destructive `Purge` control with an explicit
irreversibility confirmation. It deletes the complete conversation record and
its checkpoints, rather than treating deletion as an ordinary End that queues
memory ingress.

Conversation and Telegram sidebars group records automatically: live sessions
first, records whose memory ingress is queued, running, or failed second, and
fully completed records last. Within each group, the most recently updated
record appears first.

The message composer is not rendered while a closed record is selected. Its
textarea, Send, Send & end, and End Conversation controls return whenever the
user selects or creates an active ordinary conversation, including a legacy
pre-backend-orchestration record whose saved ownership marker says `frontend`.

While Kennedy is working, Stop Kennedy and End Conversation remain available
even though Send and Send & end are disabled. Stop aborts the active generation
or web request, prevents another tool-loop round from starting, restores the
latest durable turn checkpoint, and exposes the separate Retry Saved Query
action. End cancels or abandons that unanswered turn and sends the preserved
conversation to ingress without requiring a retry. A user-requested stop is
normal control flow and does not add a red activity-log failure.

In a live conversation, the textarea has a visible larger/compact toggle for
long messages and remains vertically resizable using either its top-edge grip
or the browser's lower-right corner. The top grip also supports the arrow,
Home, and End keys. Manual resizing is allowed up to most of the viewport
rather than being limited to a short box.

History-ingress activity belongs to the conversation record that created it
and is hidden while a live conversation is selected. Selecting an in-progress
or completed record reconstructs its live or archived ingress after the clean
transcript. There is no overlay, second scrollbar, sticky row, or dismissible
panel: the memory-update heading, mutation summary, usage, and activity are
ordinary continuation content in the transcript's scrolling flow. The summary
counts successful `CreateNode`, `UpdateNode`, and `ConnectNodes` calls; failed
attempts do not increment the totals.

### 11.2 Context Inspector

The right panel has three views of Kennedy's current chatend. The Main view is
selected by default and combines the ordinary conversation with progressively
disclosed application context and activity. The Full view uses
the exact same canonical formatter and current messages as generation; it is
not a rendering of the recovery JSON and does not hide application prompt
content. It displays the entire application-controlled plaintext boundary, not
unobservable system/tool scaffolding that Codex or its provider may inject
afterward:

- **Main view** leaves David and Kennedy's ordinary conversation responses
  visible. Kennedy responses longer than 500 Unicode characters show only the
  first 500 followed by an expandable `[...]`; user messages and responses of
  exactly 500 characters or fewer remain fully visible. The system prompt, current loaded-node set, individual directly
  loaded nodes, tool calls, tool results, context notes, and loaded-node events
  are closed disclosure rows by default. Conversation/audio provenance is also
  a closed disclosure row, including in Full History, rather than appearing as
  a potentially enormous user message. Every closed text disclosure reports
  its Unicode-character size; a truncated Kennedy response reports the exact
  number of additional characters expansion will reveal. Timing messages never become separate
  Main-view rows: LLM and tool durations appear as a compact footer on the
  corresponding tool result, while direct conversation responses show their
  LLM and turn timing as small secondary metadata beside the message role.
  Expanding the loaded-node set
  reveals closed rows for directly loaded nodes. A node reveals its details and
  connection summaries; a full active-connection node can be expanded one level
  further, while that node's outgoing connections remain summary-only because
  more distant nodes were not fetched by that load. A successful `LoadNode`
  result adds one closed event row for the requested node only; full active
  connections remain reachable inside that row and are not repeated as sibling
  event rows.
- **Full view** shows system context, conversation, transparent JSON tool
  envelopes, readable tool results, and loaded Kmap context.
- **Full History** renders every completed pre-reset context and the current
  context using Main-view disclosure rules. A prominent `ResetContext` barrier
  separates adjacent context segments. It also separates the closed
  conversation phase from its history-ingress phase, retains reset segments
  from both phases, and follows live ingress checkpoints through their final
  completed or failed state.

Full view continues to show timing messages in their exact Chatend positions;
the timing consolidation is presentation-only in Main and Full History.

While the selected record is actively undergoing history ingress, the Full
and Main inspectors switch to that ingress Chatend so the Full view's final
context-progress line matches the text being sent to Kennedy. The saved ingress
Chatend remains selected after completion or terminal failure rather than
reverting to the source conversation. Before the first ingress checkpoint, the
inspector title reports queued or starting status while the source conversation
remains the latest available Chatend. Full History shows both phases and their
current statuses.

Provider response IDs and credentials are omitted because they are not part of
the application Chatend. Copy View copies the selected view's complete expanded
plaintext content; collapsed rows do not omit their contents from the copy.

The right side of the Chatend header reports current logical context occupancy,
the model context-window size, exact remaining tokens, and the percentage of
input tokens served by prompt-cache reads. Hover details include cumulative
input/output tokens, cache-read tokens, and cache-write tokens. Values come
from provider usage rather than client-side token estimates; before the first
successful provider usage report occupancy is explicitly unmeasured while the
advertised effective limit remains visible. Thereafter the most recent
successful LLM call remains visible across reloads and provider-thread resets
until a newer successful call replaces it.

### 11.3 Audio Ingress History

`Audio Ingress` is a top-level tab with no composer. Its sidebar lists all
durable audio recordings in newest-received order, showing filename,
recording-start date, and current processing or ingress status. Selecting a
recording loads one complete history response containing its metadata, ordered
Gemini chunk transcripts, Sol-produced final transcript, Kennedy transcript
pieces, durable retry failures, and each piece's checkpointed ingress archive.

The center panel uses disclosure rows for the potentially large final
transcript, chunk JSON, and transcript-piece text. All are closed by default,
including the full Sol transcript. Below the transcript pieces, each Kennedy
piece renders the same inline history-ingress continuation used by conversation
and Telegram records: status, retry failures, mutation summary, usage, Kennedy
messages, and collapsible tool traffic. The right inspector also treats each
piece as an ordered Full History phase, retaining the piece's current Chatend
and all pre-reset segments. Preparation records remain visible before Kennedy
ingress begins, and terminal failures remain inspectable. A failed piece has an
always-visible retry action above the collapsed transcript disclosures. Retrying
preserves its old diagnostics, resets its consecutive-failure allowance, and
returns it to the durable queue.

### 11.4 Memory Explorer

The explorer starts at the user root and provides persistent toolbar actions
for jumping directly to either the user root or Kennedy root. It also supports:

- viewing a full knowledge node with `GET /api/v1/kmap/nodes/{node_id}`,
- following task, active, and fanout connections,
- viewing the node's history chain with
  `GET /api/v1/kmap/nodes/{node_id}/history`,
- opening a history entry's source with
  `GET /api/v1/kmap/provenance/{provenance_id}`,
- retrieving an attached immutable source file from
  `GET /api/v1/kmap/provenance-artifacts/{relative_path}` when requested,
- browser-local back and forward navigation.

The explorer does not edit durable data.

## 12. Rendering and Error Requirements

- Insert untrusted text with `textContent`, never `innerHTML`.
- Keep the composer editable while generation, a tool loop, or a pending
  restored query is active so the user can draft their next message. Keep Send
  disabled until Kennedy has responded and the current turn is complete.
- Preserve scroll position, disclosure state, and focused controls across
  background rerenders of the center pane, history sidebar, activity log, and
  inspector while they still show the same record and view. Never replace a
  cached audio record with an intermediate loading screen during refresh.
  Only a reader already at the exact bottom follows newly appended content to
  the new bottom. Explicit user navigation to another record or inspector mode
  may establish a new view position.
- Keep a per-live-conversation draft while switching sessions, and show a clear
  `Live · Continue` versus closed/read-only status in the sidebar.
- Preserve the diagnostic record of failed calls.
- Display backend error messages without exposing stack traces. Append
  user-visible operational errors to a timestamped, deduplicated activity log
  below the history sidebar; never insert an unrelated operational error into
  the selected conversation. The user explicitly clears this log.
- Use semantic controls and visible keyboard focus.
- Support `Ctrl+Enter` and `Cmd+Enter` for message submission.
- Use no remote scripts, fonts, stylesheets, or other CDN assets.

## 13. Telegram Sessions, Audio, and Documents

The top navigation exposes `TG Bot` and `Audio Ingress` beside Conversation and
Memory. The Telegram view reuses
the conversation transcript and Chatend inspector but filters the sidebar to
all `telegram*` records and has no message composer: Telegram itself is the
input surface. Each private Telegram user maps to a separate `sessionType:
telegram` Conversation History record and can run in parallel with other users
and UI conversations. Group invocations and background group batches use
`sessionType: telegram-group`. Every record stores direct and referenced root
IDs plus channel metadata. Dynamic prompt composition distinguishes the web UI,
private Telegram, persistent per-user Telegram groups, and group-source history
ingress. Shared prompt assets describe an arbitrary always-loaded root set;
dynamic group context supplies the session-specific root roles and order.

At backend startup the orchestrator reads the user and Kennedy roles from
`GET /api/v1/kmap/roots`, then asks the relay for unprovisioned whitelist
entries and group roots. It maps the configured web handle `taek42` to the user
root and creates every other reserved root through the same single Kmap-writer
gate used by ingress and self time; it marks each directory entry ready
afterward.
User roots start as `User Root`; group roots start as
`Group Root`. This is the only David-specific mapping; neither backend uses a
sentinel David ID. New `/adduser` entries and newly observed groups are
discovered and provisioned by the same backend loop before their events run.

The supervised backend polls the relay's durable
per-private-user/per-group-user head events, binds each event to its
Conversation History ID, runs the normal read-only conversation session, and
returns only Kennedy's final conversational output. Each fetched queue head
runs as an independent in-flight task; the worker keeps polling while long model or tool
work continues, allowing newly available heads from other private users or
group-user pairs to start without violating the relay's per-stream ordering.
The relay's persisted processing start gives each event a 30-minute hard
deadline that survives restarts. On expiry the backend stops the active
conversation turn, asks the relay to complete the event as timed out, and
transitions a saved pending conversation into history ingress; a best-effort
Telegram notice explains that the request was stopped. If an event points to a
Conversation History record that is missing or no longer active, the backend
creates a replacement and compare-and-swap rebinds from the exact stale ID,
preserving already stored voice transcription and media and restarting the
deadline for the recovered attempt.
It also polls durable passive group
context ranges and checkpoints them without entering the model loop. Event IDs are stored on user and
assistant transcript items so reload can resume generation or retry delivery
without regenerating a completed answer. A `/reset` event closes that user's
active record, queues its full archive for history ingress, and leaves creation
of the next Telegram record until the next message. History-ingress prompt
composition and provenance source identify the source as Telegram or UI.

A group event reuses the active Conversation History record keyed by the stable
group root and invoking Telegram user. Its direct roots are that user's
directory root, the group's reserved root, and Kennedy's root in that order.
The backend session assigns short identifiers to every other participant root without
loading it. The first invocation includes up to 50 recent messages. Later,
every accepted group message is appended to every open group-user session's
Chatend through an independent durable cursor, without triggering Kennedy. The
passive stream includes other users' text, voice notes, documents, and Kennedy
replies from other sessions. Voice and document media are prepared once, their
readable text is reused across sessions, and durable media references remain in
each archive. A session excludes its own invocation and response from the
passive copy because those are already in its transcript. An invocation catches
up before generation. `/reset` catches up through the reset event, then closes
and queues only that group-user record. Group binding never updates a private-DM
or another group-user session pointer.

If more than 50 group messages occur after a user's last invocation, the relay
detaches that session and queues a silent reset. The bridge checkpoints the
remaining range and transitions the old record to history ingress without any
Telegram output. The user's next invocation starts a fresh group session.

The backend also polls durable 80-message group-ingress batches. It creates a
recoverable `telegram-group` archive with the group root followed by Kennedy's
root direct, registers all participant roots as references, and immediately
queues it in Conversation History. There is no user root because nobody invoked
Kennedy. A batch is acknowledged to the relay only after Conversation History
reaches `complete`; a reload finds the batch ID in opaque record state and does
not create duplicate work.

The normal composer includes a microphone control implemented with
`MediaRecorder`. Both browser recordings and Telegram voice notes are sent as
multipart data to the intelligence backend only when provider metadata lacks
the `audio` input modality. The paid transcription is editable in the normal UI
and clearly labeled inside the Chatend. Original bytes, content type, size, and
transcription model are retained in the archive; Telegram originals also
remain in the relay. History provenance retains those originals, but its
model-facing history-ingress text replaces base64 audio with bounded metadata
because this transport cannot consume it as audio. For a Telegram turn,
crossing a new 100,000-token current-context band attaches a separate delivery
warning to the answer. A group warning names the relevant `@username` and says
that other members have separate sessions. The warning
is sent after the answer and never inserted into the Chatend.

The normal composer also accepts up to five PDF, DOCX, XLSX/XLS/XLSB/ODS,
CSV/TSV, or plain-text-family files totaling at most 20 MiB. The intelligence
backend converts each file locally to at most 1,000,000 readable characters;
the user may send attachments without typing a message. An explicit `Upload
PDF` button opens this document picker. Private and invoked-group Telegram
document events use the same conversion path and may include a caption. Group
voice notes invoke by replying to Kennedy; group documents invoke by a bot
mention in the caption or by reply. Searchable PDFs
are supported; a scanned or image-only PDF with no extractable text returns an
explicit OCR-required error. Original bytes and file metadata are archived,
while extracted text enters the Chatend once and is not duplicated in media.

In the inline history-ingress review, verbose Kennedy tool requests, memory
tool results, and tool-protocol errors use closed disclosure controls by
default and report their character size. The user can expand and collapse each entry; Kennedy's ordinary final
review remains visible.

History ingress does not replay full Kmap node bodies from the archived source
Chatend. It removes the archived Kmap-context and memory-tool-result bodies and
their matching memory-only tool requests, drops source-session timing noise,
and supplies a deduplicated list containing only each encountered node's title
and the detail that was model-visible in the source session. Ordinary and
direct-fanout references retain their short descriptions; indirect fanouts
that were name-only remain name-only. Kennedy's live ingress Kmap context and
tools remain unchanged, so she can navigate and load relevant nodes herself.

A terminal conversation-history ingress exposes Retry beside its failed status
in the history column and in an action panel at the top of the central view.
Audio recordings expose the equivalent history-column action in addition to
their per-piece central retry controls. Retry preserves provenance and failure
diagnostics, removes the exhausted history-ingress checkpoint, and returns the
work to the durable queue with a fresh model context.

## 14. Verification

Frontend tests may run directly in a browser or with lightweight Rust-served
fixtures; they must not introduce a production build step. At minimum verify:

- stable short-ID assignment and reset,
- two-root and three-root initialization and reset survival,
- ten-direct-load enforcement,
- conversation and ingress shared LoadNode/ResetContext budgets,
- parsing and sequential execution of multiple text tool calls from one round,
- canonical plaintext identity between generation requests and the Full
  inspector, including archived Chatend ingress without recovery JSON,
- continuation requests sending only canonically formatted newly appended
  messages,
- cache read/write and context-window telemetry aggregation,
- short-to-durable translation for every tool,
- chatend rebuilding after ResetContext,
- durable inspector-only pre-reset Chatend, Kmap, and usage segments with
  ordered Full History barriers across conversation and history ingress,
- optional ResetContext note retention, ordering, and 400,000-character cap,
- compact, complete, restorable ResetContext history with duplicate grouping,
- non-resetting 100-round history-ingress safety across checkpoints and retries,
- model-visible final context progress using exact previous-response usage and
  fresh-thread stale-usage clearing,
- durable five-attempt ingress failure logs and terminal queue advancement,
- clean-transcript serialization,
- checkpoint-before-generation ordering and pending-query recovery,
- lossless full-Chatend persistence, including structured media content,
- response-sized tool-round checkpoints and exact Chatend recovery,
- unrestricted new-conversation creation while other sessions remain live,
- explicit `New`-only conversation creation after ending a selected chat, with
  that closed record retained as the live history-ingress selection,
- refresh-time deletion of conversations that never received a user message,
- editable next-request drafting during Kennedy work and background ingress,
- conversation-history titles and read-only transcript selection,
- per-conversation live and archived history-ingress activity,
- direct self-time archival without duplicate history ingress,
- HTML escaping,
- recovery from failed intelligence, Kweb, and conversation-history requests.
- explicit conversation/Telegram prompt labels and source-aware history ingress,
- per-user/group direct-root persistence, TOFU-root provisioning, independent
  three-root group sessions with referenced participant roots, and durable
  two-root 80-message group ingress,
- paid multipart transcription and original-media persistence,
- durable Telegram event correlation, reset, and context-warning delivery.
