# Frontend Specification

## 1. Scope

The frontend is a browser-native HTML, CSS, and JavaScript application. It has
no Node.js dependency, package manager, bundler, TypeScript, or compile step.
The Kweb backend serves it on localhost.

The frontend owns Kennedy's UI and live orchestration: the clean
transcript, the complete chatend, context glue, prompt composition, agent tool
loops, recovery coordination, and the memory explorer. It does not own durable
data or backend mutation rules.

## 2. Source Layout

```text
Frontend/
  Specification.md
  SystemPrompts/
    KmapAgentManual.txt
    ConversationAgentManual.txt
    HistoryIngressAgentManual.txt
  public/
    index.html
    css/
      styles.css
    js/
      api.js
      app.js
      chatend.js
      conversation.js
      history_ingress.js
      intelligence.js
      kweb_context.js
      memory_explorer.js
      prompt_composer.js
      render.js
      tools.js
```

Files under `public/js` are browser-native ES modules. Module boundaries may
change during implementation, but UI rendering, API transport, chatend state,
and tool execution must remain separate concerns.

## 3. Backend Addresses

The frontend defaults to:

- Kweb API and static origin: `http://127.0.0.1:4321`
- Intelligence API: `http://127.0.0.1:4322`
- Conversation history API: `http://127.0.0.1:4323`

The values are defined once in `app.js` so alternate local ports do not require
changes throughout the codebase.

## 4. Application State

Live application state is held in JavaScript memory:

```js
{
  selectedConversationId: null,
  historyRecords: [],
  liveSessions: new Map(), // durable ID -> independent ConversationSession
  drafts: new Map(),       // durable ID -> unsent composer text
  ingressWorker: {
    running: false,
    activeRecord: null,
    diagnostic: null
  },
  explorer: {
    currentNodeId: null,
    back: [],
    forward: []
  }
}
```

Each `ConversationSession` owns its clean transcript, complete Chatend, Kweb
context and short-ID maps, LoadNode counter, tool log, usage, continuation,
start time, pending-turn flag, and busy state.

The frontend does not use local storage, IndexedDB, cookies, or service-worker
caches for persistence. Instead it checkpoints an opaque recovery snapshot to
the conversation history API. The versioned snapshot contains recovery fields
plus a lossless JSON Chatend archive: composed system prompt, retained content,
structured messages, structured Kmap snapshot and durable-ID diagnostics, tool
log and counters, usage telemetry, and a media collection reserved for future
serializable image/audio/video payloads or attachment references. Provider
response IDs and credentials are transport details and are not archived.

The archive never projects message `content` to text. Arrays and objects are
preserved recursively, so future multimodal content blocks survive storage
even before every inspector has a renderer for that media type. Active records
restore the exact archived Chatend on a fresh Codex thread. Legacy
transcript-only snapshots remain readable and recover through the old rebuild
path.

It loads all durable records for the conversation-history sidebar and keeps a
`ConversationSession` plus an independent in-memory composer draft for every
active record. The user may switch freely among active sessions or create a
new one while Kennedy is working elsewhere. Closed records are read-only.
Draft text is not durable until submitted.

## 5. Chatend Model

The chatend is the complete human-readable logical context Kennedy has formed.
It contains:

- the composed system prompt,
- the clean conversation transcript,
- the current Kweb context,
- transparent text tool requests and readable tool results that remain in
  context.

The frontend submits the entire chatend when starting a Codex thread. Later
requests use `previous_response_id` as the Codex thread ID and submit only text
appended after the referenced response. The frontend still retains and displays the
whole logical chatend. System instructions are prose sections, Kmap context is
YAML-like text, tool requests are ordinary assistant text, and local tool
results are readable memory updates.

The clean transcript is maintained separately for the uncluttered conversation
panel. Conversation provenance is created from the complete versioned Chatend
archive rather than from that transcript.

### 5.1 Context Rebuild

The frontend can rebuild a chatend from:

1. the active system-prompt manuals,
2. session content that must survive reset—the clean conversation transcript
   for conversation sessions or provenance data for history ingress,
3. freshly materialized Kweb context.

`ResetContext` first resolves all supplied short identifiers to durable IDs.
It then clears loaded Kweb data and short-ID mappings, reloads the user root and
the supplied nodes, and rebuilds the chatend. Previous Kweb context and tool
activity are omitted. The clean transcript or provenance input remains.
During an active tool loop, the rebuilt chatend ends with the assistant's
visible ResetContext request and a readable result containing the newly loaded
context. Reset abandons the old `previous_response_id` thread and submits the
rebuilt chatend as a fresh request.

### 5.2 Continuation, caching, and context growth

Conversation and ingress retain separate deterministic cache-key fields in the
frontend/backend contract, but the Codex runtime manages caching through its
threads and does not consume those keys. Append-only generations continue with
the latest Codex thread ID, keeping unchanged prefixes eligible for Codex cache
reads.

The frontend aggregates provider-reported input, output, reasoning, cache-read,
and cache-write tokens. Current context occupancy is the latest request's input
plus output tokens; remaining capacity is computed from provider model metadata.
These figures are informative and never trigger compaction, truncation, or an
automatic reset. Only an explicit ResetContext tool request rebuilds context.

## 6. Context Glue

The Kweb API returns durable IDs. The frontend converts every node exposed to
Kennedy into an in-context node:

```json
{
  "identifier": 3,
  "shortName": "Example Node",
  "shortDescription": "Short description.",
  "longDescription": "Long description.",
  "taskConnections": [
    {"identifier": 5, "shortName": "Outstanding Task", "priority": "high"}
  ],
  "activeConnections": [
    {"identifier": 4, "shortName": "Related Node"}
  ],
  "fanoutConnections": []
}
```

Short identifiers are positive integers allocated in first-seen order. A
durable node receives one short identifier per context, even if it appears in
multiple LoadNode results. Durable IDs are never included in LLM-visible tool
results or prompt context.

`fullNodeIds` distinguishes nodes whose complete long description is in context
from nodes that appear only as connection summaries. Summary-only nodes still
receive short identifiers so Kennedy can load them, but they are not treated as
fully materialized knowledge.

A directly loaded node is one for which the frontend executed the LoadNode
operation. Active connections returned alongside it are in context and receive
short identifiers, but do not count toward the seven-loaded-node limit.

The user root is directly loaded at session start, is always the first loaded
node after reset, and counts toward the limit.

## 7. Prompt Composition

The frontend fetches manuals from `/system-prompts/{filename}`.

Conversation instructions are composed, in order, from:

1. `KmapAgentManual.txt`,
2. `ConversationAgentManual.txt`.

History-ingress instructions are composed from:

1. `KmapAgentManual.txt`,
2. `HistoryIngressAgentManual.txt`.

Manual contents are inserted without rewriting. The prompt composer may add
short human-readable section headings and the current context block, but must
not add XML wrappers, JSON serialization, or duplicate behavioral instructions
already present in the manuals.

## 8. Agent Tools

All tool names, argument shapes, usage policy, and the request protocol are
written in the session's system-prompt manual. No provider-native function or
custom-tool definitions are sent.

Kennedy requests tools using an ordinary assistant response:

```text
KENNEDY_TOOL_CALLS
{"calls":[{"name":"LoadNode","arguments":{"identifier":3}}]}
```

The response must contain only the marker and an object with one non-empty
`calls` array. Each call has exactly `name` and object-valued `arguments`.
Multiple calls are allowed and execute sequentially in array order before the
next generation request. `ResetContext` must be the only call in its response.
The marker must be the first response text and the JSON closing brace must be
the final non-whitespace character. Markdown fences, commentary, status
updates, and final-answer text are forbidden before or after the envelope. The
frontend rejects malformed envelopes and distinguishes invalid JSON, text
before the marker, and trailing text after valid JSON in its readable protocol
feedback so Kennedy can retry correctly.

### 8.1 `LoadNode`

Text-protocol arguments:

```json
{
  "identifier": 3
}
```

Execution:

1. Reject the call if the session's LoadNode call budget is exhausted.
2. Reject the call if seven nodes are already directly loaded.
3. Resolve the short identifier.
4. Call `GET /api/v1/nodes/{durable_id}/context`.
5. Add the requested durable ID to the directly loaded set.
6. Mark the requested node and returned active-connection nodes as full.
7. Assign short IDs to every newly seen node or connection summary.
8. Convert the payload to in-context node shapes and return it as the tool
   result.

Every model-requested LoadNode invocation consumes one call from the session or
turn budget, including failed calls. Loads performed internally while starting
or resetting a context do not consume that budget.

### 8.2 `ResetContext`

Text-protocol arguments:

```json
{
  "identifiers": [3, 8]
}
```

The frontend resolves the identifiers before clearing the old map, then reloads
the root followed by the supplied nodes in their given order. The root must not
also appear in the argument list, and the resulting direct-load set must not
exceed seven nodes.

Reset preserves the current LoadNode counter. Its tool result contains the
complete newly loaded Kweb context.

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
connection references. The frontend resolves the IDs and calls
`POST /api/v1/connections`. Returned nodes refresh matching frontend context
records. The tool result reports the updated in-context node shapes.

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
known fanout summaries. The frontend resolves them and calls
`POST /api/v1/connections/consolidate-fanout`; returned parent and aggregator
nodes refresh the context.

### 8.5 `AssignTask`

Available only during history ingress.

```json
{
  "parentIdentifier": 2,
  "childIdentifier": 3,
  "priority": "high"
}
```

The parent and child must be full nodes and priority is `high`, `medium`, or
`low`. The string `blank` in `childIdentifier` clears the selected slot. The
frontend calls `POST /api/v1/tasks`, refreshes the parent, and reports any
displaced task. Kennedy is instructed to assign a task only when there is a
clear need for concrete work represented by that node to be completed.

### 8.6 `CreateNode`

Available only during history ingress.

```json
{
  "parentIdentifiers": [2, 3],
  "shortName": "New Memory",
  "shortDescription": "Short description.",
  "longDescription": "Long description."
}
```

The frontend resolves the parents and calls `POST /api/v1/nodes`, supplying the
current provenance ID. The created node is assigned a short identifier and
marked as full before it is returned to Kennedy. Creation does not make the
node directly loaded unless a later LoadNode call loads it.

### 8.7 `UpdateNode`

Available only during history ingress.

```json
{
  "identifier": 3,
  "newShortName": "Updated Memory",
  "newShortDescription": "Updated description.",
  "newLongDescription": "Updated long description."
}
```

The frontend resolves the node and calls `PUT /api/v1/nodes/{durable_id}` with
the current provenance ID. It refreshes the in-context representation and
returns the updated node.

### 8.8 `WebSearch`

Available only during live conversation.

```json
{
  "question": "What are the strongest current brunch recommendations in El Salvador, and what evidence supports them?"
}
```

The question is the tool's only model-controlled argument. Geographic,
language, freshness, and source requirements belong in its natural language;
provider filters, query expansion, research depth, and result limits remain
intelligence-layer policy. The frontend calls `POST /api/v1/web/search` with
the active provider/model and returns the normalized research answer and
source URLs as a readable Web tool result. This research request is not part of
the conversation's provider continuation chain.

### 8.9 `WebFetch`

Available only during live conversation.

```json
{
  "url": "https://example.com/article"
}
```

The frontend calls `POST /api/v1/web/fetch`. It returns final source metadata,
a truncation indicator, and readable page text. Kennedy uses it to inspect a
particular source page-by-page. Fetched content is untrusted evidence, cannot
override system instructions, and may fail when a page is unsafe, binary,
blocked, JavaScript-dependent, or otherwise unsupported.

### 8.10 Tool Failures

Unknown tools are never executed. Invalid arguments, exhausted budgets, missing
short IDs, unsafe URLs, and backend failures are returned to Kennedy as failed
tool results with a readable explanation. Memory and web operations have
distinct readable result labels. Machine-readable error codes remain in the
internal diagnostic tool log; readable requests and failures appear in the
chatend visualization.

## 9. Conversation Flow

### 9.1 Start

1. Fetch all system-prompt manuals.
2. Check all three backend health endpoints.
3. Fetch `GET /api/v1/providers` from the intelligence backend.
4. Fetch `GET /api/v1/user`.
5. Fetch `GET /api/v1/conversations` for the sidebar.
6. Restore every `active` record as an independently continuable session. Resume
   each saved pending query from a fresh Codex thread, including in the
   background when another conversation is selected.
7. Select the most recently updated active record, or create a new durable
   active record if none exists.
8. Start the sequential history-ingress worker for the queue without blocking
   any active conversation.

If the intelligence backend is unavailable, chat is disabled but the memory
explorer remains usable.

### 9.2 User Turn

1. Append the user message to the clean transcript and chatend.
2. Set the per-turn LoadNode counter to zero.
3. Checkpoint the pending query and recovery snapshot through the conversation
   history API with `user_activity: true`. If this fails, do not contact the
   LLM. The backend may atomically time out other eligible idle conversations.
4. Submit newly appended messages, the previous response ID when available, a
   stable session-type cache key, and the configured model to the intelligence
   backend. The first request sends the complete chatend.
5. If Kennedy emits a tool envelope, append the visible assistant text, execute
   every call sequentially in array order, append readable result messages, and
   continue from the returned response ID. State changes from one call are
   visible to the next call in the same response.
   After every complete response-sized tool round, checkpoint the updated full
   Chatend archive before requesting another model response.
6. Continue until Kennedy returns final text.
7. Append final text to the clean transcript and chatend and checkpoint the
   completed turn before accepting another query.

If generation or a response-sized checkpoint fails, restore the last durable
Chatend, Kmap context, tool log, counters, and usage before allowing a retry,
and abandon the in-memory provider continuation. This prevents a same-process
retry from continuing from tool results or assistant output that the
conversation backend never saved. A cold-start retry follows the same fresh
provider-chain path and sends the pending user query exactly once.

A conversation permits at most 20 model-requested LoadNode calls per user turn.
The UI remains in a busy state for the entire tool loop.

### 9.3 End

Starting a new conversation simply creates another durable active record and
selects it; it does not end or block any existing live conversation. When the
user explicitly ends the selected conversation:

1. atomically checkpoint its final state and transition the
   history record from `active` to `ingress_pending`,
2. remove it from the set of continuable sessions and select another live
   conversation, or create one if none remains,
3. serialize the complete versioned Chatend archive, including structured
   system, memory, tool, and media-capable message content,
4. let the serialized ingress worker create or retrieve Kweb provenance using
   source `conversation`, the
   conversation start time, and idempotency key `conversation:{conversation_id}`,
5. store the returned opaque provenance ID while transitioning the history
   record to `ingress_in_progress`,
6. run history ingress using that provenance ID in the background and
   checkpoint its complete Chatend after every tool round,
7. only after success transition the history record to `complete`, then claim
   the next queued record.

Any failure leaves the queued record retryable and does not disable live
conversation submission. A same-origin browser lock and the backend's unique
in-progress invariant prevent concurrent tabs from running two ingress workers.

An active conversation also moves to `ingress_pending` when it has been idle
for more than 24 hours and the user successfully sends a message in a different
conversation. Merely viewing or typing does not trigger expiry, and Kennedy's
pending response is never timed out.

## 10. History-Ingress Flow

History ingress has a new chatend and context. It does not reuse the ended
conversation's Kweb tool history.

1. Fetch `GET /api/v1/provenance/{provenance_id}` from the Kweb backend.
2. Compose history-ingress instructions.
3. place the provenance data into the retained session content,
4. load the user root,
5. set the session LoadNode counter to zero,
6. generate with the ingress manual describing the memory navigation and
   mutation tools; WebSearch and WebFetch are unavailable,
7. execute tools until Kennedy returns final text,
8. append live requests, results, and completion after the clean transcript in
   the same scroll container,
9. mark the conversation history record complete.

At most 50 model-requested LoadNode calls are allowed across the whole ingress
session. Zero CreateNode or UpdateNode calls is valid.

## 11. User Interface

### 11.1 Conversation Panel

The left panel contains only:

- user and Kennedy messages,
- multiline input,
- Send and End Conversation controls,
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

History-ingress activity belongs to the conversation record that created it
and is hidden while a live conversation is selected. Selecting an in-progress
or completed record reconstructs its live or archived ingress after the clean
transcript. There is no overlay, second scrollbar, sticky row, or dismissible
panel: the memory-update heading, mutation summary, usage, and activity are
ordinary continuation content in the transcript's scrolling flow. The summary
counts successful `CreateNode`, `UpdateNode`, and `ConnectNodes` calls; failed
attempts do not increment the totals.

### 11.2 Context Inspector

The right panel has four views of Kennedy's current chatend:

- **Full view** shows system context, conversation, transparent JSON tool
  envelopes, readable tool results, and loaded Kmap context.
- **System prompts** shows only the agent manuals and other system
  instructions.
- **Tool calls** shows every transparent tool-request envelope and its readable
  memory or web result currently present in the Chatend, in chronological
  order, while filtering out ordinary conversation and system context.
- **Memory tree** shows the Kmap material currently visible to Kennedy as an
  expandable tree. It distinguishes directly loaded nodes, full nodes pulled
  in through active connections, task connections, and fanout references whose
  summaries alone are visible.

Provider response IDs and credentials are omitted. Copy View copies exactly
the text representation of the selected view.

The right side of the Chatend header reports current logical context occupancy,
the model context-window size, exact remaining tokens, and the percentage of
input tokens served by prompt-cache reads. Hover details include cumulative
input/output tokens, cache-read tokens, and cache-write tokens. Values come
from provider usage rather than client-side token estimates; before the first
provider response the configured model limit supplies the empty-window size.

### 11.3 Memory Explorer

The explorer starts at the user root and supports:

- viewing a full knowledge node with `GET /api/v1/nodes/{node_id}`,
- following task, active, and fanout connections,
- viewing the node's history chain with
  `GET /api/v1/nodes/{node_id}/history`,
- opening a history entry's source with
  `GET /api/v1/provenance/{provenance_id}`,
- browser-local back and forward navigation.

The explorer does not edit durable data.

## 12. Rendering and Error Requirements

- Insert untrusted text with `textContent`, never `innerHTML`.
- Keep the composer editable while generation, a tool loop, or a pending
  restored query is active so the user can draft their next message. Keep Send
  disabled until Kennedy has responded and the current turn is complete.
- Preserve transcript scroll position across rerenders unless the reader was
  already at the bottom; only bottom-following readers track new content.
- Keep a per-live-conversation draft while switching sessions, and show a clear
  `Live · Continue` versus closed/read-only status in the sidebar.
- Preserve the diagnostic record of failed calls.
- Display backend error messages without exposing stack traces.
- Use semantic controls and visible keyboard focus.
- Support `Ctrl+Enter` and `Cmd+Enter` for message submission.
- Use no remote scripts, fonts, stylesheets, or other CDN assets.

## 13. Verification

Frontend tests may run directly in a browser or with lightweight Rust-served
fixtures; they must not introduce a production build step. At minimum verify:

- stable short-ID assignment and reset,
- seven-direct-load enforcement,
- conversation and ingress LoadNode budgets,
- parsing and sequential execution of multiple text tool calls from one round,
- continuation requests sending only newly appended messages,
- cache read/write and context-window telemetry aggregation,
- short-to-durable translation for every tool,
- chatend rebuilding after ResetContext,
- clean-transcript serialization,
- checkpoint-before-generation ordering and pending-query recovery,
- lossless full-Chatend persistence, including structured media content,
- response-sized tool-round checkpoints and exact Chatend recovery,
- unrestricted new-conversation creation while other sessions remain live,
- editable next-request drafting during Kennedy work and background ingress,
- conversation-history titles and read-only transcript selection,
- per-conversation live and archived history-ingress activity,
- HTML escaping,
- recovery from failed intelligence, Kweb, and conversation-history requests.
