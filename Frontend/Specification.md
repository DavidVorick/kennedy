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
    KennedyIdentity.txt
    ConversationManual.txt
    HistoryIngress.txt
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
- Telegram relay API: `http://127.0.0.1:4324`

The values are defined once in `app.js` so alternate local ports do not require
changes throughout the codebase.

## 4. Application State

Live application state is held in JavaScript memory:

```js
{
  activeView: "conversation", // conversation | telegram | memory
  selectedConversationId: null,
  selectedByView: { conversation: null, telegram: null },
  historyRecords: [],
  liveSessions: new Map(), // durable ID -> independent ConversationSession
  drafts: new Map(),       // durable ID -> unsent composer text
  voiceDrafts: new Map(),  // durable ID -> original audio + transcription metadata
  telegramBridge: {
    webLock: "kennedy-telegram-bridge",
    inFlightEventIds: new Set()
  },
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
start time, pending-turn flag, busy state, configured model and reasoning effort,
and their combined model-attribution value.

The frontend does not use local storage, IndexedDB, cookies, or service-worker
caches for persistence. Instead it checkpoints an opaque recovery snapshot to
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
before every successful `ResetContext` rebuild, the frontend snapshots the
outgoing structured messages, Kmap snapshot, and usage state as one completed
context segment. These segments are durable UI history only: they are never
restored into the active Chatend, formatted into generation input, or included
when history ingress extracts the archived conversation's model-readable final
Chatend.

It loads all durable records for the conversation-history sidebar and keeps a
`ConversationSession` plus an independent in-memory composer draft for every
active record. The user may switch freely among active sessions or create a
new one while Kennedy is working elsewhere. Closed records are read-only.
Draft text is not durable until submitted.

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
`────────────────────────`. The frontend submits the entire formatted Chatend
when starting a Codex thread. Later requests use `previous_response_id` as the
Codex thread ID and submit the canonically formatted newly appended suffix;
the preceding Chatend is already in that provider thread. System instructions
are prose sections, Kmap context is YAML-like text, tool requests are ordinary
assistant text, and local tool results are readable memory updates.

The clean transcript is maintained separately for the uncluttered conversation
panel. Conversation provenance stores the complete versioned recovery archive
rather than only that transcript. During history ingress the frontend parses
the archive and canonically formats its `messages` array as the `Archived
Chatend`; the archive object itself, media data URLs, counters, usage,
diagnostics, and other recovery fields are not sent to Kennedy.

### 5.1 Context Rebuild

The frontend can rebuild a chatend from:

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
It then clears loaded Kweb data and short-ID mappings, reloads the user and
Kennedy roots followed by the supplied nodes, and rebuilds the chatend. When a
`selfMessage` is supplied, the frontend retains it as an assistant-role note
immediately before the new Kweb context. Before that note, it places the compact
ResetContext history. Node names are used because short identifiers are rebuilt
by every reset; repeated node sets are collapsed into counted lines. Previous
Kweb context and other tool activity are omitted. The clean transcript or
provenance input and notes from prior resets remain.
During an active tool loop, the rebuilt chatend ends with the assistant's
visible ResetContext request and a readable result containing the newly loaded
context. Reset abandons the old `previous_response_id` thread and submits the
rebuilt chatend as a fresh request. Because the Full inspector is formatted
from that same rebuilt message list, removed nodes and tool results disappear
from both the inspector and Kennedy's next fresh-thread prompt.

### 5.2 Continuation, caching, and context growth

Conversation and ingress retain separate deterministic cache-key fields in the
frontend/backend contract, but the Codex runtime manages caching through its
threads and does not consume those keys. Append-only generations continue with
the latest Codex thread ID, keeping unchanged prefixes eligible for Codex cache
reads.

The frontend aggregates provider-reported input, output, reasoning, cache-read,
and cache-write tokens. Codex reports cumulative thread usage, so continuation
rounds are differenced before per-call and session totals are updated. Current
context occupancy is the latest request's input plus output tokens; remaining
capacity uses the effective context window that Codex advertises for the
selected model. These figures are informative and never trigger compaction,
truncation, or an automatic reset. Every Codex invocation suppresses automatic
compaction; only an explicit ResetContext tool request rebuilds context.

Every generation request ends with exactly one terse context clue:
`context window usage: {used-or-unknown} / {advertised-effective-limit}`.
The numbers use thousands separators and no token labels, percentages,
remaining-token prose, or other explanation. A new, reset, or recovered fresh
thread uses `unknown` rather than presenting usage from the abandoned thread.
The Full inspector uses the same formatter and line. A known measurement comes
from the previous completed response, so it does not include the newly appended
suffix being submitted with it.

The frontend also measures wall-clock latency at the browser boundary. Each LLM
response is followed by one compact timing line, every tool result includes one
duration line, and each completed turn ends with one line containing total time
and combined LLM/tool time. It does not repeat the calls in a summary step list.
These entries are persisted in the Chatend for later model turns.

## 6. Context Glue

The Kweb API returns durable IDs. The frontend converts every node exposed to
Kennedy into an in-context node:

```json
{
  "identifier": 3,
  "shortName": "Example Node",
  "shortDescription": "Short description.",
  "longDescription": "Long description.",
  "lastModifiedBy": "gpt-5.6-sol-xhigh",
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

`lastModifiedBy` is backend-owned metadata identifying the model and thinking
mode responsible for the node's latest mutation. It is shown in readable Kmap
context and the memory UI. It is not editable by Kennedy.

`fullNodeIds` distinguishes nodes whose complete long description is in context
from nodes that appear only as connection summaries. Summary-only nodes still
receive short identifiers so Kennedy can load them, but they are not treated as
fully materialized knowledge.

A directly loaded node is one for which the frontend executed the LoadNode
operation. Active connections returned alongside it are in context and receive
short identifiers, but do not count toward the ten-directly-loaded-node limit.

The user root and Kennedy root are directly loaded at session start, survive
every reset, and both count toward the shared limit. In a fresh or reset
context, they are loaded first in that order. Restoring a legacy one-root
archive automatically materializes the missing Kennedy root and refreshes the
archived system and Kmap context messages before generation.

## 7. Prompt Composition

The frontend fetches manuals from `/system-prompts/{filename}`.

Conversation instructions are composed, in order, from:

1. `KennedyIdentity.txt`,
2. `ConversationManual.txt`.

History-ingress instructions are composed from:

1. `KennedyIdentity.txt`,
2. `HistoryIngress.txt`.

Manual contents are inserted without rewriting. The prompt composer may add
short human-readable section headings and the current context block, but must
not add XML wrappers, JSON serialization, or duplicate behavioral instructions
already present in the manuals. The identity establishes Kennedy's purpose and
Kmap-based learning model. Mode manuals contain only mode mechanics, exact Kmap
facts, and tool contracts; Kmap usage strategy belongs in Kennedy's own graph.
The conversation manual also states the lifecycle fact that the canonical
human-readable Chatend text is retained for read-write history ingress when the
conversation ends, where learned information can be integrated into the Kmap.

After the selected mode manual, the composer adds a short dynamic runtime
section stating the exact configured model and thinking mode. These values come
from intelligence-provider metadata rather than a static manual, so restored
sessions also receive the runtime identity that will actually execute them.

## 8. Agent Tools

All tool names, argument shapes, usage policy, and the request protocol are
written in the session's system-prompt manual. No provider-native function or
custom-tool definitions are sent.

For every mutating Kmap request, the tool executor automatically adds
`model_attribution` using the active configured model and reasoning effort.
This transport field never appears in Kennedy's tool schema, request envelope,
or model-controlled arguments.

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

1. Consume one call from the shared LoadNode/ResetContext context-loading
   budget and reject the call if that budget is exhausted.
2. Reject the call if ten nodes are already directly loaded.
3. Resolve the short identifier.
4. Call `GET /api/v1/nodes/{durable_id}/context`.
5. Add the requested durable ID to the directly loaded set.
6. Mark the requested node and returned active-connection nodes as full.
7. Assign short IDs to every newly seen node or connection summary.
8. Convert the payload to in-context node shapes and return it as the tool
   result.

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

The frontend resolves the identifiers before clearing the old map, then reloads
both roots followed by the supplied nodes in their given order. Neither root
may appear in the argument list, and the resulting direct-load set must not
exceed ten nodes; therefore the argument list contains at most eight IDs.

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
result contains the complete newly loaded Kweb context.

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
records. The tool result reports the updated in-context node shapes. The
frontend adds the current model attribution to the backend request; it is not
part of Kennedy's arguments.

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

The frontend records the current model attribution for the parent, aggregator,
and moved nodes without promoting summary-only nodes to full context.

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

The frontend attributes the parent, assigned child, and displaced child
automatically without adding an argument to Kennedy's tool contract.

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
The frontend supplies the current model attribution automatically and refreshes
the created node and its already-full parents from the response.

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
returns the updated node. The backend request also receives the frontend's
current model attribution automatically.

### 8.8 `WebSearch`

Available only during live conversation.

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
timeout, query expansion, and result limits remain intelligence-layer policy.
The frontend calls `POST /api/v1/web/search` with the active provider/model and
returns the normalized research answer and source URLs as a readable Web tool
result. This research request is not part of the conversation's provider
continuation chain.

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
2. Check all four backend health endpoints.
3. Fetch `GET /api/v1/providers` from the intelligence backend.
   Retain the selected provider's configured reasoning effort alongside its
   model.
4. Fetch `GET /api/v1/user`.
5. Ask the conversation-history backend to permanently discard every record
   that has never received a user message, then fetch
   `GET /api/v1/conversations` for the sidebar. This resets abandoned “New
   conversation” placeholders on every page load without deleting any
   conversation that actually started.
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
4. Submit the canonical plaintext for newly appended messages, the previous
   response ID when available, a stable session-type cache key, and the
   configured model to the intelligence backend. The first request sends the
   complete formatted Chatend.
5. If Kennedy emits a tool envelope, append the visible assistant text, execute
   every call sequentially in array order, append readable result messages, and
   continue from the returned response ID. State changes from one call are
   visible to the next call in the same response.
   After every complete response-sized tool round, checkpoint the updated full
   recovery archive before requesting another model response.
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
2. remove it from the set of continuable sessions but keep the closed record
   selected so its queued, running, and completed ingress state remains visible;
   do not select or create a replacement conversation automatically,
3. serialize the complete versioned recovery archive, including structured
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

An ingress attempt failure is recorded through the conversation-history
backend with its stage, normalized code/message, round count, and measured
context occupancy. The outer worker retries the same logical session at most
four times. The fifth total failure transitions the record to
`ingress_failed`, excludes it from queue selection, releases the serialized
worker for the next record, and leaves its complete diagnostic log visible on
the conversation. These failures do not disable live conversation submission.
A same-origin browser lock and the backend's unique in-progress invariant
prevent concurrent tabs from running two ingress workers.

An active conversation also moves to `ingress_pending` when it has been idle
for more than 24 hours and the user successfully sends a message in a different
conversation. Merely viewing or typing does not trigger expiry, and Kennedy's
pending response is never timed out.

## 10. History-Ingress Flow

History ingress has a new chatend and context. It does not reuse the ended
conversation's Kweb tool history.

1. Fetch `GET /api/v1/provenance/{provenance_id}` from the Kweb backend.
2. Compose history-ingress instructions.
3. parse the provenance's durable recovery archive and place the canonically
   formatted `messages` text into retained session content under `Archived
   Chatend`; do not place the serialized archive or its non-message fields into
   model context,
4. load the user and Kennedy roots,
5. set the session LoadNode counter to zero,
6. generate with the ingress manual describing the memory navigation and
   mutation tools; WebSearch and WebFetch are unavailable,
7. execute tools until Kennedy returns final text,
8. append live requests, results, and completion after the clean transcript in
   the same scroll container,
9. mark the conversation history record complete.

Prompt composition and every history-ingress mutation use the selected model
and provider-reported reasoning effort. The combined attribution format is
`{model}-{reasoning_effort}`, for example `gpt-5.6-sol-xhigh`. The value is
derived ephemerally by the frontend and sent only to the Kweb mutation API; it
is not Chatend state and is never exposed as a model-controlled tool argument.

At most 50 model-requested LoadNode/ResetContext calls are allowed across the
whole ingress session. The agent loop also permits at most 100 model rounds
across the whole logical ingress session. The current round count is
checkpointed before each call and restored after retry; ResetContext starts a
fresh Codex thread but does not reset either session-wide limit. Zero
CreateNode or UpdateNode calls is valid.

Provider, checkpoint, provenance, and completion errors count toward the same
five-attempt outer-session failure allowance. Each retry restores the last
durable ingress archive; reaching the fifth failure aborts instead of scheduling
the record again.

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

The message composer is not rendered while a closed record is selected. Its
textarea, Send control, and End Conversation control return only after the user
selects or creates a live conversation.

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
  are closed disclosure rows by default. Timing messages never become separate
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
provider response occupancy is explicitly unmeasured while the advertised
effective limit remains visible.

### 11.3 Memory Explorer

The explorer starts at the user root and provides persistent toolbar actions
for jumping directly to either the user root or Kennedy root. It also supports:

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

## 13. Telegram Sessions, Audio, and Documents

The top navigation exposes `TG Bot` beside Conversation and Memory. It reuses
the conversation transcript and Chatend inspector but filters the sidebar to
Telegram records and has no message composer: Telegram itself is the input
surface. Each Telegram user maps to a separate `sessionType: telegram`
Conversation History record and can run in parallel with other users and UI
conversations. The record stores channel metadata, while the system prompt
explicitly says `telegram session`. Ordinary browser records explicitly say
`conversation session`.

One browser tab holds the `kennedy-telegram-bridge` Web Lock. It polls the
relay's durable per-user head events, binds each event to its Conversation
History ID, runs the normal read-only conversation session, and returns only
Kennedy's final conversational output. Event IDs are stored on user and
assistant transcript items so reload can resume generation or retry delivery
without regenerating a completed answer. A `/reset` event closes that user's
active record, queues its full archive for history ingress, and leaves creation
of the next Telegram record until the next message. History-ingress prompt
composition and provenance source identify the source as Telegram or UI.

The normal composer includes a microphone control implemented with
`MediaRecorder`. Both browser recordings and Telegram voice notes are sent as
multipart data to the intelligence backend only when provider metadata lacks
the `audio` input modality. The paid transcription is editable in the normal UI
and clearly labeled inside the Chatend. Original bytes, content type, size, and
transcription model are retained in the archive; Telegram originals also
remain in the relay. History provenance retains those originals, but its
model-facing history-ingress text replaces base64 audio with bounded metadata
because this transport cannot consume it as audio. For a Telegram turn, crossing a new 100,000-token current-
context band attaches a separate delivery warning to the answer. The warning
is sent after the answer and never inserted into the Chatend.

The normal composer also accepts up to five PDF, DOCX, XLSX/XLS/XLSB/ODS,
CSV/TSV, or plain-text-family files totaling at most 20 MiB. The intelligence
backend converts each file locally to at most 1,000,000 readable characters;
the user may send attachments without typing a message. An explicit `Upload
PDF` button opens this document picker. Telegram document
events use the same conversion path and may include a caption. Searchable PDFs
are supported; a scanned or image-only PDF with no extractable text returns an
explicit OCR-required error. Original bytes and file metadata are archived,
while extracted text enters the Chatend once and is not duplicated in media.

In the inline history-ingress review, verbose Kennedy tool requests, memory
tool results, and tool-protocol errors use closed disclosure controls by
default. The user can expand and collapse each entry; Kennedy's ordinary final
review remains visible.

## 14. Verification

Frontend tests may run directly in a browser or with lightweight Rust-served
fixtures; they must not introduce a production build step. At minimum verify:

- stable short-ID assignment and reset,
- two-root initialization and reset survival,
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
- HTML escaping,
- recovery from failed intelligence, Kweb, and conversation-history requests.
- explicit conversation/Telegram prompt labels and source-aware history ingress,
- paid multipart transcription and original-media persistence,
- durable Telegram event correlation, reset, and context-warning delivery.
