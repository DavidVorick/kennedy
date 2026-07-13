# Kennedy Technical Design

## 1. Purpose and Authority

Kennedy is a local-first memory application built around the kweb described in
`UserSpecification.md`. The user specification defines product behavior. This
document defines the architecture used to implement that behavior, and the
component specifications define the detailed contracts.

The MVP has four logical runtime components:

1. **Frontend**: a browser-native HTML/CSS/JavaScript application. It owns the
   user interface, the live chatend, short identifiers, prompt
   composition, and agent tool orchestration.
2. **Kweb backend**: a Rust HTTP service. It owns SQLite, durable kweb data, and
   every graph and history invariant.
3. **Intelligence backend**: a Rust HTTP service. It translates a complete LLM
   request into a provider request, performs bounded web research and public
   page retrieval, and normalizes the results. It is stateless between
   requests.
4. **Conversation history backend**: a Rust HTTP service. It durably checkpoints
   active browser conversations and owns the sequential history-ingress queue.

The three backends are independent library crates compiled into one
`kennedy-server` executable. The executable starts their listeners but shares
no router, database, application state, or service handle among them. No
backend crate depends on another backend crate; all coordination happens in the
frontend through their public HTTP APIs.

System-prompt manuals are frontend source assets under `Frontend/SystemPrompts`.

## 2. Design Principles

- The frontend is the single authority for each live session's chatend and
  in-memory draft.
- The Kweb backend is the single authority for durable memory.
- The conversation history backend is the single authority for unfinished and
  completed conversation records.
- Logical backend isolation is preserved even though all three services share
  one deployment binary.
- The intelligence backend never needs to understand the kweb, short
  identifiers, or Kennedy's text-tool protocol.
- The frontend owns the complete human-readable chatend. It sends the full
  chatend when starting a Codex thread and only newly appended text while
  continuing that thread with `previous_response_id`.
- The Codex adapter uses the machine's ChatGPT-authenticated CLI and resumes
  persisted Codex threads. A `ResetContext` call deliberately abandons the old
  thread and sends the rebuilt chatend as a fresh request.
- `ResetContext` rebuilds the chatend from retained session content and newly
  loaded kweb nodes, so unloaded node content is genuinely absent afterward.
- Short identifiers never cross the Kweb backend API boundary. The frontend
  resolves them to durable identifiers before making backend calls.
- All Kweb mutations that affect more than one row are SQLite transactions.
- Behavior absent from `UserSpecification.md` is outside the MVP unless it is
  necessary to realize an explicitly specified behavior.

## 3. Runtime Topology

```text
One kennedy-server process
  ├─ Kweb API :4321 --------------------- kennedy.sqlite3
  │    └─ serves frontend and manuals
  ├─ Intelligence API :4322 ------------ Podman Codex launcher + public web
  └─ Conversation History API :4323 ---- kennedy-conversations.sqlite3

Browser frontend calls all three APIs directly. No backend calls another.
```

Default addresses:

| Component | Address |
| --- | --- |
| Kweb backend | `http://127.0.0.1:4321` |
| Intelligence backend | `http://127.0.0.1:4322` |
| Conversation history backend | `http://127.0.0.1:4323` |

The browser calls all three services directly. Cross-origin backends permit
requests from the Kweb frontend origin. Every listener binds to loopback by
default.

## 4. Ownership Boundaries

### 4.1 Frontend

The frontend owns:

- the clean user/Kennedy transcript,
- the current chatend sent to the LLM,
- directly loaded nodes and their task, active, and fanout connections,
- durable-ID to short-ID mappings,
- conversation and history-ingress call budgets,
- the transparent text tool protocol and tool loops,
- prompt composition from system-prompt manuals,
- the context inspector and memory explorer state,
- checkpoint and recovery orchestration across backend APIs.

The context inspector renders the human-readable chatend, including Kennedy's
text tool requests and readable tool results. It provides full-context,
system-prompt-only, and expandable Kmap-memory views. The memory view derives
node provenance from the Kweb context snapshot so direct loads, task edges,
active-edge expansions, and summary-only fanout references remain visually distinct.
Token, context-window, and cache telemetry is displayed in the Chatend header.
Provider IDs and credentials remain hidden.

The frontend itself has no persistent state. It saves versioned, opaque,
lossless Chatend archives to the conversation history API before generation,
after complete tool rounds, and after final output. Structured content is
preserved for future media support. It reconstructs the exact live session
from that backend after a reload or abrupt close.

### 4.2 Kweb Backend

The Kweb backend owns:

- creation and migration of the SQLite schema,
- the hardcoded MVP user and root node,
- knowledge, provenance, and history nodes,
- connection ordering, promotion, and demotion,
- atomic create, update, and connect operations,
- read APIs used by context loading and the memory explorer,
- serving the frontend and prompt-manual files.

It knows nothing about short identifiers, chatends, LLM messages, session call
budgets, or provider APIs.

### 4.3 Intelligence Backend

The intelligence backend owns:

- validating ChatGPT Codex login and loading model/CLI configuration,
- validating the normalized generation request,
- translating normalized text messages and continuation controls into bounded,
  read-only non-interactive Codex turns,
- translating Codex text, thread IDs, errors, and detailed token usage
  into one response shape,
- executing isolated Codex WebSearch research runs,
- safely fetching and extracting bounded text from public pages for WebFetch.

It stores no local LLM session and never parses Kennedy's tool envelopes. The
normal generation path has no provider tools. The frontend recognizes
WebSearch and WebFetch text calls, invokes the corresponding intelligence API,
then appends their readable results to the main conversation chain. Hosted
search runs are fresh ephemeral Codex threads and cannot alter that chain.

### 4.4 Conversation History Backend

The conversation history backend owns its own SQLite database, opaque frontend
checkpoints, optimistic record versions, and the conversation phase machine:

```text
active -> ingress_pending -> ingress_in_progress -> complete
```

It does not call the Kweb or intelligence APIs or validate their identifiers.
It permits multiple `active` and `ingress_pending` records while enforcing one
`ingress_in_progress` record. A user-activity checkpoint also closes other
active conversations idle for more than 24 hours, unless Kennedy still owes a
response in that record.

## 5. Session Model

### 5.1 Conversation

Each active conversation has its own transcript, chatend, loaded Kmap snapshot,
Codex thread, and composer draft. The frontend can keep several sessions live
and switch or create sessions while Kennedy is working in another. For each
user turn it appends the user message, checkpoints the pending query, and only
then calls the intelligence backend. Conversation tools are read-only with
respect to Kmap: `LoadNode`, `ResetContext`, `WebSearch`, and `WebFetch`.
Only user and Kennedy text is added to the clean transcript.

Ordinary model generations invoke `codex-safe`, which runs
`codex exec --json` inside a persistent Podman sandbox with `gpt-5.6-sol` and
`xhigh`, resuming by Codex thread ID. The child process is bounded by a total
deadline, uses the ChatGPT login, and cannot use shell/file/internet tools. If
generation or a tool-round checkpoint fails, the frontend restores the last
durable Chatend and local execution state and retries the pending turn from a
fresh Codex thread.

The Kweb portion of the chatend accumulates during the conversation. A
`ResetContext` call resolves its arguments, removes all Kweb context, resets
short identifiers, reloads the root and requested nodes, and rebuilds the
chatend while retaining the clean transcript and the current turn's LoadNode
counter.

Explicitly ending a conversation transitions its durable record to
`ingress_pending`; starting a new conversation does not end existing live ones.
A successful user-message checkpoint in another conversation also queues active
records idle for more than 24 hours, except pending Kennedy turns. The frontend
worker processes queued records oldest-activity-first, creates idempotent data
provenance from each complete Chatend archive, transitions exactly one record
to `ingress_in_progress`, and completes it before claiming the next. Live
conversations remain usable throughout. Completed records and both archives
remain queryable from the sidebar.

### 5.2 History Ingress

History ingress uses a separate chatend composed from the Kmap and
HistoryIngress manuals, the provenance data, and the loaded user root node.
Kennedy may navigate the kweb, connect nodes, reorganize fanout, manage task
slots, and create or update knowledge nodes. WebSearch and WebFetch are not
available; ingress must interpret only the archived conversation and Kmap
material in front of it. The current provenance identifier is held by the
frontend and supplied implicitly when it translates CreateNode and UpdateNode
tool calls into Kweb API requests.

The session ends when Kennedy returns final text. Its whole Chatend is
checkpointed on the owning conversation record after each tool round and at
completion. Live tool requests, results, and the completion are shown only
when that record is selected, appended after its clean transcript in the same
scrolling flow; completed records reconstruct that continuation from the saved
archive. Completing with zero knowledge mutations is valid.

At startup every `active` record restores its transcript, directly loaded nodes,
and pending-turn flag. Pending user queries resume from fresh Codex threads.
Queued ingress resumes independently and sequentially without disabling any
live composer.

## 6. Kweb Data Model

SQLite stores exactly the three durable node types from the user specification:

- **Knowledge node**: the current human-readable memory and its connection
  lists, with a pointer to the newest history node.
- **Data provenance node**: immutable source material, its source, and its
  source creation time.
- **Data history node**: an append-only link from one knowledge node to one
  provenance node and the previous history node.

Connections are represented in a relational table as an implementation detail
of knowledge nodes. Each directed connection has an active, fanout, or task
role and a deterministic order. Task priority slots use reserved negative order
values in the existing connection schema, so legacy databases need no migration
and nodes without assigned tasks expose an empty task list. `ConnectNodes`
promotes every ordered pair in the supplied set. If a source exceeds the active
limit, its least recently active connections are demoted unless that would
exceed the fanout limit. `ConsolidateFanout` moves selected fanout references
under an existing aggregator. `AssignTask` replaces or clears one directional
high, medium, or low task slot.

## 7. API Conventions

All three backends expose versioned JSON APIs under `/api/v1` and an
unversioned `GET /health` endpoint.

- Durable identifiers are lowercase hexadecimal encodings of 20 random bytes.
- Timestamps are RFC 3339 UTC strings.
- Requests and responses use `application/json`.
- Successful deletion-style operations, where defined, may return `204`; all
  other successful operations return JSON.
- Errors use one envelope:

```json
{
  "error": {
    "code": "machine_readable_code",
    "message": "Human-readable explanation."
  }
}
```

The detailed endpoints and payloads are defined only in the owning component's
specification.

## 8. Repository Layout

```text
UserSpecification.md
TechnicalDesign.md
BackendKweb/
  Specification.md
ConversationHistory/
  Specification.md
Frontend/
  Specification.md
  SystemPrompts/
    KmapAgentManual.txt
    ConversationAgentManual.txt
    HistoryIngressAgentManual.txt
  public/
IntelligenceBackend/
  Specification.md
KennedyServer/
  src/main.rs
```

Implementation code belongs under its owning component directory.

## 9. MVP Non-Goals

- Multiple users or access control.
- Network deployment beyond the local machine.
- Manual knowledge editing, deletion, or fanout pruning.
- Self-action sessions.
- Streaming generation.
- Provider-side conversation persistence.
- A frontend build system, Node.js, or TypeScript.
