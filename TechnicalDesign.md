# Kennedy Technical Design

## 1. Purpose and Authority

Kennedy is a local-first memory application built around the kweb described in
`UserSpecification.md`. The user specification defines product behavior. This
document defines the architecture used to implement that behavior, and the
component specifications define the detailed contracts.

The MVP has five logical runtime components:

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
5. **Telegram relay**: a Rust service built on `teloxide`. It long-polls
   Telegram, durably queues authorized text/voice/document/reset events, and ferries
   conversational output between Telegram and the browser without constructing
   prompts or running Kennedy.

The four backends are independent library crates compiled into one
`kennedy-server` executable. The executable starts their listeners but shares
no router, database, application state, or service handle among them. No
backend crate depends on another backend crate; all coordination happens in the
frontend through their public HTTP APIs.

`kennedy-server` also owns a generic named credential vault stored as the
passphrase-encrypted `kennedy-secrets.age` file. At startup the server unlocks
the vault and passes the conventionally named OpenAI and Telegram values
directly to their trusted connectors. The vault has terminal-only
set/remove/list/passphrase commands and no HTTP, browser, Kennedy-tool, Codex,
or reveal surface. Stable runtime policy is compiled into code; only
deployment-specific listeners, paths, limits, and the vault location are CLI
options.

System-prompt manuals are frontend source assets under `Frontend/SystemPrompts`.
Every session composes `KennedyIdentity.txt` with exactly one technical mode
manual. Harness strategy is durable Kmap knowledge rather than static prompt
policy. The frontend appends provider-reported current model and thinking mode
to every composed system prompt.

## 2. Design Principles

- The frontend is the single authority for each live session's chatend and
  in-memory draft.
- The Kweb backend is the single authority for durable memory.
- The conversation history backend is the single authority for unfinished and
  completed conversation records.
- Logical backend isolation is preserved even though all four services share
  one deployment binary.
- The intelligence backend never needs to understand the kweb, short
  identifiers, or Kennedy's text-tool protocol.
- The frontend owns the complete human-readable chatend. It sends the full
  chatend when starting a Codex thread and only newly appended text while
  continuing that thread with `previous_response_id`.
- The Codex adapter uses the machine's ChatGPT-authenticated CLI and resumes
  persisted Codex threads. A `ResetContext` call deliberately abandons the old
  thread and sends the rebuilt chatend as a fresh request.
- `ResetContext` rebuilds the chatend from retained session content, an optional
  assistant-role note to self capped at 400,000 characters, and newly loaded
  kweb nodes. Reset notes remain in retained history across later resets, while
  unloaded node content is genuinely absent afterward.
- Short identifiers never cross the Kweb backend API boundary. The frontend
  resolves them to durable identifiers before making backend calls.
- All Kweb mutations that affect more than one row are SQLite transactions.
- Behavior absent from `UserSpecification.md` is outside the MVP unless it is
  necessary to realize an explicitly specified behavior.

## 3. Runtime Topology

```text
One kennedy-server process
  ├─ Encrypted credential vault -------- kennedy-secrets.age
  ├─ Kweb API :4321 --------------------- kennedy.sqlite3
  │    └─ serves frontend and manuals
  ├─ Intelligence API :4322 ------------ Podman Codex + OpenAI transcription + public web
  ├─ Conversation History API :4323 ---- kennedy-conversations.sqlite3
  └─ Telegram Relay API :4324 ---------- kennedy-telegram.sqlite3 + Telegram long polling

Browser frontend calls all four APIs directly. No backend calls another.
```

Default addresses:

| Component | Address |
| --- | --- |
| Kweb backend | `http://127.0.0.1:4321` |
| Intelligence backend | `http://127.0.0.1:4322` |
| Conversation history backend | `http://127.0.0.1:4323` |
| Telegram relay | `http://127.0.0.1:4324` |

The browser calls all four services directly. Cross-origin backends permit
requests from the Kweb frontend origin. Every listener binds to loopback by
default.

The encrypted vault is portable rather than machine-bound. Copying it with the
three databases preserves configured credentials on a new machine, where the
same passphrase unlocks it. The vault is excluded from Git. Kennedy has no
tracked runtime configuration file.

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
- automatic derivation of model attribution from the selected model and the
  provider's configured reasoning effort,
- the context inspector and memory explorer state,
- checkpoint and recovery orchestration across backend APIs.

The context inspector renders the canonical human-readable Chatend, including
Kennedy's text tool requests and readable tool results. Its Full view and the
generation path share one formatter over the current messages, so the Full
view is every byte of application-controlled plaintext sent to Codex for
Kennedy, not a representation of a JSON recovery archive. Forced Codex or
upstream-provider system/tool scaffolding may be added outside the
application's observable boundary; the application minimizes exposed
scaffolding and does not pretend invisible layers are inspectable. It provides full-context,
system-prompt-only, and expandable Kmap-memory views. The memory view derives
node provenance from the Kweb context snapshot so direct loads, task edges,
active-edge expansions, and summary-only fanout references remain visually distinct.
Token, context-window, and cache telemetry is displayed in the Chatend header.
The canonical request and Full inspector end with the terse line
`context window usage: {used-or-unknown} / {advertised-effective-limit}`.
It uses the previous completed response's exact provider usage; fresh/reset
threads use `unknown` so abandoned-thread data cannot mislead Kennedy.
Each LLM response and readable tool result also carries one compact measured
duration line; the end of a turn contains only total and combined LLM/tool time.
The server emits one concise log per LLM call and tool call plus an aggregate
turn line that separates LLM, tool, and other orchestration time.
Provider IDs and credentials remain hidden.

The frontend itself has no persistent state. It saves versioned, opaque,
lossless recovery archives to the conversation history API before generation,
after complete tool rounds, and after final output. Structured content is
preserved for future media support, but the JSON archive is never used as a
model prompt. It reconstructs the live session from that backend after a reload
or abrupt close while refreshing the active manuals and required root context.

### 4.2 Kweb Backend

The Kweb backend owns:

- creation and migration of the SQLite schema,
- the hardcoded MVP user root and Kennedy root,
- knowledge, provenance, and history nodes,
- connection ordering, promotion, and demotion,
- atomic create, update, and connect operations,
- opaque latest-model attribution for every knowledge node,
- read APIs used by context loading and the memory explorer,
- serving the frontend and prompt-manual files.

It knows nothing about short identifiers, chatends, LLM messages, session call
budgets, or provider APIs.

### 4.3 Intelligence Backend

The intelligence backend owns:

- validating ChatGPT Codex login and loading model/CLI configuration,
- discovering each configured model's effective context window from Codex's
  advertised catalog rather than a local constant,
- validating the canonical plaintext generation request,
- passing canonical plaintext and continuation controls into bounded,
  read-only non-interactive Codex turns,
- minimizing exposed Codex overhead through terse inline instructions,
  suppressed optional instruction/tool/plugin features, and a probed slim
  catalog derived from live model metadata without changing advertised model
  limits,
- suppressing Codex auto-compaction beyond every reachable context so Kmap
  material is never silently summarized,
- translating Codex text, thread IDs, errors, and detailed token usage
  into one response shape,
- routing isolated WebSearch runs across compiled Codex and Gemini tiers,
- safely fetching and extracting bounded text from public pages for WebFetch,
- publishing model input modalities and using paid OpenAI
  `gpt-4o-transcribe` only when the selected model transport does not accept
  native audio.

It stores no local LLM session and never parses Kennedy's tool envelopes. The
normal generation path has no enabled shell, file-mutation, app, multi-agent,
or internet capability. Stock Codex still emits its irreducible `update_plan`
and environment-backed `view_image` schemas, which the inline instruction
forbids Kennedy from using. The frontend recognizes WebSearch and WebFetch text
calls, invokes the corresponding intelligence API, then appends their readable
results to the main conversation chain. Quality and balanced search runs are
fresh ephemeral Codex threads; fast search is a stateless Gemini 3.1 Flash-Lite
interaction with Google Search grounding. None can alter the conversation
continuation chain.

### 4.4 Conversation History Backend

The conversation history backend owns its own SQLite database, opaque frontend
checkpoints, optimistic record versions, and the conversation phase machine:

```text
active -> ingress_pending -> ingress_in_progress -> complete
             |               |
             +---------------+-> ingress_failed (fifth failure)
```

It does not call the Kweb or intelligence APIs or validate their identifiers.
It permits multiple `active` and `ingress_pending` records while enforcing one
`ingress_in_progress` record. A user-activity checkpoint also closes other
active conversations idle for more than 24 hours, unless Kennedy still owes a
response in that record. Telegram sessions are exempt and remain active until
their user sends `/reset`.
The backend atomically records concise ingress failure diagnostics. The fifth
failure moves the record to `ingress_failed`, removes it from queue selection,
and frees the worker to process the next conversation; failed records and all
five logs remain queryable.

### 4.5 Telegram Relay

The relay owns the bot token, numeric Telegram authorization bindings, original
Telegram voice/document bytes, and the per-user inbound/outbound delivery queue. It
bootstraps the configured `@taek42` username once and thereafter authorizes by
stable numeric Telegram user ID. It accepts private chats only and refuses all
unpaired users without storing their content. The browser binds each event to a
Conversation History record, runs the normal frontend Chatend/tool loop, and
returns Kennedy's final conversational text. The relay never receives the rest
of the Chatend.

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
deadline, uses the ChatGPT login, receives canonical Chatend text on stdin,
disables automatic compaction, and cannot use shell/file/internet tools. Model
capacity comes from Codex's advertised effective window at startup. If
generation or a tool-round checkpoint fails, the frontend restores the last
durable Chatend and local execution state and retries the pending turn from a
fresh Codex thread.

The Kweb portion of the chatend accumulates during the conversation. A
`ResetContext` call resolves its arguments, validates its optional 400,000
character note to self, removes all Kweb context, resets short identifiers,
reloads the roots and requested nodes, and rebuilds the chatend while retaining
the clean transcript, prior reset notes, and the current shared context-loading
counter. LoadNode and ResetContext consume the same per-turn or per-session
context-loading budget, while ResetContext's internal node loads consume no
extra calls. The rebuild places a complete, duplicate-grouped reset history
before the latest note, using node names because short identifiers are unstable
across resets. The latest note precedes the new Kweb context; root nodes precede
explicitly requested nodes. The Full inspector and next fresh-thread request
are formatted from this rebuilt list, so wiped Kmap material is absent from
both.

Explicitly ending a conversation transitions its durable record to
`ingress_pending`; starting a new conversation does not end existing live ones.
A successful user-message checkpoint in another conversation also queues active
records idle for more than 24 hours, except pending Kennedy turns. The frontend
worker processes queued records oldest-activity-first, creates idempotent data
provenance from each complete recovery archive, transitions exactly one record
to `ingress_in_progress`, and completes it before claiming the next. Live
conversations remain usable throughout. Completed records and both archives
remain queryable from the sidebar.

### 5.2 History Ingress

History ingress uses a separate chatend composed from `KennedyIdentity.txt` and
`HistoryIngress.txt`, the canonical archived conversation text, and both loaded
root nodes. The provenance node stores the complete recovery JSON for
durability, but ingress parses it and formats only its `messages`; recovery
counters, diagnostics, media data URLs, and the JSON envelope do not enter
Kennedy's context.
Kennedy may navigate the kweb, connect nodes, reorganize fanout, manage task
slots, and create or update knowledge nodes. WebSearch and WebFetch are not
available; ingress must interpret only the archived conversation and Kmap
material in front of it. The current provenance identifier is held by the
frontend and supplied implicitly when it translates CreateNode and UpdateNode
tool calls into Kweb API requests.

The frontend likewise holds the active model attribution. For every Kmap
mutation it adds `{model}-{reasoning_effort}` to the Kweb request outside
Kennedy's text-tool arguments. The Kweb backend atomically applies that value to
every semantically affected node. Full node responses expose it as
`last_modified_by`; summary-only nodes remain summary-only, even when their
durable attribution changes.

The session ends when Kennedy returns final text. Its whole Chatend is
checkpointed on the owning conversation record after each tool round and at
completion. Live tool requests, results, and the completion are shown only
when that record is selected, appended after its clean transcript in the same
scrolling flow; completed records reconstruct that continuation from the saved
archive. Completing with zero knowledge mutations is valid.

The ingress session permits at most 100 model rounds in total. That count is
checkpointed before each request and survives recovery and ResetContext; reset
may start a fresh Codex thread but never refreshes the logical-session guard.
The outer worker separately permits five failed attempts across provenance,
model-loop, checkpoint, and completion stages. Each failure records its stage,
sanitized error, round, and measured occupancy. Attempt five aborts the record
permanently instead of starting another retry loop.

At startup every `active` record restores its transcript, directly loaded nodes,
and pending-turn flag. Pending user queries resume from fresh Codex threads.
Queued ingress resumes independently and sequentially without disabling any
live composer.

### 5.3 Telegram and Audio

Conversation records declare `sessionType: conversation` or
`sessionType: telegram`. Prompt composition tells Kennedy which one she is in;
history ingress separately declares whether its archive came from a Telegram
session or a browser conversation. A Telegram session uses the ordinary
conversation manual and read-only conversation tool set. It ends only on
`/reset`, which queues the complete recovery archive; normal sequential history
ingress extracts its canonical Chatend text. Each time current context crosses another 100,000-token band,
the relay sends a separate operational notice with current and maximum tokens
and suggests `/reset`; the notice is not added to the Chatend.

Browser recordings and Telegram voice notes follow the same capability-aware
path. The frontend preserves the original recording and asks the intelligence
backend to transcribe it only when the selected transport lacks native audio.
For the configured text/image-only `gpt-5.6-sol` transport this uses the paid
OpenAI `gpt-4o-transcribe` API, then adds a clearly labeled transcription to the
ordinary text Chatend.

Browser and Telegram document uploads share a local intelligence-backend
extraction endpoint. Searchable PDFs use PDF text extraction, DOCX uses its
OpenXML document body, spreadsheets become sheet-labeled tabular text, and
plain-text formats are normalized directly. Extracted text is bounded and
placed once in the Chatend; original bytes and metadata remain in conversation
media. Image-only PDFs fail with an explicit OCR-required message.

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

All four backends expose versioned APIs under `/api/v1` and an
unversioned `GET /health` endpoint.

- Durable identifiers are lowercase hexadecimal encodings of 20 random bytes.
- Timestamps are RFC 3339 UTC strings.
- Requests and responses use `application/json`, except multipart audio upload
  and raw relay media retrieval.
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
TelegramRelay/
  Specification.md
Frontend/
  Specification.md
  SystemPrompts/
    KennedyIdentity.txt
    ConversationManual.txt
    HistoryIngress.txt
  public/
IntelligenceBackend/
  Specification.md
KennedyServer/
  src/main.rs
```

Implementation code belongs under its owning component directory.

## 9. MVP Non-Goals

- General-purpose user administration beyond configured Telegram bootstrap
  identities.
- Network deployment beyond the local machine.
- Manual knowledge editing, deletion, or fanout pruning.
- Self-action sessions.
- Streaming generation.
- Provider-side conversation persistence.
- A frontend build system, Node.js, or TypeScript.
