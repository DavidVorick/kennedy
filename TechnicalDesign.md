# Kennedy Technical Design

## 1. Purpose and Authority

Kennedy is a local-first memory application built around the kweb described in
`UserSpecification.md`. The user specification defines product behavior. This
document defines the architecture used to implement that behavior, and the
component specifications define the detailed contracts.

The MVP has seven logical runtime components:

1. **Frontend**: a browser-native HTML/CSS/JavaScript observer and command
   client. It renders durable state, keeps unsent drafts and capture state, and
   submits explicit user intents; it owns no live Kennedy execution.
2. **Backend orchestrator**: native Rust tasks inside `kennedy-server`. It owns
   every live Chatend, prompt composition, short-ID context, model/tool loop,
   checkpoint/retry path, Telegram handling, self time, and the shared
   Kmap-write scheduler.
3. **Kweb storage and HTTP adapter**: `kweb-db-core` is a standalone Rust
   library that owns only SQLite persistence and per-node history invariants.
   `kennedy-server` imports it and owns the versioned Kmap HTTP routes, static
   frontend assets, root-role mappings, and transport policy.
4. **Intelligence adapter**: an in-process Rust router. It translates a complete LLM
   request into a provider request, performs bounded web research and public
   page retrieval, and normalizes the results. It is stateless between
   requests.
5. **Conversation history library**: an in-process Rust router. It durably checkpoints
   all conversations, stores idempotent web/self-time start intents, exposes
   per-conversation command heads, and owns the history-ingress state machine.
6. **`kcode-tg-kennedy-bot` Telegram transport**: an exact-version crates.io Rust library built on `teloxide`. It long-polls
   Telegram, owns Telegram transport, stable opaque group identity, membership,
   and group-security state, durably queues private and group work, and ferries
   conversational output between Telegram and Kennedy's orchestration worker
   without constructing prompts or running Kennedy itself. Kennedy passes the bot token and an identity
   callback into the library; Kennedy's user directory separately owns
   whitelist/TOFU identity, `/adduser` capability, and Kmap-root assignments.
7. **Audio ingress library**: an in-process Rust router. It durably accepts vnote WAV
   files, owns content-hash idempotency and restartable preparation, transcribes
   ordered overlapping chunks with Gemini, reconciles a final transcript with
   Sol, and queues timestamped transcript pieces for Kennedy ingress.

The Rust services are library crates compiled into one `kennedy-server`
executable, whose Tokio runtime also runs the native Rust orchestrator. Kmap,
intelligence, conversation history, audio ingress, Telegram identity, and the
frontend are merged into one Axum application on port 4321. All Kennedy
coordination happens in the backend orchestrator through the same public HTTP
APIs used for durable boundaries. The published Telegram transport remains on
port 4324 until its crate exposes a mergeable router instead of owning a
listener.

`kennedy-server` also owns a generic named credential vault stored as the
passphrase-encrypted `kennedy-secrets.age` file. At startup the server unlocks
the vault and passes the conventionally named OpenAI, Gemini, and Telegram
values directly to their trusted connectors. The vault has terminal-only
set/remove/list/passphrase commands and no HTTP, browser, Kennedy-tool, Codex,
or reveal surface. Stable runtime policy is compiled into code; only
deployment-specific listeners, paths, limits, and the vault location are CLI
options.

System-prompt assets live under `Frontend/SystemPrompts` for shared serving and
inspection, but the backend orchestration worker is their execution consumer.
Every session composes identity, one minimal session description, shared Kmap
basics, and shared read tools, including Kmap reads and web research. History
and audio ingress additionally receive write tools.
Harness strategy is durable Kmap knowledge rather than static prompt policy.
The backend appends the provider-reported current model and thinking mode.

## 2. Design Principles

- The backend orchestration worker is the single authority for every live
  session Chatend; the frontend owns only unsent local draft/capture state.
- Each native `Session` owns one backend Chatend, while the orchestration worker
  supplies conversation/audio ingress mechanics and places those mechanics plus
  self time and root provisioning behind one Kmap-writer gate.
- Ordinary web command heads and Telegram stream heads launch as independent
  asynchronous tasks. They share no global read-session lock and do not wait
  for unrelated conversations; durable per-conversation/per-stream ordering is
  still enforced at their queue boundaries.
- The Kmap storage library is the single authority for durable memory rows;
  the backend orchestrator owns graph policy and the main server owns HTTP and
  root roles.
- The conversation history backend is the single authority for unfinished and
  completed conversation records.
- Logical service isolation is preserved except for the deliberate in-process
  Kmap storage-library adapter in the main server.
- The intelligence backend never needs to understand the kweb, short
  identifiers, or Kennedy's text-tool protocol.
- The backend owns the complete human-readable chatend. It sends the full
  chatend when starting a Codex thread and only newly appended text while
  continuing that thread with `previous_response_id`.
- The Codex adapter uses the machine's ChatGPT-authenticated CLI and resumes
  persisted Codex threads. A `ResetContext` call deliberately abandons the old
  thread and sends the rebuilt chatend as a fresh request.
- `ResetContext` rebuilds the chatend from retained session content, an optional
  assistant-role note to self capped at 400,000 characters, and newly loaded
  kweb nodes. Reset notes remain in retained history across later resets, while
  unloaded node content is genuinely absent afterward.
- Short identifiers never cross the Kmap HTTP boundary. The backend session
  resolves them to durable identifiers before making backend calls.
- Each Kmap mutation has a required caller-generated 16-byte idempotency
  identifier and commits its durable receipt in the same SQLite transaction.
  Backend graph workflows spanning several nodes remain sequential and
  non-atomic; their individual requests are safely replayable under their own
  identifiers, but the whole workflow is not one atomic/idempotent operation.
- Behavior absent from `UserSpecification.md` is outside the MVP unless it is
  necessary to realize an explicitly specified behavior.

### 2.1 Security Requirement: Credential-Bearing Dependencies

Any third-party dependency whose API is given an API key, access token, bot
token, or equivalent reusable credential through any function, method,
constructor, callback, or configuration value is a security-critical
dependency. Its declared version must be pinned to one exact, immutable release
or source revision; a compatible-version range and a lockfile alone do not
satisfy this requirement.

Before Kennedy first imports or otherwise uses such a dependency, maintainers
must closely audit the complete source code of the exact pinned version for
credential handling, network behavior, logging, persistence, build scripts,
and any other path that could disclose the credential. The dependency may not
be accepted until that audit finds its behavior safe. Every proposed version
change, including a patch-level bump, requires the same close source audit of
the proposed exact version before the pin is changed. The review must record
the audited version or revision and its conclusion so the pin and audit are
verifiable together. Automated vulnerability or license scans may supplement
this review but do not replace it.

## 3. Runtime Topology

```text
One kennedy-server process
  ├─ Encrypted credential vault -------- kennedy-secrets.age
  ├─ Main HTTP application :4321
  │    ├─ /api/v1/kmap/* via kweb-db-core
  │    ├─ intelligence routes via kcode libraries
  │    ├─ conversation history routes --- kennedy-conversations.sqlite3
  │    ├─ audio ingress routes ----------- kennedy-audio.sqlite3 + kennedy-audio-ingress/
  │    ├─ Telegram identity routes ------- kennedy-users.sqlite3
  │    └─ frontend and manuals
  └─ Telegram Relay API :4324 ---------- kennedy-telegram.sqlite3 + Telegram long polling

Browser frontend calls the main application and Telegram relay directly. The
main application calls its storage libraries in-process.
```

Default addresses:

| Component | Address |
| --- | --- |
| Main frontend and owned APIs | `http://127.0.0.1:4321` |
| Telegram relay | `http://127.0.0.1:4324` |

All owned browser requests are same-origin. Only the Telegram relay is
cross-origin and permits requests from the frontend origin. Both listeners
bind to loopback by default.

The encrypted vault is portable rather than machine-bound. Copying it with the
five databases and audio media preserves configured credentials and queued
vnotes on a new machine, where the same passphrase unlocks the vault. The vault
is excluded from Git. Kennedy has no tracked runtime configuration file.

### 3.1 Offline backup boundary

`kennedy-server backup` acquires the configured Kweb listener before inspecting
any persistent path and holds it while serving a maintenance page. Ordinary
server startup acquires the same listener before unlocking the vault or opening
any database. The listener is therefore the inter-process exclusion boundary:
backup fails while Kennedy is running, and a competing server fails before it
can mutate a backup source.

While that boundary is held, the command uses SQLite's backup API to create
standalone snapshots of the audio-ingress, Telegram transport, user directory,
conversation-history, and Kweb databases, copies the complete Kweb provenance-artifact and audio media trees and still-encrypted
credential vault when present, verifies SQLite integrity and foreign keys, and
atomically publishes a private timestamped `.tar.gz`. The archive contains a
checksummed JSON manifest and a recovery README beginning with the creating
commit hash. Exact snapshot DDL and semantic descriptions of every persisted
format travel with the data so a later version can construct an explicit
migration without trusting current source docs.

The default backup is complete and validates each copied Kweb artifact against
the byte length and SHA-256 retained in SQLite. `--lightweight-kweb`
deliberately omits that tree while retaining its metadata and all lightweight
node/history state; externally stored provenance is unavailable from that
archive alone.

## 4. Ownership Boundaries

### 4.1 Frontend

The frontend owns:

- active view and selected-record navigation,
- unsent composer drafts, browser media capture, and attachment preparation,
- start/message/retry/end/stop and explicit ingress-retry requests,
- polling and rendering Conversation History, Telegram, audio, and Kmap state,
- the context inspector and memory explorer state,
- accessibility, focus, scrolling, and disclosure presentation state.

The frontend never constructs a `ConversationSession`, calls an agent loop,
claims ingress, or performs Kmap mutation. Closing every browser window changes
neither backend conversation processing nor Telegram, self-time, or ingress
execution.

The context inspector renders the canonical human-readable Chatend, including
Kennedy's text tool requests and readable tool results. Its Full view and the
generation path share one formatter over the current messages, so the Full
view is every byte of application-controlled plaintext sent to Codex for
Kennedy, not a representation of a JSON recovery archive. Forced Codex or
upstream-provider system/tool scaffolding may be added outside the
application's observable boundary; the application minimizes exposed
scaffolding and does not pretend invisible layers are inspectable. The formatter
lives in shared Rust backend code. Conversation History and Audio Ingress apply
it while reading legacy archives that contain messages but lack `chatendText`,
including pre-reset segments, and return the computed field to the browser.
Existing persisted canonical strings are never replaced, and the frontend has
no legacy formatter or alternate Full-view representation. It provides full-context,
system-prompt-only, and expandable Kmap-memory views. The memory view derives
node provenance from the Kweb context snapshot so direct loads, task edges,
active-edge expansions, and summary-only fanout references remain visually distinct.
Token, context-window, and cache telemetry is displayed in the Chatend header.

The canonical Kmap formatter normalizes memory by role instead of repeating
connection summaries inside every full node. Direct nodes keep both
descriptions and identifier-only edges. Active expansions appear once with
their long descriptions and identifier-only edges. Direct fanouts are unique
name-and-summary references, while fanouts found only one level below an active
expansion are unique name-only references. The structured snapshot remains
richer for recovery and the interactive memory tree.
The canonical request and Full inspector end with the terse line
`context window usage: {used-or-unknown} / {advertised-effective-limit}`.
It uses the latest completed response's provider usage. Reloads and fresh/reset
provider threads retain that last successful measurement until a newer LLM
response replaces it; only a session with no successful usage report displays
`unknown`.
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
Conversation History persists a bounded summary beside each opaque archive.
The frontend starts from those compact records so a large collection of media
and Chatend histories cannot block the sidebar, then retrieves the complete
state only for a conversation it opens or restores.

### 4.2 Kmap Storage Library and HTTP Adapter

The `kweb-db-core` library owns the strict current Kweb SQLite schema,
its sibling immutable artifact store, knowledge/provenance/history rows, ordered fixed/recent ID arrays, individual
create/update transactions, latest-model attribution, modification timestamps,
and extensible text statistics. It initializes new databases but deliberately
contains no compatibility or migration behavior; existing files must already
satisfy its schema before they are opened. Its complete public contract and
Rust API are published on
[`docs.rs`](https://docs.rs/crate/kweb-db-core/0.2.2).

The library has no HTTP server and knows nothing about root roles, Telegram
identities, users, prompts, short identifiers, active/fanout policy, graph
operations, LLM messages, session budgets, or provider APIs. Its caller must
serialize access. Mutations are individually idempotent through required
caller-generated identifiers, but there is no optimistic lost-update
protection or transaction spanning multiple calls.

`kennedy-server` wraps one library handle in a mutex, exposes it under
`/api/v1/kmap`, serves frontend/prompt files, and stores the `user` and
`kennedy` role mappings in the separate identity database. Each node read and
mutation response enriches the library's ordered connection IDs with an
additive summary projection containing the connected nodes' names and short
descriptions. The backend orchestrator owns recent-connection ordering and
implements multi-node graph operations through sequential complete-node
updates. It joins that metadata onto each connection and interprets the first
eight recent IDs as active expansions and the remainder as fanout summaries;
the frontend applies the same projection only for its interactive memory
explorer.

The prompt route accepts safe plain `.txt` basenames from the configured prompt
directory instead of duplicating the frontend's filename manifest in Rust.
Adding a new mode prompt therefore cannot fail merely because a second
hardcoded allowlist was not updated.

Frontend startup treats Kweb, prompt assets, conversation history,
intelligence, Telegram, and audio ingress as separate feature dependencies.
Successful subsystems remain usable when a sibling subsystem fails. Shared
identity, Kmap-basics, and read-tools assets gate every model session;
session-specific and write assets gate only modes that consume them. In
particular, `AudioIngressSession.txt` gates audio Kmap mutation without gating
conversation chat, ordinary history ingress, audio preparation, or audio
history inspection. Conversation and audio ingress queue polls also isolate
transient failures from one another.

### 4.3 Intelligence Adapter and Standalone Libraries

`kcode-codex-runtime` owns Codex generation, native Codex web search, login and
prompt-boundary validation, and the process-wide versioned sanitized model
catalog cache shared with AudioIngress. `kcode-gemini-api` owns Gemini grounded
search, `kcode-openai-api` owns paid transcription and image generation,
`kcode-web-fetch` owns safe bounded local HTTP fetching, and
`kcode-doc-extraction` owns local PDF, DOCX, spreadsheet, and text extraction.
Each is standalone and can be moved to its own repository and crates.io without
Kennedy code.

KennedyServer's thin intelligence adapter owns:

- Kennedy's provider/model allowlist and response DTOs,
- HTTP request validation and provider-error normalization,
- active-operation cancellation and current-process Codex thread admission,
- the fixed `quality`, `balanced`, and `fast` search profiles and deadlines,
- publishing model input modalities and selecting paid OpenAI transcription
  only when the configured model transport does not accept native audio.

It stores no local LLM session and never parses Kennedy's tool envelopes. The
normal generation path has no enabled shell, file-mutation, app, multi-agent,
or internet capability. Codex still emits its irreducible `update_plan` and
environment-backed `view_image` schemas despite their exposed switches being
false, but Kennedy receives no invisible instruction about them. The frontend
recognizes WebSearch and WebFetch text calls, invokes the corresponding
intelligence API, then appends their readable results to the main conversation
chain. Quality and balanced search runs are fresh ephemeral Codex threads; fast
search is a stateless Gemini 3.1 Flash-Lite interaction with Google Search
grounding. Callers cannot override search deadlines, and the adapter does not
cap source count or ask Gemini for an artificial output-token ceiling. None can
alter the conversation continuation chain.

### 4.4 Conversation History Backend

The conversation history backend owns its own SQLite database, opaque frontend
checkpoints, optimistic record versions, and the conversation phase machine:

```text
active -> ingress_pending -> ingress_in_progress -> complete
   |
   +---------------------------------------------> complete (self time)
             |               |
             +---------------+-> ingress_failed (fifth failure)
```

It does not call the Kweb or intelligence APIs or validate their identifiers.
It permits multiple `active` and `ingress_pending` records while enforcing one
`ingress_in_progress` record. A user-activity checkpoint also closes other
active conversations idle for more than 24 hours, unless Kennedy still owes a
response in that record. All Telegram session types are exempt from idle
closure. Private sessions and per-group-user sessions remain active until
`/reset`; per-group-user sessions also close after the relay reports more than
50 messages without that user's invocation. Only background group batches are
explicitly queued immediately.
The backend atomically records concise ingress failure diagnostics. The fifth
failure moves the record to `ingress_failed`, removes it from queue selection,
and frees the worker to process the next conversation; failed records and all
five logs remain queryable.
An explicit, optimistic-version-checked purge permanently deletes a record in
any phase without transitioning it into the ingress queue. The frontend uses
this destructive path for stuck sessions and cancels locally owned work first.

### 4.5 `kcode-tg-kennedy-bot` Telegram Transport

Kennedy imports the exact published `kcode-tg-kennedy-bot` crate, unlocks the
secret vault, and passes the bot token into the relay library. The crate's
source and embedded migrations are maintained outside this repository.
The relay owns a transport/event database containing private
session pointers, opaque stable group IDs, chat-ID aliases, membership ledgers,
group-security decisions, event/message archives, and queue cursors. The
separate Kennedy user directory owns whitelist handles, TOFU-pinned numeric
IDs, reserved user and group Kmap root IDs, and root readiness. It maps relay
group IDs to local roots; the relay database never stores a Kmap root. `@taek42` is the only
initial unresolved privileged handle; the backend uses the generic directory
capability rather than a David-specific ID path. Its eventual numeric ID and
every `/adduser @handle` entry are pinned on first matching observation.

Private chats retain per-user sessions and `/reset`. In groups, Kennedy must be
an administrator and the observed active-member ledger must exactly match the
Telegram member count. Every identity ever observed in the group remains in a
historical ledger and must be whitelisted even after leaving or being kicked.
An incomplete roster, unauthorized historical identity, or loss of monitoring
quarantines the logical group; eligibility is recomputed and becomes allowed
once the complete history is whitelisted. Message content is discarded before
text/media handling while quarantined. Each observed group keeps a stable
opaque relay ID, mapped by Kennedy to a stable root, through quarantine and
Telegram chat-ID migration. Mentions, replies, and scoped group resets queue
response work onto a persistent `(relay group ID, user)`
session with up to 50 messages of initial context. Every allowed group message
is also archived once and exposed as a durable passive-context stream to every
open session in the group, including voice notes, supported documents, and
Kennedy replies produced for another user. Passive delivery advances a
per-session cursor and never runs the model. Queue heads are isolated by group
user. A session whose user has not invoked Kennedy for more than 50 group
messages is detached and silently queued for history ingress after its pending
passive context is checkpointed. More than 100 uninvoked messages
queue the oldest 80 for background ingress. The backend worker provisions
reserved roots through its single Kmap-writer gate, binds each event to
Conversation History, runs the Chatend/tool loop, and returns only Kennedy's
final text. Fetched queue heads run independently in the backend, so the
worker continues polling and can start other private-user or group-user
streams while one head is waiting on long model or tool work; the relay remains
the authority for ordering within each stream. Event binding records a durable
processing start. The backend enforces a 30-minute deadline from that value,
cancels the active turn on expiry, and invokes an idempotent relay abort that
completes the event before best-effort Telegram notification and clears only a
still-matching stream pointer. A missing Conversation History target is repaired
by compare-and-swap rebinding from the exact stale ID, retaining stored media
and transcription and restarting the recovered attempt's deadline. The relay
never receives the rest of the Chatend.

### 4.6 Audio Ingress Backend

The audio-ingress backend owns a fourth SQLite database and a private media
tree. Upload is streaming and complete only after the WAV has been synced,
hashed, content-addressed, and represented by a durable `uploaded` row. The
SHA-256 uniqueness constraint is the recording identity used by both normal and
historical imports; filename changes do not create duplicate ingestion.

Its restartable worker advances one oldest job through `chunking`,
`transcribing`, `reconciling`, and `ready_for_ingress`. WAV chunks are equalized
for a recording, never exceed four minutes, and overlap adjacent windows by
fifteen seconds. The archived original is the byte-for-byte uploaded WAV. For
each request, AudioIngress resamples a working WAV to 48 kHz only inside the
in-memory Ogg Opus conversion, retains its mono or stereo channel count, and
encodes at 192 kbps per channel (384 kbps for stereo). `kcode-gemini-api` sends
the Opus bytes inline to `gemini-3.1-pro-preview` and applies the JSON response
schema for utterances with local speaker labels, original language, English
translation, timestamps, annotations, and confidence. No compressed audio is
persisted and no Gemini Files API object is created. Failed provider stages
retain their exact durable stage and retry with bounded exponential delay after
restart.

One fresh ephemeral `gpt-5.6-sol`/`xhigh` turn receives all chunk transcripts
in exact chronological order, reconciles speakers, removes repeated overlap,
and produces the canonical final transcript without summarizing it. Sol inserts
explicit sensible boundaries when necessary; a second copy-only Sol pass is
used if a resulting piece exceeds the shared estimate of one token per four
Unicode characters and hard limit of 50,000 estimated tokens. The database
then owns one independently checkpointed Kennedy-ingress row per final piece.
The service calls neither Kweb nor the intelligence backend.
After every Kennedy-ingress row reaches `complete`, the service deletes the
recording's generated WAV shard directory. The raw content-addressed original,
chunk transcript JSON, canonical transcript, and ingress history remain
durable; startup also removes shard directories left by older completed jobs.

## 5. Session Model

### 5.1 Conversation

Each active conversation has its own backend transcript, Chatend, loaded Kmap
snapshot, and Codex thread, plus an optional browser-local composer draft. The
frontend can switch or create sessions while Kennedy is working in another.
It submits a durable command; the backend appends the user message, checkpoints
the pending query, and only then calls the intelligence backend. Conversation
tools are read-only with
respect to Kmap: `LoadNode`, `ResetContext`, `WebSearch`, and `WebFetch`.
Only user and Kennedy text is added to the clean transcript.

Ordinary model generations invoke `codex-safe`, which runs
`codex exec --json` inside a persistent Podman sandbox with `gpt-5.6-sol` and
`xhigh`, resuming by Codex thread ID. The child process is bounded by a total
deadline, uses the ChatGPT login, receives canonical Chatend text on stdin,
disables automatic compaction, and cannot use shell/file/internet tools. Model
capacity comes from Codex's advertised effective window at startup. If
generation or a tool-round checkpoint fails, the backend restores the last
durable Chatend and local execution state and retries the pending turn from a
fresh Codex thread.

The Kweb portion of the chatend accumulates during the conversation. A
`ResetContext` call resolves its arguments, validates its optional 400,000
character note to self, removes all loaded Kweb material while preserving the
session's short-identifier ledger, reloads the roots and requested nodes, and
rebuilds the chatend while retaining
the clean transcript, prior reset notes, and the current shared context-loading
counter. LoadNode and ResetContext consume the same per-turn or per-session
context-loading budget, while ResetContext's internal node loads consume no
extra calls. The rebuild places a complete, duplicate-grouped reset history
before the latest note, using node names for readability. Existing identifiers
remain resolvable after the reset, while newly seen nodes receive monotonically
increasing identifiers. The latest note precedes the new Kweb context; root nodes precede
explicitly requested nodes. The backend formats the next fresh-thread request
from this rebuilt list and checkpoints that exact plaintext for verbatim Full
inspector display, so wiped Kmap material is absent from both.

Explicitly ending a conversation transitions its durable record to
`ingress_pending`; starting a new conversation does not end existing live ones.
A separate `Send & end` path first checkpoints one final user message into the
Chatend without starting a model turn, then performs the same transition so
history ingress sees that message.
A successful user-message checkpoint in another conversation also queues active
records idle for more than 24 hours, except pending Kennedy turns. The backend
orchestrator processes conversation and audio queues through one serialized
Kmap writer shared with self time and root provisioning. It resumes
already-claimed work first, otherwise chooses
the oldest source timestamp, creates data provenance from each complete
recovery archive, persists its returned ID while transitioning exactly one record
to `ingress_in_progress`, and completes it before claiming the next. Live
conversations remain usable throughout. Completed records and both archives
remain queryable from the sidebar.

Session construction executes `ToolCheck({})` through the real backend tool
dispatcher and retains both its assistant envelope and successful result. This
gives every later model round—including rounds after `ResetContext`—visible
evidence that the outer text-tool path works. The shared agent loop treats prose
as candidate turn content, checkpoints it, and continues until standalone
`EndTurn({})` succeeds. Conversation and Telegram controllers then publish that
candidate response; one-turn autonomous and ingress controllers terminate.

The history-ingress agent loop cannot complete from ordinary assistant text.
Such text is checkpointed, followed by a controller reminder that Kennedy's
text-protocol Kmap tools are available, and generation continues. Only a
successful, standalone `EndTurn({})` result returns the loop-control
sentinel that permits the durable record to transition to `complete`.

### 5.2 Autonomous Self Time

Self time is a durable autonomous backend session family presented in its own
category tab alongside Conversation, TG Bot, and Audio Ingress. Its start panel
accepts a duration and optional custom prompt. The controller stores that prompt
in the run metadata and repeats it in every clean-slate slice. Starting creates
a run-level Kweb provenance and a backward-compatible `free-time` Conversation
History record containing an absolute deadline and clean-slate slice number.
Kennedy's prompt tells her to have fun and includes the shared read/web manuals
plus the Kmap write manual. The self-time executor therefore permits every
baseline Kennedy tool; the universal `EndTurn` result
closes the current record and opens a fresh slice without changing the run
deadline only when at least five minutes remain. Its optional, bounded message
is checkpointed on the ending record, promoted into that next slice as a
user-role handoff, and then consumed so it cannot leak into later slices. Below
the rollover threshold, yielding ends the run and no message is forwarded.

The frontend exposes one pending start request while Conversation History
atomically rejects a second active run. The backend restores pending work after
restart. Every closed slice transitions
directly to read-only Conversation History instead of normal history ingress,
because the self-time session already writes useful memory to the Kmap under
its run-level provenance. Conversation History atomically refuses a
second active `free-time` record, closing the remaining cross-browser race. The
controller checks the clock before requests and after responses, durably
injects a warning inside the last three minutes, blocks tools at expiry, and
grants one wrap-up response. Provider request timeouts remain profile-specific
(including the longer quality-search allowance) and are clamped only when the
remaining two-minute shutdown grace is shorter; a backend cancellation timer
enforces the same hard stop. Conversation History list reconciliation is
version-monotonic, preventing a delayed response from replacing a newer local
checkpoint version. At an equal version, a cached complete record also wins
over an incoming compact summary. Conflict recovery immediately adopts the
latest server record.

### 5.3 History Ingress

History ingress uses a separate chatend composed from identity, the history
session description, Kmap basics, read tools, and write tools, followed by the
canonical archived conversation text and the archived session's loaded roots. The
provenance node stores the complete recovery JSON for
durability, but ingress parses it and formats only its `messages`; recovery
counters, diagnostics, media data URLs, and the JSON envelope do not enter
Kennedy's context.
Kennedy may navigate the kweb, connect nodes, reorganize fanout, manage fixed
slots, create or update owned knowledge nodes, and use WebSearch or WebFetch when
external evidence would help. The current provenance identifier is held by the
backend session and supplied implicitly when it translates CreateNode and UpdateNode
tool calls into namespaced Kmap API requests.

The backend likewise holds the active model attribution. For every Kmap
mutation it adds `{model}-{reasoning_effort}` to the Kmap request outside
Kennedy's text-tool arguments. Each ordinary storage update atomically applies
that value to its node. Full node responses expose it as
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
sanitized error, round, and measured occupancy. Audio pieces persist a
next-attempt timestamp with one-, five-, fifteen-, and sixty-minute delays so a
short provider outage cannot consume the allowance immediately. Attempt five
aborts the record instead of starting another retry loop.

At startup every `active` record restores its transcript, directly loaded nodes,
and pending-turn flag. Pending user queries resume from fresh Codex threads.
Queued ingress resumes independently and sequentially without disabling any
live composer.

### 5.4 Telegram and Conversational Audio

Conversation records declare `sessionType: conversation`, `telegram`, or
`telegram-group`. Prompt composition tells Kennedy which one she is in;
history ingress separately declares whether its archive came from a Telegram
session or a browser conversation. A private Telegram session uses the conversation
session description and read-only conversation tool set. It ends only on
`/reset`, which queues the complete recovery archive; normal sequential history
ingress extracts its canonical Chatend text. Each time current context crosses another 100,000-token band,
the relay sends a separate operational notice with current and maximum tokens
and suggests `/reset`; the notice is not added to the Chatend.

Each group user retains a separate session within each group until that user
runs `/reset` or goes more than 50 group messages without invoking Kennedy. Its direct roots are the user's reserved root, the group's
reserved root, and Kennedy's root
in that order. Every other member root receives a session-local short identifier
without being loaded, so Kennedy may load it deliberately. Dynamic context
lists the group root, all participants/root identifiers, and the latest 50
messages initially. Thereafter the relay exposes every message through a
per-session cursor and the backend checkpoints it into all open group-user
Chatends without generating a response. This includes messages by other users,
voice-note transcriptions, extracted attachment text, and Kennedy replies from
other sessions; a session's own invocation and response are excluded from its
context copy because they already appear in its transcript. An invocation
catches up any pending context before generation, and `/reset` catches it up
before closure. Context warnings name the relevant `@username` and state
that other participants have separate sessions. Background 80-message archives
have no invoker, so they load the
group root followed by Kennedy's root and register all participant roots before
ordinary sequential history ingress. Group-root assignments are created when a
group is first observed and survive quarantine and Telegram chat-ID
migration.

Browser recordings and Telegram voice notes follow the same capability-aware
path. The frontend preserves the original recording and asks KennedyServer's
intelligence route to transcribe it only when the selected transport lacks native audio.
For the configured text/image-only `gpt-5.6-sol` transport this uses the paid
OpenAI `gpt-4o-transcribe` API, then adds a clearly labeled transcription to the
ordinary text Chatend.

Browser and Telegram document uploads share a KennedyServer endpoint backed by
`kcode-doc-extraction`. Searchable PDFs use PDF text extraction, DOCX uses its
OpenXML document body, spreadsheets become sheet-labeled tabular text, and
plain-text formats are normalized directly. Group voice notes invoke by replying
to Kennedy; group documents may use a bot mention in their caption or a reply.
Extracted text is bounded and
placed once in the Chatend; original bytes and metadata remain in conversation
media. Image-only PDFs fail with an explicit OCR-required message.

### 5.5 Durable Vnote Audio Ingress

The i3 start script runs `arecord` directly into `/home/user/media/vnotes` and
puts the recording-start Unix timestamp in the filename. The stop script ends
`arecord`, waits briefly for WAV finalization, and checks the SHA-256 hashes of
the five most recently modified vnotes against the loopback audio endpoint. It
uploads only hashes Kennedy does not yet know. A successful HTTP response means
only that the complete audio is durable; the caller never waits for
transcription, reconciliation, or Kmap mutation.

After server-side preparation, the backend orchestrator polls the audio and
conversation queues through its one process-wide Kmap-writer gate. Claimed
audio pieces therefore cannot overlap conversation/Telegram ingress, self
time, or Telegram root provisioning. A piece creates immutable `audio-vnote`
provenance keyed by recording
SHA and piece index, with `source_created_at` equal to recording start. Its
model context repeats the timestamp and explicitly defines it as recording
time rather than upload or ingress time.

Audio ingress selects `AudioIngressSession.txt` and combines it with shared
Kmap basics, read tools, and write tools. Recording-time semantics are supplied
with the immutable audio provenance; additional learned audio-ingress judgment
belongs in the Kmap rather than duplicated static instructions. Pieces from one
recording are queued in chronological order and ingressed one at a time. Every
tool-round checkpoint and the complete diagnostic
history survive server restarts. Nonterminal failures use
durable increasing retry delays; a terminal piece can be explicitly requeued
from the UI without discarding its previous failure log. Preparation runs
without a browser, and Kmap mutation resumes automatically while
`kennedy-server` is running.

The UI's `Audio Ingress` tab lists durable recording jobs independently of
conversation history. A recording-detail endpoint returns its final transcript,
ordered Gemini chunk transcripts, Kennedy ingress pieces, retry records, and
each piece's checkpointed Chatend. The center pane displays preparation
artifacts and piece text as closed disclosures, then renders every piece's
history ingress in the same inline continuation format as conversation and
Telegram records. The existing inspector renders every piece as an ordered Full
History phase, including pre-reset segments.

Shared rendering captures the current record/view identity, scroll position,
bottom-following state, open disclosure keys, and focused keyed control before
rebuilding a pane. It restores those values only when the pane still represents
the same logical view. Thus unrelated background checkpoints cannot disturb the
center pane, sidebar, activity log, or inspector; only a pane already at its
bottom follows appended content.

## 6. Kweb Data Model

SQLite stores exactly the three durable node types from the user specification:

- **Knowledge node**: the current human-readable memory, nullable owner node,
  latest attribution/time, fixed/recent ID arrays, and a pointer to the newest
  history node.
- **Data provenance node**: immutable source material, its source, source
  creation time, and ordered links to immutable artifacts.
- **Data history node**: an append-only link from one knowledge node to one
  provenance node and the previous history node.

The default physical store is `kweb-db-core.sqlite3` plus its sibling
`kweb-provenance-artifacts/`. Provenance text at or below 256 KiB remains
inline. Larger text and explicit media are written as private immutable files;
SQLite stores relative path, preserved original basename, media type, byte
length, SHA-256, creation time, semantic role, and order. The main provenance
reader loads external UTF-8 data transparently, while attached media is
returned as metadata and streamed by the HTTP adapter when requested.

Each stored basename inserts a retry-stable 12-character URL-safe Base64
suffix before its final extension. The suffix derives from the mutation's
random `IdempotencyId` and artifact position; its first two characters select
a shard folder. Conversation archives remove top-level media `dataUrl` values
and retain `provenanceArtifactIndex`, which addresses the returned ordered
artifact metadata without placing the bytes back in JSON.

Connections are normalized into ordered `fixed_connections` and
`recent_connections` tables only to preserve ID arrays and foreign-key
integrity. The storage layer gives those arrays no graph semantics. The frontend treats
the first eight recent IDs as active and the remainder as fanout and performs
connect/consolidate/fixed workflows using ordinary complete-state updates.

`knowledge_nodes.owner_node_id` is nullable. Null is exposed as `unowned`; a
self-owner sentinel resolves to the created/updated node ID. The library does
not know which nodes are Kennedy, user, or group roots. System role mappings
live in `kmap_system_roots` in the separate identity database.

`last_modified_at` is required and generated on every create/update. Reads do
not repair or otherwise mutate stored nodes.

Each `create_provenance`, `create_node`, and `update_node` call also supplies a
random 16-byte `IdempotencyId`. The library hashes the normalized semantic
request and records the operation kind, hash, result ID, and commit time in
`idempotency_receipts` within the mutation transaction. An exact replay makes
no write; mismatched reuse conflicts. Successful receipts are retained for the
lifetime of the database. Failed validation or rolled-back writes create no
receipt.

## 7. API Conventions

All API domains expose versioned APIs under `/api/v1`. Kmap health is
`GET /api/v1/kmap/health`; the still-separate services retain unversioned
`GET /health` endpoints.

- Durable node/provenance identifiers are lowercase hexadecimal encodings of
  20-byte values. Library-generated values are random; when an HTTP node-create
  omits its ID, the adapter derives a stable 20-byte value from that request's
  idempotency identifier. Mutation idempotency identifiers encode 16 random
  bytes as 32 lowercase hexadecimal characters.
- Timestamps are RFC 3339 UTC strings.
- Requests and responses use `application/json`, except multipart audio and
  Kweb provenance-artifact upload and raw relay/Kweb artifact retrieval.
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
ConversationHistory/
  Specification.md
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
KennedyServer/
  KmapHttp.md
  src/main.rs
  src/intelligence/
  src/orchestration.rs
  src/orchestration/
    context.rs
    http.rs
    prompts.rs
    session.rs
    worker.rs
```

Implementation code belongs under its owning component directory.
The Kweb storage and five kcode libraries are external crates.io dependencies,
not workspace members. Credential-bearing provider libraries use exact version
requirements; the other kcode libraries use compatible version requirements
resolved by the workspace lockfile.

## 9. MVP Non-Goals

- General-purpose user administration beyond David's `/adduser @handle`
  whitelist command.
- Network deployment beyond the local machine.
- Manual knowledge editing, deletion, or fanout pruning.
- Self-action sessions.
- Streaming generation.
- Provider-side conversation persistence.
- A frontend build system or TypeScript. JavaScript is browser-only; backend
  orchestration is native Rust and has no Node.js runtime dependency.
