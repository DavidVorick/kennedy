# Kennedy Technical Design

## 1. Purpose and Authority

Kennedy is a local-first memory application built around the kweb described in
`UserSpecification.md`. The user specification defines product behavior. This
document defines the architecture used to implement that behavior, and the
component specifications define the detailed contracts.

The MVP has six logical runtime components:

1. **Frontend**: a browser-native HTML/CSS/JavaScript application. It owns the
   user interface, the live chatend, short identifiers, prompt
   composition, and agent tool orchestration.
2. **Kweb storage and HTTP adapter**: `kweb` is a standalone Rust
   library that owns only SQLite persistence and per-node history invariants.
   `kennedy-server` imports it and owns the versioned Kmap HTTP routes, static
   frontend assets, root-role mappings, and transport policy.
3. **Intelligence backend**: a Rust HTTP service. It translates a complete LLM
   request into a provider request, performs bounded web research and public
   page retrieval, and normalizes the results. It is stateless between
   requests.
4. **Conversation history backend**: a Rust HTTP service. It durably checkpoints
   active browser conversations and owns the sequential history-ingress queue.
5. **Telegram relay**: a Rust service built on `teloxide`. It long-polls
   Telegram, owns a separate identity/group-policy database, durably queues
   private and group work, and ferries conversational output between Telegram
   and the browser without constructing prompts or running Kennedy.
6. **Audio ingress backend**: a Rust HTTP service. It durably accepts vnote WAV
   files, owns content-hash idempotency and restartable preparation, transcribes
   ordered overlapping chunks with Gemini, reconciles a final transcript with
   Sol, and queues timestamped transcript pieces for Kennedy ingress.

The runtime services are library crates compiled into one `kennedy-server`
executable. Kmap is the deliberate exception to the former service-isolation
rule: its separately publishable storage crate is imported by the main binary,
which serves its HTTP adapter. Other backend coordination still happens in the
frontend through public HTTP APIs. A later routing consolidation may serve all
API domains on one listener without changing the storage crate.

`kennedy-server` also owns a generic named credential vault stored as the
passphrase-encrypted `kennedy-secrets.age` file. At startup the server unlocks
the vault and passes the conventionally named OpenAI, Gemini, and Telegram values
directly to their trusted connectors. The vault has terminal-only
set/remove/list/passphrase commands and no HTTP, browser, Kennedy-tool, Codex,
or reveal surface. Stable runtime policy is compiled into code; only
deployment-specific listeners, paths, limits, and the vault location are CLI
options.

System-prompt assets are frontend source files under `Frontend/SystemPrompts`.
Every session composes identity, one minimal session description, shared Kmap
basics, and shared read tools, including Kmap reads and web research. History
and audio ingress additionally receive write tools.
Harness strategy is durable Kmap knowledge rather than static prompt policy.
The frontend appends the provider-reported current model and thinking mode.

## 2. Design Principles

- The frontend is the single authority for each live session's chatend and
  in-memory draft.
- Frontend orchestration has explicit boundaries: `app.js` connects UI and
  transports, `ConversationSession` owns one live Chatend, and
  `MemoryIngressCoordinator` owns serialized conversation/audio ingress
  selection, claims, checkpoints, retries, cancellation, and completion.
- The Kmap storage library is the single authority for durable memory rows;
  the frontend owns graph policy and the main server owns HTTP and root roles.
- The conversation history backend is the single authority for unfinished and
  completed conversation records.
- Logical service isolation is preserved except for the deliberate in-process
  Kmap storage-library adapter in the main server.
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
- Short identifiers never cross the Kmap HTTP boundary. The frontend
  resolves them to durable identifiers before making backend calls.
- Each Kmap mutation has a required caller-generated 16-byte idempotency
  identifier and commits its durable receipt in the same SQLite transaction.
  Frontend graph workflows spanning several nodes remain sequential and
  non-atomic; their individual requests are safely replayable under their own
  identifiers, but the whole workflow is not one atomic/idempotent operation.
- Behavior absent from `UserSpecification.md` is outside the MVP unless it is
  necessary to realize an explicitly specified behavior.

## 3. Runtime Topology

```text
One kennedy-server process
  ├─ Encrypted credential vault -------- kennedy-secrets.age
  ├─ Main HTTP adapter :4321 ------------ kweb.sqlite3 + kweb-provenance-artifacts/ + kennedy-users.sqlite3
  │    ├─ /api/v1/kmap/* via kweb library
  │    └─ serves frontend and manuals
  ├─ Intelligence API :4322 ------------ Podman Codex + OpenAI transcription + public web
  ├─ Conversation History API :4323 ---- kennedy-conversations.sqlite3
  ├─ Telegram Relay API :4324 ---------- kennedy-telegram.sqlite3 + kennedy-users.sqlite3 + Telegram long polling
  └─ Audio Ingress API :4325 ----------- kennedy-audio.sqlite3 + kennedy-audio-ingress/

Browser frontend calls all API domains directly. The main adapter calls the
Kmap library in-process; no other backend calls it.
```

Default addresses:

| Component | Address |
| --- | --- |
| Main frontend/Kmap adapter | `http://127.0.0.1:4321` |
| Intelligence backend | `http://127.0.0.1:4322` |
| Conversation history backend | `http://127.0.0.1:4323` |
| Telegram relay | `http://127.0.0.1:4324` |
| Audio ingress backend | `http://127.0.0.1:4325` |

The browser calls all API domains directly. Cross-origin backends permit
requests from the Kweb frontend origin. Every listener binds to loopback by
default.

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

- the clean user/Kennedy transcript,
- the current chatend sent to the LLM,
- directly loaded nodes and their task, active, and fanout connections,
- durable-ID to short-ID mappings,
- conversation and history-ingress call budgets,
- selection and checkpointing of timestamped audio-ingress pieces,
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

The canonical Kmap formatter normalizes memory by role instead of repeating
connection summaries inside every full node. Direct nodes keep both
descriptions and identifier-only edges. Active expansions appear once with
their long descriptions and identifier-only edges. Direct fanouts are unique
name-and-summary references, while fanouts found only one level below an active
expansion are unique name-only references. The structured snapshot remains
richer for recovery and the interactive memory tree.
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

### 4.2 Kmap Storage Library and HTTP Adapter

The `kweb` library owns creation/migration of the Kweb SQLite schema,
its sibling immutable artifact store, knowledge/provenance/history rows, ordered fixed/recent ID arrays, individual
create/update transactions, latest-model attribution, modification timestamps,
and extensible text statistics. Its complete public contract is documented in
the published [`kweb` 0.1.0 specification](https://docs.rs/crate/kweb/0.1.0/source/Specification.md),
and its Rust API is available through
[`docs.rs`](https://docs.rs/kweb/0.1.0/kweb/).

The library has no HTTP server and knows nothing about root roles, Telegram
identities, users, prompts, short identifiers, active/fanout policy, graph
operations, LLM messages, session budgets, or provider APIs. Its caller must
serialize access. Mutations are individually idempotent through required
caller-generated identifiers, but there is no optimistic lost-update
protection or transaction spanning multiple calls.

`kennedy-server` wraps one library handle in a mutex, exposes it under
`/api/v1/kmap`, serves frontend/prompt files, and stores the `user` and
`kennedy` role mappings in the separate identity database. The frontend owns
recent-connection ordering and implements multi-node graph operations through
sequential complete-node updates. It interprets the first eight recent IDs as
active expansions and the remainder as fanout summaries.

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

### 4.3 Intelligence Backend

`kennedy-codex-runtime` owns the process-wide, Codex-versioned sanitized model
catalog cache shared by Intelligence and AudioIngress. It has no HTTP surface.
Kennedy launches all long-running service futures concurrently; within
Intelligence, live login validation also runs concurrently with the shared
catalog load. Only version-dependent discovery, sanitization, and verification
remain ordered because each consumes the previous step's output.

The intelligence backend owns:

- validating ChatGPT Codex login and loading model/CLI configuration,
- obtaining each configured model's effective context window from the shared,
  Codex-versioned sanitized catalog cache rather than a local constant,
- validating the canonical plaintext generation request,
- passing canonical plaintext and continuation controls into bounded,
  read-only non-interactive Codex turns,
- enforcing an empty non-Chatend prompt boundary through empty developer
  instructions, suppressed optional instruction/tool/plugin features, a
  mandatory sanitized catalog with blank provider prompt fields, and a
  model-visible prompt probe cached for each Codex/configuration identity,
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
or internet capability. Codex still emits its irreducible `update_plan` and
environment-backed `view_image` schemas despite their exposed switches being
false, but Kennedy receives no invisible instruction about them. The frontend
recognizes WebSearch and WebFetch text calls, invokes the corresponding
intelligence API, then appends their readable results to the main conversation
chain. Quality and balanced search runs are fresh ephemeral Codex threads; fast
search is a stateless Gemini 3.1 Flash-Lite interaction with Google Search
grounding. None can alter the conversation continuation chain.

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

### 4.5 Telegram Relay

The relay owns the bot token, transport/event database, and a separate user
directory containing whitelist handles, TOFU-pinned numeric IDs, reserved user
and group Kmap root IDs, root readiness, and permanent group decisions. `@taek42` is the only
initial unresolved privileged handle; the backend uses the generic directory
capability rather than a David-specific ID path. Its eventual numeric ID and
every `/adduser @handle` entry are pinned on first matching observation.

Private chats retain per-user sessions and `/reset`. In groups, Kennedy must be
an administrator and the observed active-member ledger must exactly match the
Telegram member count with every identity whitelisted. Unknown/conflicting
members, an incomplete ledger, or loss of monitoring after activation
permanently blacklists the chat ID. Each observed group keeps a stable root even
after blacklisting or a Telegram chat-ID migration. Mentions, replies, and
scoped group resets queue response work onto a persistent `(group root, user)`
session with up to 50 messages of initial context. Every allowed group message
is also archived once and exposed as a durable passive-context stream to every
open session in the group, including voice notes, supported documents, and
Kennedy replies produced for another user. Passive delivery advances a
per-session cursor and never runs the model. Queue heads are isolated by group
user. A session whose user has not invoked Kennedy for more than 50 group
messages is detached and silently queued for history ingress after its pending
passive context is checkpointed. More than 100 uninvoked messages
queue the oldest 80 for background ingress. The browser provisions reserved roots, binds each
event to Conversation History, runs the Chatend/tool loop, and returns only
Kennedy's final text. Fetched queue heads run independently in the browser, so
the bridge continues polling and can start other private-user or group-user
streams while one head is waiting on long model or tool work; the relay remains
the authority for ordering within each stream. Event binding records a durable
processing start. The browser enforces a 30-minute deadline from that value,
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
fifteen seconds. Each ordered chunk is uploaded through Gemini's Files API and
transcribed by `gemini-3.1-pro-preview` into structured utterances with local
speaker labels, original language, English translation, timestamps,
annotations, and confidence. Remote temporary files are deleted after the
request. Failed provider stages retain their exact durable stage and retry with
bounded exponential delay after restart.

One fresh ephemeral `gpt-5.6-sol`/`xhigh` turn receives all chunk transcripts
in exact chronological order, reconciles speakers, removes repeated overlap,
and produces the canonical final transcript without summarizing it. Sol inserts
explicit sensible boundaries when necessary; a second copy-only Sol pass is
used if a resulting piece exceeds the shared estimate of one token per four
Unicode characters and hard limit of 50,000 estimated tokens. The database
then owns one independently checkpointed Kennedy-ingress row per final piece.
The service calls neither Kweb nor the intelligence backend.

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
A separate `Send & end` path first checkpoints one final user message into the
Chatend without starting a model turn, then performs the same transition so
history ingress sees that message.
A successful user-message checkpoint in another conversation also queues active
records idle for more than 24 hours, except pending Kennedy turns. The frontend
`MemoryIngressCoordinator` processes conversation and audio queues through one
serialized worker. It resumes already-claimed work first, otherwise chooses
the oldest source timestamp, creates data provenance from each complete
recovery archive, persists its returned ID while transitioning exactly one record
to `ingress_in_progress`, and completes it before claiming the next. Live
conversations remain usable throughout. Completed records and both archives
remain queryable from the sidebar.

### 5.2 Autonomous Self Time

Self time is a durable autonomous browser session family presented in its own
category tab alongside Conversation, TG Bot, and Audio Ingress. Its start panel
accepts a duration and optional custom prompt. The controller stores that prompt
in the run metadata and repeats it in every clean-slate slice. Starting creates
a run-level Kweb provenance and a backward-compatible `free-time` Conversation
History record containing an absolute deadline and clean-slate slice number.
Kennedy's prompt tells her to have fun and includes the shared read/web manuals
plus the Kmap write manual. The self-time executor therefore permits every
baseline Kennedy tool and adds `EndSelfTimeSession`, whose loop-control result
closes the current record and opens a fresh slice without changing the run
deadline only when at least five minutes remain. Its optional, bounded message
is checkpointed on the ending record, promoted into that next slice as a
user-role handoff, and then consumed so it cannot leak into later slices. Below
the rollover threshold, yielding ends the run and no message is forwarded.

The controller immediately exposes one pending start promise, owns a cross-tab
Web Lock, and restores pending work after reload. Every closed slice transitions
directly to read-only Conversation History instead of normal history ingress,
because the self-time session already writes useful memory to the Kmap under
its run-level provenance. Conversation History atomically refuses a
second active `free-time` record, closing the remaining cross-browser race. The
controller checks the clock before requests and after responses, durably
injects a warning inside the last three minutes, blocks tools at expiry, and
grants one wrap-up response. Provider request timeouts remain profile-specific
(including the longer quality-search allowance) and are clamped only when the
remaining two-minute shutdown grace is shorter; a browser cancellation timer
enforces the same hard stop. Conversation History list reconciliation is
version-monotonic, preventing a delayed response from replacing a newer local
checkpoint version, and conflict recovery immediately adopts the latest server
record.

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
frontend and supplied implicitly when it translates CreateNode and UpdateNode
tool calls into namespaced Kmap API requests.

The frontend likewise holds the active model attribution. For every Kmap
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
per-session cursor and the browser checkpoints it into all open group-user
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
group is first observed and survive permanent blacklisting and Telegram chat-ID
migration.

Browser recordings and Telegram voice notes follow the same capability-aware
path. The frontend preserves the original recording and asks the intelligence
backend to transcribe it only when the selected transport lacks native audio.
For the configured text/image-only `gpt-5.6-sol` transport this uses the paid
OpenAI `gpt-4o-transcribe` API, then adds a clearly labeled transcription to the
ordinary text Chatend.

Browser and Telegram document uploads share a local intelligence-backend
extraction endpoint. Searchable PDFs use PDF text extraction, DOCX uses its
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

After server-side preparation, the browser's existing ingress worker polls the
audio and conversation queues under one `kennedy-history-ingress` Web Lock.
Claimed audio pieces therefore cannot overlap any other browser-owned Kmap
mutation. A piece creates immutable `audio-vnote` provenance keyed by recording
SHA and piece index, with `source_created_at` equal to recording start. Its
model context repeats the timestamp and explicitly defines it as recording
time rather than upload or ingress time.

Audio ingress selects `AudioIngressSession.txt` and combines it with shared
Kmap basics, read tools, and write tools. Recording-time semantics are supplied
with the immutable audio provenance; additional learned audio-ingress judgment
belongs in the Kmap rather than duplicated static instructions. Pieces from one
recording are queued in chronological order and ingressed one at a time. Every
tool-round checkpoint and the complete diagnostic
history survive both server and browser restarts. Nonterminal failures use
durable increasing retry delays; a terminal piece can be explicitly requeued
from the UI without discarding its previous failure log. Preparation runs
without a browser, while Kmap mutation resumes whenever Kennedy's browser
worker is open.

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

The default physical store is `kweb.sqlite3` plus its sibling
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
integrity. The storage layer gives those arrays no graph semantics. During
legacy migration, former fixed slots retain slot order and former active plus
fanout edges are merged by descending activation order. The frontend treats
the first eight recent IDs as active and the remainder as fanout and performs
connect/consolidate/fixed workflows using ordinary complete-state updates.

`knowledge_nodes.owner_node_id` is nullable. Null is exposed as `unowned`; a
self-owner sentinel resolves to the created/updated node ID. The library does
not know which nodes are Kennedy, user, or group roots. System role mappings
live in `kmap_system_roots` in the separate identity database.

`last_modified_at` is generated on create/update. A migrated legacy row starts
null and receives the current time on its first node load.

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
CodexRuntime/
  Specification.md
ConversationHistory/
  Specification.md
TelegramRelay/
  Specification.md
Frontend/
  Specification.md
  SystemPrompts/
    KennedyIdentity.txt
    ConversationSession.txt
    HistoryIngressSession.txt
    AudioIngressSession.txt
    KmapBasics.txt
    ReadTools.txt
    WriteTools.txt
  public/
IntelligenceBackend/
  Specification.md
KennedyServer/
  KmapHttp.md
  src/main.rs
```

Implementation code belongs under its owning component directory.
The Kweb storage implementation is the external, exact-version dependency
[`kweb` 0.1.0](https://crates.io/crates/kweb/0.1.0), not a workspace member.

## 9. MVP Non-Goals

- General-purpose user administration beyond David's `/adduser @handle`
  whitelist command.
- Network deployment beyond the local machine.
- Manual knowledge editing, deletion, or fanout pruning.
- Self-action sessions.
- Streaming generation.
- Provider-side conversation persistence.
- A frontend build system, Node.js, or TypeScript.
