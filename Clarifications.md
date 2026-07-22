# Project Clarifications

These user-provided decisions supplement the repository specifications and
technical design. They may be consolidated or removed once represented by the
canonical documents; this file is not an append-only log.

## Audio Transcription Boundary

- Kennedy consumes the published `kcode-audio-transcribe` package rather than
  retaining its implementation in this repository.
- The host constructs and passes configured `kcode-gemini-api` and
  `kcode-codex-runtime` client objects; the
  transcription library never receives or unlocks raw API keys.
- Its caller API is job-oriented and asynchronous:
  `transcribe(audio_bytes)` takes ownership of raw audio bytes and returns a job
  handle, and `job.status()` returns pollable progress and the completed final
  transcript. Kennedy does not orchestrate chunking, provider calls, retries,
  or reconciliation.
- The library owns no media directory, database, durable job state, or restart
  recovery. Filenames, recording timestamps, hashes, and durable originals
  remain Kennedy metadata. If transcription is interrupted by shutdown,
  Kennedy starts a new job from the retained original and may repeat provider
  work.
- `kcode-audio-transcribe` owns transcription only. It has no Kennedy HTTP,
  Chatend, Kmap, provenance, or MemoryIngress behavior. Kennedy alone persists
  the final transcript, maps it into MemoryIngress jobs, and builds and
  checkpoints the audio-ingress Chatend.

## Repository-Local Runtime Data

- Keep Kennedy's persistent runtime data inside the repository sandbox, but
  consolidate it under the single ignored `data/` directory so the repository
  root remains source-focused and the data remains inspectable by Codex.
- Do not add a `--data-dir` abstraction. Change the existing individual path
  defaults to locations under `./data/` while retaining those existing flags as
  overrides. Store live databases and their WAL/SHM companions directly in
  `data/`, audio originals in `data/audio-ingress-media/`, Kweb artifacts in
  `data/kweb-provenance-artifacts/`, the encrypted vault in `data/`, and backup
  and manual recovery material in `data/backups/` and `data/recovery/`.

## Runtime Service Boundaries and Audio Retention

- Serve Kmap, intelligence, conversation history, audio ingress, Telegram
  identity, and the frontend from the main `127.0.0.1:4321` listener. The
  published Telegram relay remains on `4324` while it owns its listener.
- Preserve each raw audio original in Kennedy-owned storage. The external audio
  transcription library owns no media directory or durable generated shards;
  it receives only owned bytes and keeps all internal working data ephemeral and
  memory-only. Kennedy retains completed transcript metadata in SQLite and may
  restart an interrupted transcription from the raw original rather than
  resuming completed chunks.
- Exact-pin dependencies that receive provider credentials. Ordinary
  dependencies such as the Opus codec should use compatible semantic-version
  requirements and the workspace lockfile rather than an unnecessary exact
  requirement.
- Leave the externally published Telegram relay unchanged for now.
- Collapse same-process loopback HTTP boundaries: Kennedy-owned orchestration
  calls Kmap, intelligence, conversation history, audio ingress, and Telegram
  identity through in-process service handles. HTTP remains a browser adapter;
  the published Telegram relay remains the sole temporary loopback exception
  until its crate exposes an in-process service API.
- Use one durable memory-ingress queue for conversation Chatend archives and
  prepared audio transcript pieces. The source libraries submit ordered jobs;
  the shared queue alone owns claiming, provenance binding, checkpoints,
  retries, failures, and completion. Keep source-record fields as compatibility
  mirrors while existing browser APIs still expose them.

## Kmap DB Core

- The `kweb-db-core` crate is a standalone Rust library package that owns only
  Kmap storage. `kennedy-server` imports its published release and exposes the HTTP adapter under
  `/api/v1/kmap`; the Kennedy-owned application routers share that listener.
- The core API is limited to opening/initializing a database path, creating
  provenance, creating/updating nodes with an existing provenance ID, reading
  nodes/provenance/full provenance history, and returning extensible stats.
  Stats are typed in Rust with private fields and stable getters and serialize
  to additive JSON whose unknown fields clients must ignore.
- Nodes store a required `last_modified_at`, ordered `fixed_connections` ID arrays,
  and ordered `recent_connections` ID arrays. The library assigns no active/fanout,
  fixed-slot, root-role, system-prompt, user, or graph-operation meaning.
- The backend session treats the first eight recent connections as active and
  the remainder as fanout, maintains order, and implements connect,
  consolidation, and fixed-connection workflows through sequential ordinary
  updates. The frontend mirrors that projection for rendering. These
  multi-call workflows are intentionally non-atomic.
- Root-role/user mappings live outside the Kmap database. Creation may use a
  self-owner sentinel; ordinary create/update is the only ownership mechanism.
  Core operations are serialized by the caller and are not concurrency-friendly.
- Every explicit mutation requires a caller-generated random 16-byte
  `IdempotencyId`. A successful write stores a permanent receipt atomically;
  an exact retry no-ops and succeeds, while reuse for a different operation or
  normalized request conflicts. Node replays return current state and
  provenance replays return the original provenance ID. Each call in a
  multi-call workflow uses its own stable ID, which makes individual retries
  safe without making the workflow atomic.
- This storage is a Kweb (knowledge web), not a Kennedy-specific database. The
  database path is caller-supplied; Kennedy defaults it to
  `data/kweb-db-core.sqlite3` and supplies the sibling
  `data/kweb-provenance-artifacts/` directory.
- Provenance media and large provenance payloads live outside SQLite. Artifact
  filenames preserve the original safe basename with exactly 12 random
  URL-safe Base64 characters inserted before the final extension; the first
  two suffix characters form a shard folder. The database retains relative
  filenames, original names, content types, sizes, hashes, roles, and order.

## Offline Backups

- `kennedy-server backup` creates a timestamped gzip-compressed tar archive of
  all six SQLite databases, including the shared memory-ingress queue, the complete Kweb provenance-artifact and
  audio-ingress media trees, and the
  encrypted credential vault when present.
  Each archive is self-describing: its README starts with the creating commit
  hash and records the exact schemas and current data-format semantics.
- Backups are deliberately offline. Before reading persistent data, the backup
  command binds the configured Kweb HTTP address and serves a maintenance page
  for the full operation. Normal server startup acquires that same address
  before opening any persistent state, so an active or competing instance
  fails before the databases can be touched.
- Backup creation verifies standalone SQLite snapshots and publishes the final
  archive atomically. The user is responsible for moving archives to durable
  off-machine storage.
- `backup --lightweight-kweb` intentionally omits Kweb provenance artifact
  bytes but retains the database and metadata. Full backups verify every
  referenced copied artifact; lightweight backups cannot reconstruct
  externally stored provenance without a separate artifact-tree backup.

## Kmap Size Estimate

- `kennedy-server kmap-size` reports a deliberately approximate total token
  footprint for current Kmap nodes. It reports both all three node text fields
  and long descriptions alone, while excluding history, provenance,
  connections, and all non-node tables.

## Durable Vnote Audio Ingress

- Historical `vnote-ingress` scans must cache local SHA-256 results and avoid
  rereading unchanged large recordings; the cache must invalidate when file
  identity or content-related metadata changes.
- An i3 hotkey runs `arecord` directly into `/home/user/media/vnotes`. The stop
  hotkey ends `arecord`, then checks the five newest vnotes by SHA-256 and
  uploads any Kennedy has not accepted. Once Kennedy has durably accepted the
  bytes, the script never waits for later processing.
- Audio job identity and originals survive shutdown. SHA-256 is the durable
  identity and lookup key so a large historical recording archive can skip
  files Kennedy has already accepted or ingressed even when renamed. An
  in-progress external-library transcription does not preserve step progress;
  Kennedy restarts it from the retained original after shutdown.
- Kennedy ingresses the resulting transcript pieces individually and in order.
  Every piece and provenance record must prominently carry the exact vnote
  recording timestamp, distinct from upload or ingress time, so Kennedy can
  distinguish historical or superseded claims from current Kmap knowledge.
- The `Audio Ingress` UI tab lists every retained audio job. Selecting one shows
  its latest external-job progress, persisted transcript artifacts, and all
  durable Kennedy ingress history.
- Audio ingress is explicitly fallible: speech, translation, annotations, and
  speaker identity may be seriously wrong. Kennedy preserves uncertainty and
  dated contradictions, and may create useful clarification notes or concrete
  tasks when important context is missing rather than blindly overwriting
  newer knowledge.

## Model-Readable Kmap Context

- Keep model-readable Kmap text compact and role-based. Direct nodes retain
  their descriptions but refer to connections by identifier. Classify the
  complete projection before rendering it, then emit all directly loaded nodes,
  all remaining full active-connection nodes, all remaining direct-node fanouts
  with name and short description, and finally all remaining fanouts of full
  active-connection nodes by name alone. A node appears in only its highest
  applicable section within one projection. For an envelope containing several
  `LoadNode` calls, execute the calls in order but project their combined delta
  once in this same section-wide order, including any status upgrades.
  Deduplicate that delta against earlier Chatend memory output. Richer structured
  summaries may remain in recovery state and the memory UI, but never serialize
  those structures into model-facing Chatend text.
- `LoadNode` and `ResetContext` must share the same selected-first,
  role-deduplicating load projection and renderer. `ResetContext` is only a
  context clear followed by that loader over the automatic roots and the
  explicitly retained nodes; it must not rebuild memory through a separate
  whole-snapshot rendering path.
- Keep exact context occupancy and remaining-window telemetry visible in the
  Chatend header. Also report provider cache reads, cache writes, and total
  token usage where available.

## Tools and Provider Execution

- Use the ChatGPT-authenticated Codex CLI, so Kennedy consumes Codex
  subscription limits rather than a billed
  OpenAI API key. On this deployment, invoke it only through the host's
  `codex-safe` launcher in `/home/user/podman`; that launcher keeps Codex in the
  same persistent Podman sandbox rather than installing or invoking Codex on
  the host. Keep `gpt-5.6-sol` with `xhigh` reasoning.
- Register only the provider-native `call_ktool` function. Codex may request
  several calls before its next inference; execute them sequentially in
  provider order and return each result through its matching native call ID.
- Give Kennedy `WebSearch(question, mode)` and `WebFetch(url)` as shared
  read-only tools in conversation, history ingress, and audio ingress. Kennedy
  chooses `balanced` by default, `fast` for simple latency-sensitive lookups
  where reduced research quality is acceptable, and `quality` for difficult,
  high-stakes, cross-source, or conflict-resolution research. Search language,
  geography, freshness, and domains remain natural language in the question;
  the compiled mappings are `quality` = `gpt-5.6-sol`/`xhigh`, `balanced` =
  `gpt-5.6-terra`/`low`, and `fast` = Gemini 3.1 Flash-Lite with grounded
  Google Search and a small amount of latency-conscious thinking.
- Eliminate the tracked runtime configuration file. Stable provider, model,
  audio, web, secret-name, and safety defaults belong in code. Keep only
  genuinely deployment-specific values such as listeners, data/source paths,
  and the encrypted vault path as `kennedy-server` CLI options.
- Continue append-only conversation and history-ingress rounds using Codex
  thread IDs. `ResetContext` starts a fresh Codex thread with Kennedy's rebuilt
  logical context.
- Run ordinary Kennedy generations through `codex-safe` and Codex app-server
  under a bounded deadline, with no shell, file, MCP, environment, plugin, or
  internet tools. Give only the dedicated WebSearch run internet access.
- Eliminate controllable non-Chatend prompting on every call: leave developer
  runtime and developer instructions empty, blank provider base instructions and model message
  templates, disable personality and project instructions, omit skill,
  permission, app, collaboration, and environment instruction blocks, and
  disable optional tools, plugins, goals, browser/computer features, hooks,
  shell snapshots, and elicitation features. Ordinary generation disables web
  search; a dedicated hosted-research turn retains only its required search
  capability. Do not disable bundled skills through a setting that mutates
  shared Codex state.
- Explicitly disable Codex's experimental `request_user_input` registration.
  Codex still registers `update_plan` and `view_image` for environment-backed
  turns despite every exposed switch being false; current Codex exposes no
  supported setting that removes those final forced schemas. Do not add an
  invisible instruction to compensate for them.
- Derive a sanitized model catalog from the live `codex-safe debug models`
  result by blanking `base_instructions`, removing `model_messages`, disabling
  model-selected skill instructions, and removing Codex's agent-tool selectors
  (`tool_mode`, `multi_agent_version`, and `apply_patch_tool_type`). Verify that
  every model's advertised effective context limit is identical before using
  it. Probe the sanitized catalog and model-visible prompt through the launcher;
  abort startup rather than falling back if either boundary cannot be verified.
- The `codex-safe` container boundary must expose the host temporary directory's
  `kcode-codex-catalogs` subdirectory at the same absolute path, read-only.
  This lets the backend pass the verified live catalog without copying mutable
  provider state into the application.
- Reuse Codex threads and report cache reads where Codex exposes them. Do not
  automatically compact or reset context; resets remain under user or Kennedy
  control.
- Discover each configured model's effective context window from Codex's
  advertised model metadata at startup and fail closed when it is unavailable.
  Do not substitute a locally invented window. Set Codex's documented
  auto-compaction threshold beyond any reachable context for every Kennedy
  generation so a provider thread cannot silently compact Kmap material.
- Return actionable, sanitized provider errors, including request IDs when
  useful, without exposing credentials or other sensitive provider data.
- Keep operational logs concise and action-oriented. Emit one timed log line
  for an entire LLM call, one for each Kennedy tool call, and one aggregate
  user-turn line. In the Chatend, add only one compact line after each LLM or
  tool call and one final line with total turn time plus combined LLM/tool time;
  do not repeat those calls in a step list.
- Telegram's HTTP request timeout must exceed its long-poll timeout. Healthy
  idle long polls must not produce warning noise; genuine polling failures may
  still warn and retry.

## Frontend Behavior

- History-ingress activity is appended after its conversation transcript in
  the same scroll container, as a natural continuation rather than an overlay
  or independently scrolling panel. Its header and usage row are ordinary
  content that can be scrolled past.
- Keep the message composer editable while Kennedy is generating or running
  tools so the user can draft the next message, but keep Send disabled until
  Kennedy completes the current turn.
- Persist every active conversation through a conversation-history backend.
  Checkpoint each user query before any LLM request and restore unfinished work
  on startup.
- Persist the entire structured Chatend, not only clean dialog: system prompts,
  retained messages, loaded memory, tool requests/results, counters, usage, and
  future serializable media blocks or attachment references. Use a versioned
  lossless JSON archive, restore it exactly, and store that complete archive as
  conversation provenance for history ingress.
- In history ingress, parse that durable recovery archive and place only its
  canonical human-readable `messages` text under `Archived Chatend`. Never
  expose the archive's JSON envelope, media data URLs, counters, diagnostics,
  or other recovery bookkeeping to Kennedy. ResetContext rebuilds this text and
  starts a fresh provider transcript, so removed Kmap nodes and tool results do
  not enter the new context or current Full view.
- Kennedy has logically separate API domains for Kmap, intelligence, and
  conversation history. The Kmap domain is now an adapter in `kennedy-server`
  over the imported storage library, rather than an independent backend
  server; other services retain their own state and databases.
- Show live history-ingress tool requests and results inline after the owning
  conversation so the user can follow memory updates in one continuous scroll.
- Store the complete history-ingress Chatend on its owning conversation record.
  Show live or archived ingress activity only when that closed conversation is
  selected; never carry it into another live chat.
- At the top of each live or archived history-ingress review, summarize the
  number of successfully added nodes, successfully updated nodes, and
  successful `ConnectNodes` calls. Do not count failed tool attempts.
- Show durable conversation history in a sidebar and allow completed
  transcripts to be reopened read-only.
- When a conversation ends, keep that closed conversation selected while its
  history ingress unfolds. Never select or create a replacement conversation
  automatically; the user can select another existing chat or press `New`
  when ready. Continue to run required history ingress sequentially in the
  background without disabling other live chats.
- Serve the local frontend without reusable browser caching and version its
  entry assets so HTML and JavaScript revisions cannot be mixed.
- Surface startup exceptions as visible failures instead of leaving the UI
  frozen at “Starting…”.
- On backend orchestration startup, permanently discard conversation
  placeholders where the user never sent a message. A conversation becomes
  durable history as soon as it contains its first user message; started
  conversations must not be removed by this cleanup.

## Concurrent Conversations and Serialized Memory

- Superseding the earlier single-unfinished-conversation behavior, allow any
  number of durable live conversations. The user can switch among them and
  create a new one even while Kennedy is responding in another; preserve one
  unsent in-memory draft per live conversation.
- Conversation sessions are read-only with respect to the Kmap. Their complete
  tool set is `LoadNode`, `ResetContext`, `WebSearch`, and `WebFetch`.
  `ConnectNodes`, `ConsolidateFanout`, `SetFixedConnection`, `CreateNode`, and
  `UpdateNode` are history-ingress-only. This supersedes the earlier decision
  to teach the mutation tools in both manuals.
- History and audio ingress can use `WebSearch` and `WebFetch` in addition to
  their Kmap mutation tools.
- Explicit End closes a conversation immediately. Also close a conversation
  after more than 24 hours without a user message only when the user
  successfully sends in another conversation. Do not treat viewing, switching,
  or typing as activity, and never time out a turn Kennedy has not answered.
- Queue closed conversations oldest-user-activity-first and run history ingress
  sequentially, with at most one Kmap-mutating ingress session at a time. Live
  conversation reads remain available during Kmap updates.
- A failed ingress attempt must release its in-progress claim before waiting
  for an automatic retry, so other eligible conversation or audio jobs keep the
  pipeline moving. Retry failed ingress after roughly 15 seconds; do not shorten
  the normal Codex generation timeout. Terminal failures may be retried
  manually from their preserved checkpoints.
- Mark sidebar records clearly as live/continuable or closed/read-only.
- Hide the entire message composer when a closed/read-only conversation is
  selected; do not show a disabled textarea for an unavailable action.
- Let the user resize the live message box substantially from either its top
  edge or lower-right corner, and provide an explicit one-click larger mode for
  composing long messages to Kennedy.
- On startup, idempotently remove the obsolete singleton-conversation index
  even from databases already marked migration v2; an early v2 build could
  recreate that index after the version advanced.
- On transcript rerenders, follow new content only if the reader was already at
  the bottom. Otherwise preserve the exact scroll offset so status/tool updates
  do not interrupt reading.
- Put timestamped, deduplicated user-visible operational errors below the
  history sidebar. Do not inject unrelated failures into the currently selected
  conversation; retain the log until the user clears it.

## Web Search Recovery

- Treat a completed hosted-search answer as successful even when the provider
  returns only a URL-less live-data feed (such as time, weather, finance, or
  sports). Preserve and display HTTP(S) citations whenever they are present.

## Kmap-Learned Harness Strategy and Dual Roots

- Keep Kennedy's identity and the mechanical harness instructions separate.
  The identity explains who Kennedy is and that harness strategy is learned
  through the Kmap. The mechanical prompt assets stay concise; strategic
  judgment belongs in Kennedy's graph. The later Layered System Prompt Assembly
  clarification defines the current file boundaries and composition order.
- Give Kennedy a durable MVP root node alongside the user's root. Both roots
  load automatically in every conversation and history-ingress context and
  survive every `ResetContext`; neither is included in reset arguments.
- Replace the former per-user seven-node model with one shared maximum of ten
  directly loaded nodes across both graphs. Kennedy chooses the allocation.
  Active-connection expansions do not count as direct loads, and existing
  per-turn/session LoadNode request budgets remain enforced.
- Tell Kennedy in the conversation-mode manual that completing a conversation
  retains its recovery archive for the separate read-write history-ingress
  mode. Ingress extracts the archive's canonical human-readable application
  messages; the exact provider JSONL and recovery envelope are not prose input.
- Expose both the user root and Kennedy root as direct navigation options in the
  frontend memory explorer.
- Let Kennedy optionally pass `selfMessage` to `ResetContext`, capped at 400,000
  characters. Preserve successful reset notes as assistant-role conversation
  messages across later resets, ordered after earlier history and before the
  mandatory roots and explicitly retained nodes. Count each ResetContext as one
  call against the same 20-call conversation-turn or 50-call ingress-session
  budget as LoadNode. Keep a compact Chatend history of every successful reset,
  grouping duplicate retained-node name sets so Kennedy can recognize loops.
  Preserve every short-ID assignment for the life of the session across resets
  and recovery, even when its node is not retained in the rebuilt context.
- Keep the 100-model-round history-ingress safety limit cumulative across
  checkpoints and retries. A reset starts a fresh provider thread but does not
  reset that session-wide guard.
- Give the outer history-ingress worker five failed attempts for one logical
  session. Persist every concise failure diagnostic and, on the fifth, move the
  record to a terminal failed state that is excluded from the retry queue.
- End every model-facing Chatend request with one terse line:
  `context window usage: {used-or-unknown} / {advertised-effective-limit}`.
  Full view exposes the line in its exact position inside the outbound JSONL.
  Do not add percentages, remaining-token prose, or explanatory wording to this
  recurring clue.

## Automatic Model Attribution

- Track the latest model and thinking mode responsible for every knowledge
  node creation, update, or graph mutation, using a combined value such as
  `gpt-5.6-sol-xhigh`.
- Derive and attach this metadata automatically in the backend tool executor. It must not be
  a Kennedy-managed field or a model-visible tool argument, and the conversation
  history backend has no role in it.
- Persist attribution in Kmap storage, expose it with full node data, and
  show it in Kennedy's Kmap context and the human memory UI.
- Add a dynamic system-prompt element telling Kennedy which model and thinking
  mode is executing the current conversation or history-ingress session.

## Telegram Relay and Voice Input

- The published Rust Telegram relay is transport and durable queue only: it
  must not construct Kennedy's prompt, translate Kmap identifiers, run tools,
  or receive non-conversational Chatend content. The backend orchestrator owns
  each Telegram Chatend and continues processing while no browser is open.
- Add `TG Bot` beside Conversation and Memory. Reuse ordinary conversation
  mechanics and support parallel Telegram users as distinct sidebar sessions,
  but dynamically and durably label browser sessions `conversation` and bot
  sessions `telegram` so Kennedy explicitly knows the current surface.
- `/reset` is the explicit end boundary for Telegram. It queues the full
  Telegram Chatend for normal sequential history ingress. Both provenance and
  the history-ingress prompt must distinguish an archived Telegram session from
  an archived UI conversation. Do not apply the UI conversation's 24-hour idle
  closure rule to Telegram sessions.
- Initially authorize only `@taek42`. Use that username solely to bootstrap the
  first binding, then persist and authorize the stable numeric Telegram user
  ID even if the username changes. Refuse unpaired users without storing their
  message content. Design storage so more allowed users can later have separate
  parallel conversations.
- After every newly crossed 100,000-token line in current Telegram context,
  send a separate notice containing current and maximum context usage and
  suggesting `/reset`. This notice is delivery metadata and must not enter the
  Chatend.
- Add voice notes to both the normal UI composer and Telegram. Preserve the
  original audio. The intelligence backend owns capability detection and paid
  transcription: only transcribe when the selected active model transport
  cannot ingest audio natively. The current `gpt-5.6-sol` transport is
  text/image-only, so use OpenAI's higher-quality paid
  `gpt-4o-transcribe` API rather than local Whisper, and clearly label the
  transcript presented to Kennedy.
- Accept PDF, DOCX, spreadsheet, CSV, and text attachments in both the browser
  composer and Telegram. Convert them locally to bounded readable text for the
  Chatend while retaining the original file and useful metadata. Searchable
  PDFs are required; image-only PDFs should fail clearly when OCR is needed.
- Quality web searches can take substantially longer than ten minutes, so
  their deadline is 40 minutes. Double the prior balanced and fast deadlines
  to 180 and 90 seconds respectively because both tiers have timed out in
  ordinary use.
- Make PDF upload discoverable through an explicitly labeled composer button.
- During history ingress, show Kennedy's final review normally but start
  Kennedy tool requests, memory tool results, and protocol-error details
  collapsed until the user expands them.
- A failed Telegram document extraction must receive an error reply and finish
  that relay event so one document cannot block all later messages for the user.
- A live conversation must expose a kill control while Kennedy is responding.
  It must stop the current model or web operation and prevent further agent-loop
  retries; the already-checkpointed user query remains preserved for an
  explicit retry.

## Main Chatend View

- Keep Full view as the exact, uninterrupted outbound Codex JSONL passthrough.
- Keep Main view as the default inspector: ordinary user/Kennedy conversation
  stays visible while system context, the loaded-node set, each node, tool calls,
  tool results, node-load events, and other under-the-hood activity are collapsed
  by default and expandable inline.
- A directly loaded node may expose the full node returned for each active
  connection. Connections of that active-connection node remain summary-only,
  matching the one-hop context-loading boundary.
- Replace the separate System prompts, Tool calls, and Memory tree inspector
  tabs with Main view; retain the independent durable Memory explorer.

## Full History Inspector

- Keep Full History alongside Main and Full. Main represents the current
  post-reset application context, while Full remains the exact current outbound
  provider transcript.
- Full History durably retains each outgoing Main-view context when
  `ResetContext` succeeds and places a visible reset barrier before the next
  context. Multiple resets create multiple ordered segments and barriers.
- Keep Full History data inspector-only: old segments must not re-enter
  Kennedy's generation context or history-ingress provenance prompt.
- During history ingress, Main and Full must follow the current live or saved
  ingress session through completion. Full History follows as well, preserving
  both conversation and ingress reset segments so the entire process can be
  traced in the UI.

## Main-View Timing Presentation

- Do not render latency messages as independent rows in Main or in the
  Main-style segments of Full History.
- Merge LLM and tool timing into a compact footer at the bottom of the related
  tool result. For an ordinary expanded conversation response, show timing as
  small secondary text beside the message heading.
- Keep Full view unchanged so it continues to expose timing text at its exact
  position inside the outbound JSONL.

## Main-View Density

- A `LoadNode` result should render one collapsed event row for the directly
  requested node. Do not automatically add its full active-connection nodes as
  sibling rows; expose them only inside the requested node's expansion tree.
- In Main and the Main-style segments of Full History, truncate Kennedy
  responses only when they exceed 500 Unicode characters. Show the first 500
  followed by an expandable `[...]` control that reveals the full response.
  Keep user messages, responses of 500 characters or fewer, and Full view
  unchanged.

## Layered System Prompt Assembly

- The backend chooses the inference provider before it composes Kennedy's
  prompt. When that selected provider's explicit kind is `codex`, include the
  concise `CodexHarness.txt` layer describing the native `call_ktool` boundary.
  Do not include or require this layer for other provider kinds.
- Supersede the earlier three-file prompt design with small, single-purpose
  prompt layers assembled in this order: Kennedy identity, one session type,
  Kmap basics, read-only tools, writable tools when allowed, and current
  runtime details.
- Keep session-local identifier stability rules, always-loaded-root behavior,
  and the native `call_ktool` contract in `KmapBasics.txt`. That layer must also tell Kennedy
  that additional tools and more detailed tool documentation may be available
  in the Kmap.
- Keep all shared read-only tools—including Kmap reads and web research—in
  `ReadTools.txt`, and ingress-only mutations in `WriteTools.txt`. Do not
  repeat tool contracts in session files.
- Use exactly one minimal session file per invocation:
  `ConversationSession.txt`, `SelfTimeSession.txt`, `HistoryIngressSession.txt`,
  or `AudioIngressSession.txt`.
- Expose a force-purge control for a conversation that remains live but cannot
  be recovered or resumed. Purge must permanently delete its durable history
  record without history ingress, so the stuck conversation neither reappears
  in history nor updates the Kmap.

## Multi-User Telegram Identity and Groups

- Supersede the earlier private-only Telegram authorization design. Keep
  Telegram identity-to-root mappings in a dedicated SQLite database; the Kweb
  database remains limited to Kmap data. A normalized whitelisted handle owns
  a reserved, initially blank Kmap root. Its first observed matching Telegram
  account pins the stable numeric user ID under TOFU, after which the numeric ID
  is authoritative and a different account presenting that handle is refused.
- Seed only `@taek42` as an unresolved privileged handle. Do not special-case
  David in backend request paths. The frontend maps the web UI to that handle's
  root (and preserves the existing legacy user root); David's eventual numeric
  Telegram ID is learned through the same TOFU path as every other handle.
- `/adduser @handle` is available only to the numeric identity pinned to the
  initial privileged entry. It immediately whitelists the handle and reserves
  a blank root; there is no registration, onboarding, or code-driven user
  interaction beyond the command.
- Do not add static prompt warnings, confidentiality partitions, or Kmap access
  controls. These users are trusted and Kennedy may load any participant's
  root. Group membership/session facts belong to dynamic Chatend context.

## Node Ownership, Fixed Connections, and Persistent Group Sessions

- Every knowledge node has a nullable owner-root field. Valid owners are the
  self-owned Kennedy root, a user root, or a group root. Existing non-root rows
  with no owner remain `unowned`; Kennedy sees that state and must assign an
  owner when updating the node. Newly created nodes require an owner.
- Replace task-connection terminology and priority semantics with three
  arbitrary numbered fixed-connection slots. Kennedy may set, replace, or clear
  slots 1, 2, and 3. Preserve the existing reserved connection-order storage so
  no edge rewrite is required.
- Kmap topology hygiene belongs to Kennedy. The harness should not
  automatically promote connections or otherwise clean the graph; Kennedy can
  assign fixed connections when an edge is important.
- Keep one active session per `(group root, Telegram user)`, distinct from that
  user's DMs and sessions in other groups. The session remains open after
  replies and `/reset` closes and queues only that exact session for history
  ingress.
- Group context warnings must identify the relevant `@username` and explicitly
  say that other members have separate context. Refresh a retained session with
  unseen group messages and participant roots without duplicating the current
  invoking message.
- Give invoked group voice notes and supported documents the same durable media,
  transcription, and extraction behavior as DMs. Voice notes invoke by replying
  to Kennedy; documents may invoke by bot mention in their caption or by reply.
  Queue heads are isolated per group user so one stuck session does not block
  other participants.

## Passive Group Context, Session Endings, and Ingress Coordination

- Supersede invocation-only group context. Archive every accepted group message
  and durably append it, without generating a response, to every open Kennedy
  session in that group. Include other users' voice notes and attachments and
  Kennedy replies produced for other users; catch up before an invocation so
  Kennedy sees the intervening discussion.
- Before a group `/reset` closes the invoking user's session, append all unseen
  group messages through the reset. If a user goes more than 50 group messages
  without invoking Kennedy, silently catch up and reset that one session into
  history ingress without sending anything to Telegram.
- Add a browser `Send & end` action that checkpoints one final user message
  without generating a Kennedy response, then immediately queues the complete
  Chatend for history ingress.
- Keep memory-ingress lifecycle orchestration outside the frontend. The shared
  MemoryIngress service owns cross-source queue selection, claims, checkpoints,
  retries, failures, and completion.

## Autonomous Self Time

- Present autonomous self time in its own browser category tab, alongside
  Conversation, TG Bot, and Audio Ingress. Its start panel accepts an optional
  user prompt, persists it with the run, and repeats it in every clean-slate
  slice. Use that prompt and the session number for sidebar titles. Retain
  `free-time` as the durable internal session type for existing records.
- In self time Kennedy is broadly told to have fun and may use the complete
  read, web, and Kmap write tool set.
- Default a run to 30 minutes while allowing a manually entered duration for
  short tests and overnight use. All clean-slate sessions in one run share a
  persisted absolute deadline; opening a new session never resets or reduces
  the remaining time.
- Use `EndSession` to archive the current self-time
  session and immediately open a new clean Chatend if time remains. In self
  time it may include an optional non-empty `message` of at most 400,000
  characters; checkpoint and deliver that message to the next session only.
  Ordinary prose does not end or roll over the session.
- Inject a Chatend timer notice once a live session enters its final three
  minutes. At the deadline block substantive tools, retain `EndSession`, and
  allow one wrap-up response; abort any remaining intelligence
  operation two minutes later, with each
  provider request capped to that same hard-stop window.
- Persist and restore active self-time work through Conversation History,
  serialize execution across browser tabs, and give each run mutation
  provenance. A completed clean-slate session becomes read-only history
  directly; do not send it through normal history ingress because self time
  already performs its own Kmap memory work.
- Make starting visibly single-flight: disable the control and show a
  `Starting…` state before the first network wait, and have Conversation History
  reject creation while any `free-time` record is already active so separate
  browser contexts cannot create overlapping runs.
- Preserve the intelligence backend's request allowance for each provider
  profile (including long quality searches), shortened only when the remaining
  self-time deadline plus two-minute shutdown grace is smaller. Do not impose a
  blanket 90-second cap on autonomous work.
- Reconcile asynchronous Conversation History list responses monotonically by
  record version, and adopt the latest record after a checkpoint conflict, so a
  late stale response cannot pin every later tool checkpoint to an old version.
- When Kennedy yields a self-time slice, open another clean-slate session only
  if at least five minutes remain in the shared run; otherwise end the run.
- Keep Telegram streams independent in the backend worker: do not await one
  complete fetched batch before polling again. Continue enforcing strict order
  within each private-user or group-user stream while other stream heads run.
- Recover a processing Telegram event whose Conversation History record is
  missing by compare-and-swap rebinding it from the exact stale conversation
  ID to a fresh record. Preserve any stored media and transcription so the
  original query completes instead of requiring a replacement chat group.
- Give every Telegram event a durable 30-minute processing deadline. If no
  complete response is ready by then, cancel Kennedy's active turn, atomically
  complete that event as a timeout, clear only its matching session pointer,
  notify the Telegram chat on a best-effort basis, and close any saved pending
  turn into history ingress so later events in that stream can proceed.

## Telegram Repository and Storage Boundary

- Keep user management in Kennedy. The published Telegram library supplies
  handles and numeric Telegram IDs when known, accepts whitelist/security
  inputs from its consumer, and does not own Kennedy users or Kmap-root policy.
- Telegram transport owns private session pointers, opaque stable group IDs,
  chat-ID history, member
  ledgers, group-security decisions, cursors, events, and message/ingress
  archives. Kennedy's user directory owns whitelists, observed identity, user
  roots, and the mapping from an opaque Telegram group ID to a local group root.
  Kmap root IDs must not be persisted in Telegram transport storage.
- The Telegram crate is a pure library. Kennedy unlocks and retains
  the encrypted credential vault, then passes the Telegram bot token into the
  library's initializer; the library does not open the vault or own secret
  storage.
- Use a reversible, fail-closed historical-membership gate. Retain every human
  identity ever observed in a logical group's membership history even after
  departure or removal. Kennedy
  may interact only when the roster is known complete and every historical
  identity is currently whitelisted. A newly observed unauthorized identity
  returns the group to quarantine; whitelisting all historical identities makes
  it eligible again.
- While a group is quarantined or its roster is incomplete, discard its message
  updates before inspecting, downloading, logging, archiving, or exposing their
  content to Kennedy. Membership/service updates may still be processed so the
  gate can eventually become satisfied. Because the Telegram Bot API cannot
  enumerate ordinary members, inability to prove the initial roster complete
  must remain fail-closed rather than silently enabling the group.
- Kennedy will always be made an administrator of every Telegram group she is
  added to. The relay must still verify that status at runtime and fail closed
  if Telegram reports otherwise.

## Backend-Owned Orchestration

- All Kennedy orchestration belongs in the backend. The frontend only presents
  backend-owned state and submits durable user intents; it must not own prompt
  composition, Chatend or session state,
  agent/tool loops, checkpoints, retries, work queues, self-time execution,
  Telegram processing, or memory-ingress coordination.
- Backend workers must continue conversations, Telegram relays, self time, and
  history/audio memory ingress without an open browser. Multiple browser tabs
  are independent views of the same authoritative backend state rather than
  competing orchestrators.
- Keep only inherently local presentation/input concerns in the frontend, such
  as rendering, navigation state, unsent drafts, media capture, and forwarding
  explicit user commands or uploads to the backend.
- The browser creates durable conversation or self-time intents, queues
  message/retry/end/stop commands, and polls the
  resulting checkpoints; `kennedy-server` owns every live Chatend and all
  continuation, tool-loop, retry, timeout, and close behavior.
- Run read-only web conversations and Telegram stream heads as independent
  concurrent backend tasks. There is no global read-session lock: one user,
  browser conversation, or Telegram stream must not wait for unrelated model
  or read-tool work, while ordering within one durable conversation/stream is
  still preserved.
- Put every Kmap-writing workflow behind one backend single-writer gate for
  now. This includes ordinary and Telegram history ingress, background group
  ingress, audio ingress, autonomous self time, and the Kmap mutation needed
  to provision Telegram user/group roots. The browser never owns or competes
  for this queue. Read-only sessions remain concurrent while the one active
  writer completes.
- Kennedy is a Rust backend with a vanilla browser JavaScript/CSS/HTML
  frontend. Backend orchestration must be implemented natively in Rust; do not
  add Node.js or another server-side JavaScript runtime or process. JavaScript
  remains browser-only.

## Credential-Bearing Dependency Security

- Treat every third-party dependency that receives an API key, access token,
  bot token, or equivalent reusable credential through any of its APIs as
  security-critical. Pin it to one exact immutable version or revision; a
  version range and lockfile alone are insufficient.
- Closely audit the complete source of the exact version before first importing
  or using the dependency, and repeat that audit before every version bump.
  Record the audited version or revision and the safety conclusion; automated
  scans do not replace the source audit.

## Vnote History Ordering

- Sort the Audio Ingress vnote history by recording-start time from most recent
  to least recent, independently of upload or ingress time.

## Native Codex Turns and Exact Provider Transparency

- Audio tools and isolated web research use the published
  `kcode-codex-runtime`; Kennedy conversation generation uses the separately
  published `kcode-codex-runtime-v2`. Neither implementation lives in this
  repository.
- Use Codex app-server's standard turn and dynamic-function protocol for
  Kennedy conversation generation. Register one function named `call_ktool`;
  its arguments contain one Ktool name and one object-valued Ktool arguments
  field. Do not register every Ktool separately and do not add a nested
  `call_ktools` batch API.
- A model may issue several native `call_ktool` invocations before its next
  inference. Preserve Codex's native delivery behavior. Parallel execution is
  not required; sequential delivery and execution are valid because outputs
  from calls in one model response cannot feed sibling calls.
- Return every Ktool result through the matching native tool-call response and
  let Codex continue inference on the same provider turn. Do not add generic
  mutation idempotency; mutating Ktools retain their own protections.
- A terminal assistant response normally completes a browser or Telegram turn.
- Use `EndSession` for history ingress, audio ingress, and self time. Terminal
  prose without it receives a minimal controller continuation. `EndSession`
  may appear alongside other Ktool calls and fails if another call in its
  native call group fails. Its optional self-time handoff message retains the
  existing validation and rollover semantics.
- Keep the live system prompts minimal and consistent with their existing
  style: explain only `call_ktool`, relevant Ktool contracts, ordinary terminal
  conversation responses, and `EndSession` where applicable.
- Full view is a transparency surface, not a human-friendly rendering. For
  current archives it must be byte-for-byte identical to the complete UTF-8
  JSONL Kennedy's backend sent to Codex, including exact serialization and
  trailing newlines for initialize, thread start/resume, turn input, and native
  tool-result responses. It must never parse, reconstruct, pretty-print, hide,
  add, relabel, reorder, or otherwise transform those bytes.
- The exactness requirement is scoped to the provider client boundary. When
  Codex is the provider, display exactly what Kennedy sent to Codex. Include
  additions Codex exposes to the client; do not claim visibility into content
  Codex or its upstream provider adds internally and does not expose. Legacy
  records may retain their old backend-hydrated plaintext compatibility view,
  but it must never replace or alter a current exact provider transcript.

## Chatend Context Usage

- The Chatend context-usage display shows the latest available measurement of
  current context-window occupancy, not cumulative tokens consumed across the
  provider thread. Keep cumulative token totals separate as usage telemetry.

## AudioIngress and MemoryIngress Boundary

- `kennedy-audio-ingress` remains Kennedy-specific and accepts a
  `kennedy-memory-ingress` queue, but it owns only durable audio intake,
  delegation to the external transcriber, and persistence and preparation of
  the final transcript. Its interaction with MemoryIngress ends after inserting
  the immutable transcript payload or ordered transcript pieces into that queue.
- MemoryIngress owns the submitted payload and every later claim, checkpoint,
  retry, failure, repair, and completion transition. The orchestration worker
  must consume audio payloads directly from MemoryIngress instead of fetching
  pieces from, or proxying lifecycle operations through, AudioIngress.
