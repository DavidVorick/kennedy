# Project Clarifications

These user-provided decisions supplement the repository specifications and
technical design. They may be consolidated or removed once represented by the
canonical documents; this file is not an append-only log.

## Offline Backups

- `kennedy-server backup` creates a timestamped gzip-compressed tar archive of
  all four SQLite databases, the complete audio-ingress media tree, and the
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

## Kmap Size Estimate

- `kennedy-server kmap-size` reports a deliberately approximate total token
  footprint for current Kmap nodes. It reports both all three node text fields
  and long descriptions alone, while excluding history, provenance,
  connections, and all non-node tables.

## Durable Vnote Audio Ingress

- An i3 hotkey runs `arecord` directly into `/home/user/media/vnotes`. The stop
  hotkey ends `arecord`, then checks the five newest vnotes by SHA-256 and
  uploads any Kennedy has not accepted. Once Kennedy has durably accepted the
  bytes, the script never waits for later processing.
- Audio jobs and originals survive shutdown and resume gracefully. SHA-256 is
  the durable identity and lookup key so a large historical recording archive
  can skip files Kennedy has already accepted or ingressed even when renamed.
- Split each recording into equalized, ordered windows no longer than four
  minutes, with fifteen-second overlap. The four-minute limit is a deliberate
  Gemini fidelity choice despite longer advertised support. Gemini 3.1 Pro
  Preview produces structured speaker-aware transcripts with original text,
  per-line English translations for non-English speech, and useful annotations.
- Give every piecemeal transcript to `gpt-5.6-sol` with `xhigh` reasoning in
  exact chronological order. Tell it about the overlap and trust it to produce
  the canonical complete transcript, reconcile speakers, and remove repeated
  boundary speech. If the final transcript exceeds an estimated 50,000 tokens,
  Sol chooses sensible, not necessarily equal, boundaries before Kennedy sees it.
- Kennedy ingresses the resulting transcript pieces individually and in order.
  Every piece and provenance record must prominently carry the exact vnote
  recording timestamp, distinct from upload or ingress time, so Kennedy can
  distinguish historical or superseded claims from current Kmap knowledge.
- The `Audio Ingress` UI tab lists every retained audio job. Selecting one shows
  its complete preparation artifacts and all durable Kennedy ingress history.
- Audio ingress is explicitly fallible: speech, translation, annotations, and
  speaker identity may be seriously wrong. Kennedy preserves uncertainty and
  dated contradictions, and may create useful clarification notes or concrete
  tasks when important context is missing rather than blindly overwriting
  newer knowledge.

## Chatend and Inspector

- The Chatend is the canonical human-readable application text supplied to
  Codex for Kennedy. The Full inspector and the intelligence request use the
  same formatter, so the Full view shows every application-controlled
  plaintext byte rather than an approximation, a JSON serialization, or
  transport diagnostics. Codex and its upstream provider may still add forced
  system content or structured tool metadata outside the application's
  inspectable boundary. Reveal and minimize everything the deployment exposes,
  but do not claim visibility into hidden provider/runtime prompts. A durable
  "Chatend archive" is a separate versioned recovery object; it is never sent
  to Kennedy as JSON.
- Show readable system and conversation text, memory context, Kennedy's
  ordinary-text JSON tool requests, and readable tool results. Hide provider
  response IDs, credentials, and non-context bookkeeping.
- Organize system instructions as prose sections and loaded Kmap nodes and tool
  results as clear YAML-like text rather than serialized JSON.
- Keep model-readable Kmap text compact and role-based. Direct nodes retain
  their descriptions but refer to connections by identifier. Emit each full
  active-connection node once with its long description and no short
  description. Emit direct-node fanouts once with name and short description,
  and fanouts found only beneath active nodes once by name alone. Richer
  structured summaries may remain in recovery state and the memory UI.
- Provide Full, System Prompts, Tool Calls, and expandable Memory Tree views.
  The Tool Calls view contains each transparent tool request and its readable
  response in chronological order. Distinguish directly loaded nodes, full
  nodes included through active connections, and summary-only fanout
  references.
- Keep exact context occupancy and remaining-window telemetry visible in the
  Chatend header. Also report provider cache reads, cache writes, and total
  token usage where available.

## Tools and Provider Execution

- Use the ChatGPT-authenticated Codex CLI, so Kennedy consumes Codex
  subscription limits rather than a billed
  OpenAI API key. On this deployment, invoke it only through the host's
  `codex-safe` launcher in `/home/user/podman`; that launcher keeps Codex in the
  same persistent Podman sandbox rather than installing or invoking Codex on
  the host. Keep `gpt-5.6-sol` with `xhigh` reasoning. Kennedy's Kweb
  tools remain ordinary, human-visible text protocol rather than provider-native
  function or custom-tool APIs.
- A Kennedy response may request multiple tools. Execute them sequentially in
  written order and return readable results to the model.
- Treat tool-request output as an exclusive response mode: the marker must be
  first, exactly one JSON envelope follows, and its closing brace is the last
  non-whitespace character. Never mix narration or status text with a tool
  request; return specific protocol feedback for leading or trailing prose.
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
- Run ordinary Kennedy generations through `codex-safe` and non-interactive
  `codex exec` under a bounded deadline, read-only sandbox, no approval prompts,
  and no shell/file tools. Give only the dedicated WebSearch run internet access.
- Minimize exposed Codex scaffolding on every call: use a terse inline base
  instruction, disable personality and project instructions, omit skill,
  permission, app, collaboration, and environment instruction blocks, and
  disable optional tools, plugins, goals, browser/computer features, hooks,
  shell snapshots, and elicitation features. Ordinary generation disables web
  search; a dedicated hosted-research turn retains only its required search
  capability. Do not disable bundled skills through a setting that mutates
  shared Codex state.
- Explicitly disable Codex's experimental `request_user_input` registration.
  After the slim catalog is applied, stock Codex still registers
  `update_plan` and `view_image` for environment-backed turns; current Codex
  exposes no supported setting to remove those final core schemas. They remain
  downstream runtime scaffolding and are forbidden by Kennedy's terse base
  instruction.
- Derive a slim model catalog from the live `codex-safe debug models` result by
  removing only Codex's agent-tool selectors (`tool_mode`,
  `multi_agent_version`, and `apply_patch_tool_type`). Verify that every
  model's advertised effective context limit is identical before using it.
  Probe the filtered catalog through the launcher and fall back to prompt-only
  reduction with a warning if the sandbox cannot read it.
- The `codex-safe` container boundary must expose the host temporary directory's
  `kennedy-codex-catalogs` subdirectory at the same absolute path, read-only.
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
  or other recovery bookkeeping to Kennedy. ResetContext rebuilds this text as
  well as the Full inspector, so removed Kmap nodes and tool results disappear
  from both.
- Kennedy has three logically separate backends: Kweb, intelligence, and
  conversation history. They are independent services with separate APIs,
  listeners, state, and databases and must not call or access one another. They
  happen to be compiled into and hosted by one Rust binary for operational
  convenience; the frontend and architecture otherwise treat them as if they
  were unrelated processes.
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
- On every frontend refresh, permanently discard all conversation placeholders
  where the user never sent a message. A conversation becomes durable history
  as soon as it contains its first user message; started conversations must not
  be removed by this cleanup.

## Task Connections

- Existing Kweb databases require no schema migration or task backfill. Nodes
  without explicitly assigned task connections behave as though their task
  list is empty.
- Teach Kennedy `ConsolidateFanout` and `AssignTask` in history ingress only.
  A task connection is justified only by a clear need for concrete work to be
  completed; ordinary relationships, vague possibilities, and completed work
  do not belong in task slots.

## Concurrent Conversations and Serialized Memory

- Superseding the earlier single-unfinished-conversation behavior, allow any
  number of durable live conversations. The user can switch among them and
  create a new one even while Kennedy is responding in another; preserve one
  unsent in-memory draft per live conversation.
- Conversation sessions are read-only with respect to the Kmap. Their complete
  tool set is `LoadNode`, `ResetContext`, `WebSearch`, and `WebFetch`.
  `ConnectNodes`, `ConsolidateFanout`, `AssignTask`, `CreateNode`, and
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
- Initialize frontend features independently. A missing service or
  mode-specific prompt disables only the feature that consumes it. In
  particular, a missing audio-ingress prompt must not disable chat, memory,
  ordinary history ingress, audio preparation, or audio history.
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
  retains the human-readable Full-inspector Chatend text for the separate
  read-write history-ingress mode, which can integrate anything learned during
  that conversation. The durability archive around it is not model input.
- Expose both the user root and Kennedy root as direct navigation options in the
  frontend memory explorer.
- Let Kennedy optionally pass `selfMessage` to `ResetContext`, capped at 400,000
  characters. Preserve successful reset notes as assistant-role conversation
  messages across later resets, ordered after earlier history and before the
  mandatory roots and explicitly retained nodes. Count each ResetContext as one
  call against the same 20-call conversation-turn or 50-call ingress-session
  budget as LoadNode. Keep a compact Chatend history of every successful reset,
  grouping duplicate retained-node name sets so Kennedy can
  recognize loops despite short-ID reassignment.
- Keep the 100-model-round history-ingress safety limit cumulative across
  checkpoints and retries. A reset starts a fresh provider thread but does not
  reset that session-wide guard.
- Give the outer history-ingress worker five failed attempts for one logical
  session. Persist every concise failure diagnostic and, on the fifth, move the
  record to a terminal failed state that is excluded from the retry queue.
- End every model-facing Chatend request with one terse line:
  `context window usage: {used-or-unknown} / {advertised-effective-limit}`.
  The Full inspector uses the same line. Do not add percentages, remaining-token
  prose, or explanatory wording to this recurring clue.

## Automatic Model Attribution

- Track the latest model and thinking mode responsible for every knowledge
  node creation, update, or graph mutation, using a combined value such as
  `gpt-5.6-sol-xhigh`.
- Derive and attach this metadata automatically in the frontend. It must not be
  a Kennedy-managed field or a model-visible tool argument, and the conversation
  history backend has no role in it.
- Persist attribution in the Kweb backend, expose it with full node data, and
  show it in Kennedy's Kmap context and the human memory UI.
- Add a dynamic system-prompt element telling Kennedy which model and thinking
  mode is executing the current conversation or history-ingress session.

## Telegram Relay and Voice Input

- Add a Rust Telegram relay to the existing Kennedy binary, using a mature Rust
  Telegram library. The relay is transport and durable queue only: it must not
  construct Kennedy's prompt, translate Kmap identifiers, run tools, or receive
  non-conversational Chatend content. The browser remains the visible owner of
  each Telegram Chatend and therefore must be open for Kennedy to answer;
  messages queue durably while it is closed.
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
- Quality web searches can take longer than ten minutes, so their deadline is
  15 minutes; balanced and fast search deadlines should
  remain latency-oriented.
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

- Keep Full view as the exact, uninterrupted application Chatend passthrough.
- Make a new Main view the default inspector: ordinary user/Kennedy conversation
  stays visible while system context, the loaded-node set, each node, tool calls,
  tool results, node-load events, and other under-the-hood activity are collapsed
  by default and expandable inline.
- A directly loaded node may expose the full node returned for each active
  connection. Connections of that active-connection node remain summary-only,
  matching the one-hop context-loading boundary.
- Replace the separate System prompts, Tool calls, and Memory tree inspector
  tabs with Main view; retain the independent durable Memory explorer.

## Full History Inspector

- Add Full History alongside Main and Full. Main and Full continue to represent
  only the current post-reset Chatend; Full remains the exact generation
  passthrough.
- Full History durably retains each outgoing Main-view context when
  `ResetContext` succeeds and places a visible reset barrier before the next
  context. Multiple resets create multiple ordered segments and barriers.
- Keep Full History data inspector-only: old segments must not re-enter
  Kennedy's generation context or history-ingress provenance prompt.
- During history ingress, Main and Full must follow the current live or saved
  ingress Chatend through completion. Full History follows as well, preserving
  both conversation and ingress reset segments so the entire process can be
  traced in the UI.

## Main-View Timing Presentation

- Do not render latency messages as independent rows in Main or in the
  Main-style segments of Full History.
- Merge LLM and tool timing into a compact footer at the bottom of the related
  tool result. For an ordinary expanded conversation response, show timing as
  small secondary text beside the message heading.
- Keep Full view unchanged so it continues to expose timing messages at their
  exact positions in the canonical Chatend.

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

- The frontend chooses the inference provider from backend metadata before it
  composes Kennedy's prompt. When that selected provider's explicit kind is
  `codex`, include a concise `CodexHarness.txt` layer explaining that Codex is
  an inner wrapper, its API/tool limitation claims may be wrong, and Kennedy's
  tool calls are caught by the outer harness. Do not include or require this
  layer for other provider kinds.
- Supersede the earlier three-file prompt design with small, single-purpose
  prompt layers assembled in this order: Kennedy identity, one session type,
  Kmap basics, read-only tools, writable tools when allowed, and current
  runtime details.
- Keep temporary identifier rules, always-loaded-root behavior, and the exact
  tool-call text protocol in `KmapBasics.txt`. That layer must also tell Kennedy
  that additional tools and more detailed tool documentation may be available
  in the Kmap.
- Keep all shared read-only tools—including Kmap reads and web research—in
  `ReadTools.txt`, and ingress-only mutations in `WriteTools.txt`. Do not
  repeat tool contracts in session files.
- Use exactly one minimal session file per invocation:
  `ConversationSession.txt`, `HistoryIngressSession.txt`, or
  `AudioIngressSession.txt`.
- Load prompt assets independently so a missing mode-specific file disables
  only the session type that requires it.
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
- A Telegram group is usable only while Kennedy can maintain a complete member
  ledger and every member is a TOFU-valid whitelisted identity. Kennedy must be
  a group administrator. Any observed unknown member, handle/ID conflict, loss
  of administrator monitoring after activation, or incomplete membership
  ledger permanently blacklists that chat ID. Later whitelisting or membership
  changes never revive it; users must create a new group.
- Treat Telegram's canonical `GroupAnonymousBot` sender, or a `sender_chat`
  equal to the current group, as group-authored transport rather than a member
  identity. It must never enter TOFU, the whitelist, or the member ledger and
  must not cause blacklisting. Preserve ordinary anonymous-admin messages in
  group context and background ingress, but do not let them invoke Kennedy
  because they have no identifiable invoking-user root. Handle both halves of
  Telegram group-to-supergroup migration as service updates, not user traffic.
- Invoke Kennedy in an allowed group only when a message mentions her bot
  handle or replies to one of her messages. Every invocation is an independent
  `telegram-group` session. Assign every observed group its own reserved blank
  Kmap root. Load the invoking user's root, the group's root, and Kennedy's root
  in that order; register every other current participant root as a
  session-local reference Kennedy may choose to load. Dynamic context
  identifies the group and all participants/root identifiers and includes the
  latest 50 group messages. `/reset` remains private-DM-only.
- After more than 100 non-invocation group messages, queue the oldest 80 since
  the last invocation or batch for background history ingress, leaving a
  20-message buffer. Background batches load the group root and Kennedy's root,
  since they have no invoking user. Group roots survive Telegram chat-ID
  migration and permanent blacklisting. Group invocations and background
  batches use normal durable Conversation History recovery and sequential Kmap
  ingress.
- Do not add static prompt warnings, confidentiality partitions, or Kmap access
  controls. These users are trusted and Kennedy may load any participant's
  root. Group membership/session facts belong to dynamic Chatend context.
