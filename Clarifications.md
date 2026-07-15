# Project Clarifications

These user-provided decisions supplement the repository specifications and
technical design. They may be consolidated or removed once represented by the
canonical documents; this file is not an append-only log.

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
- Give live-conversation Kennedy `WebSearch(question, mode)` for delegated
  hosted research and `WebFetch(url)` for inspecting one public page. Kennedy
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
- History ingress cannot use `WebSearch` or `WebFetch`; it must process the
  archived conversation and Kmap context already in front of it.
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

## Web Search Recovery

- Treat a completed hosted-search answer as successful even when the provider
  returns only a URL-less live-data feed (such as time, weather, finance, or
  sports). Preserve and display HTTP(S) citations whenever they are present.

## Kmap-Learned Harness Strategy and Dual Roots

- Replace the former shared Kmap manual and agent-mode manuals with
  `KennedyIdentity.txt`, `ConversationManual.txt`, and `HistoryIngress.txt`.
  The identity explains who Kennedy is and that harness strategy is learned
  through the Kmap. Each mode manual stays concise and contains only exact mode,
  Kmap, tool-protocol, argument, and hard-limit mechanics; strategic judgment
  belongs in Kennedy's graph.
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
