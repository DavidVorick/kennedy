# Historical Project Clarifications (Partly Superseded)

This file records pre-overhaul behavior and remains useful for unrelated
provider, audio, Telegram, and deployment decisions. Its Conversation History,
Chatend, ResetContext, context-budget, and Kweb-persistence statements are not
current requirements.

Current Chatend authority is `UserSpecification.md`, `TechnicalDesign.md`,
`Frontend/Specification.md`, the
[published `kcode-session-history` 0.1.0 specification](https://docs.rs/crate/kcode-session-history/0.1.0/source/Specification.md),
`chatend-overhaul/chatend-overhaul-clarifications.md`, and
`chatend-overhaul/chatend-discussion-review.txt`.

## Managed Rust Library Context

- Keep one complete managed Rust source snapshot in Kennedy's active context.
  `create` and `open` establish a stable stateful box, and successful `write`
  calls update that box rather than adding another complete-source box.
- Retain exact write arguments in durable invocation history, but project only
  a compact write-call receipt into later Kennedy inputs. Failed writes do not
  change the source box. Existing summarized or dehydrated representations
  remain under Kennedy's control when the canonical source changes.

## Intelligence Library Boundary and Accounting

- Replace KennedyServer's same-process intelligence Axum/JSON layer with the
  managed `kcode-intelligence-router` typed Rust library. Remove its health
  check, provider catalog, unused legacy generation paths, and other machinery
  that is unnecessary once callers use the library directly.
- Every model-backed operation takes the exact requested model identifier, not
  only a provider name. Kennedy may explicitly select models such as a
  particular Gemini Pro or Flash release; the library resolves the provider
  internally and rejects models or media capabilities it does not support.
- Every model-backed operation also requires a stable Kennedy user identifier.
  Persist exactly one JSON usage receipt file per provider call, including
  failed calls for which metering is unavailable. Daily UTC totals open and
  iterate all receipts without folders or indexes, grouped by exact actual
  model and user and split into non-cached input, cached input,
  thinking/reasoning, and visible output tokens. Background audio transcription
  and reconciliation make their actual Gemini and Codex calls through
  `kcode-intelligence-router`; `kcode-audio-ingress` owns no provider client and
  receives only typed model-call callbacks. Add folders or indexes only when
  the linear scan becomes too slow.
- Background audio chunk transcription remains pinned to
  `gemini-3.1-pro-preview`. Interactive search, transcription, and annotation
  take an exact supported model from Kennedy instead of a provider or
  quality-mode alias.
- Media inputs carry an explicit image, audio, or video kind independently of
  their declared MIME type. Normalize Ogg audio to an audio MIME type and do not
  let a misleading `video/ogg` declaration turn known audio into video.

## Session-log UI Authority

- The append-ordered `kcode-session-log` is the canonical source of session events
  across the source and history-ingress phases. The browser derives transcript,
  diagnostic, and ingress views from that one event stream and must partition
  phases at the durable source-termination and history-ingress markers.
- Session History control state remains the separate authority for lifecycle,
  failures, recovery checkpoints, staged Kweb plans, and commit receipts. The
  UI must not substitute source-phase events when ingress event activity is
  absent.

## Session History Component Boundary

- When separating and cleaning up Session History, prefer one small typed
  library boundary and err toward moving session/history ownership out of
  KennedyServer. KennedyServer and Session History should not both directly
  manage `kcode-session-log`; the Session History component should present the
  session operations Kennedy needs without exposing storage paths or
  path-shaped JSON service calls.

# Original Project Clarifications

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
  Chatend, Kmap, provenance, or memory-ingress behavior. AudioIngress persists
  the final transcript; KennedyServer splits it and submits each piece to
  Session History's memory-ingress lifecycle.

## Repository-Local Runtime Data

- Keep Kennedy's persistent runtime data inside the repository sandbox, but
  consolidate it under the single ignored `data/` directory so the repository
  root remains source-focused and the data remains inspectable by Codex.
- Do not add a `--data-dir` abstraction. Change the existing individual path
  defaults to locations under `./data/` while retaining those existing flags as
  overrides. Store live databases and their WAL/SHM companions directly in
  `data/`, audio originals in `data/audio-ingress-media/`, the transactional
  Kweb root in `data/kweb/`, managed Kcode sources and publications beneath
  `data/kcode/`, the encrypted vault in `data/`, and manual recovery material
  in `data/recovery/`.
  Generated whole-tree backup archives live outside `data/` so they cannot
  recursively include themselves.

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
- Submit prepared audio transcript pieces to Session History. Their claim,
  provenance, checkpoint, retry, failure, and completion state uses the same
  lifecycle as conversation history ingress. KennedyServer's one writer lane
  serializes all ingress work.

## Kmap DB Core

- Kennedy uses the published `kcode-kweb-db` 1.x crate through a compatible
  Cargo version requirement; `Cargo.lock` records the reviewed release used by
  reproducible builds without making the manifest an exact dependency pin. The
  retired `kweb-db-core` SQLite database is not a runtime dependency.
  `kennedy-server` owns the application adapter and root/user policy.
- `NodeData` atomically replaces the complete node: text, owner, ordered fixed
  connections, ordered recent connections, and object references. The core
  assigns no active/fanout, slot, root-role, user, or policy meaning and imposes
  no application-level connection-count limits.
- KennedyServer enforces node-text policy while the write session is still
  open: short names contain 4–50 Unicode characters, short descriptions at
  most 200, and long descriptions at most 5,000. `CreateNode` or `UpdateNode`
  rejects an invalid field without changing the staged Kweb plan so Kennedy can
  correct and retry it in-session. Final session commit and `kcode-kweb-db` do
  not re-enforce these application limits; sealed historical sessions remain
  commit-compatible.
- The Kweb is a canonical binary on-disk store rooted at `data/kweb/`. Current
  nodes and immutable objects use two-level sharded files. History, accepted
  transactions, state, and the append-only transaction log are binary and
  checksummed. Startup trusts current state and performs bounded WAL recovery;
  it does not rebuild the graph from the transaction log.
- Node IDs and object IDs are distinct type domains represented by canonical
  eight-character URL-safe unpadded Base64 strings. Kennedy now receives these
  real IDs directly; session-local numeric aliases no longer exist.
- Native signed transaction IDs make reception of the exact same transaction
  idempotent. Gossip transactions wait for all recursively referenced child
  transactions; local submissions with missing children fail. Gossip protocol
  integration remains later work.
- Kennedy provenance envelopes are canonical binary application records stored
  in immutable Kweb objects. Attached media are separate immutable objects.
  JSON is used at application API boundaries, not as an internal checksummed
  persistence encoding.
- Root-role and user/group mappings remain in `kennedy-users.sqlite3`. Kennedy's
  permanent writer private key lives only in the encrypted credential vault;
  the Kweb state contains the ordered public writer list.

## Offline Backups

- Replace the application-aware `kennedy-server backup` command with the simple
  `scripts/backup` Bash script. It first refuses to run while Kennedy's process
  or listener is active, then puts the complete opaque `data/` directory into
  one timestamped gzip-compressed tar archive while printing progress.
- Do not interpret, filter, snapshot, or selectively document individual
  persistence formats during backup. Unknown future files, manual recovery
  data, legacy archives, SQLite sidecars, and all Kweb objects are included
  automatically.
- Include one small metadata member with the Git commit and dirty-tree status.
  Recovery is deliberately source-assisted: restore the bytes alongside the
  last working source version and let Codex inspect and migrate formats then.
- Write backup output outside `data/` and publish it only after `tar` succeeds.
  The user remains responsible for durable off-machine storage. Codex
  authentication state and the original vnote source directory are external
  inputs rather than Kennedy-owned application persistence.

## Kmap Size Estimate

- `kennedy-server kmap-size` reports a deliberately approximate total token
  footprint for current Kmap nodes. It reports both all three node text fields
  and long descriptions alone, while excluding history, provenance,
  connections, and all non-node tables.

## Telegram Identity and Kmap Size Library Boundaries

- Extract the Telegram directory into the standalone Rust package
  `kcode-telegram-identity`. It owns the SQLite whitelist, observed identity
  binding, delegated additions, opaque group records, and user/group Kmap-root
  assignments behind a typed in-process API. It owns no HTTP routes, Telegram
  networking, Kweb root creation, orchestration, listener, or credential
  vault.
- Extract current-node Kmap footprint measurement and report rendering into
  the standalone Rust package `kcode-kmap-size`. KennedyServer retains the
  `kmap-size` CLI, maintenance exclusion, vault unlock, and configured Kweb
  opening.
- Maintain both packages as conforming `kcode-rust-libs-v2` libraries under
  `data/kcode/kcode-rust-libs/`, publish their releases to crates.io, and have
  KennedyServer consume exact published versions rather than workspace members
  or local path dependencies.
- Leave Kweb writer provisioning in KennedyServer for now; its boundary needs
  a separate design decision before extraction.

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

- Give Kennedy model-callable enrichment tools for newly supplied staged
  objects. The tools take the object's existing `pending:N` identifier and
  return supplementary text without replacing or modifying the authoritative
  original bytes.
- Images may be annotated through OpenAI, Codex, or Gemini at Kennedy's
  discretion. Native audio and video annotation uses Gemini. PDF, legacy DOC,
  and DOCX inputs use local text conversion. These annotations and conversions
  remain fallible source material in the ordinary Chatend tool history.
- Every provider-backed media annotation call includes an explicit,
  Kennedy-authored prompt describing what to inspect and the desired textual
  result. Do not hide annotation behavior behind a fixed server-side prompt;
  validate and bound the supplied prompt as part of the Ktool contract.
- User-sent audio, including browser voice recordings and current Telegram
  voice notes, must reach Kennedy as its original staged object without an
  eager transport-generated transcript. Kennedy decides whether to use
  OpenAI's dedicated transcription tool, Gemini native audio annotation, or no
  enrichment, and supplies the exact bounded prompt for either provider call.
  Background Telegram group voice notes must not be silently pre-transcribed.
- In Telegram groups, invocation controls when Kennedy takes a turn, not which
  retained thread media she can inspect. Media posted anywhere in the bounded
  group context must remain available to Kennedy even when its original
  message neither mentioned her nor replied to her; keep raw bytes out of
  model-readable context and expose them through staged or equivalently
  bounded tool-addressable objects.
- The initial model-readable document-conversion scope is PDF, DOC, and DOCX;
  adding other document formats is a separate decision even when the underlying
  upload path accepts arbitrary files.
- Use the ChatGPT-authenticated Codex CLI, so Kennedy consumes Codex
  subscription limits rather than a billed
  OpenAI API key. On this deployment, invoke it only through the host's
  `codex-safe` launcher in `/home/user/podman`; that launcher keeps Codex in the
  same persistent Podman sandbox rather than installing or invoking Codex on
  the host. Keep `gpt-5.6-sol` with `xhigh` reasoning.
- For an interactive `codex-safe` terminal launch, use the Git repository root
  as the workspace when one exists and otherwise use the current directory.
  Keep the noninteractive no-workspace failsafe separate, and wipe that
  failsafe directory immediately before every fallback mount.
- Register only the provider-native `call_ktool` function. Codex may request
  several calls before its next inference; execute them sequentially in
  provider order and return each result through its matching native call ID.
- Give Kennedy `WebSearch(question, model)` and `WebFetch(url)` as shared
  read-only tools in conversation, history ingress, and audio ingress. Kennedy
  chooses an exact supported Gemini or Codex search model rather than a
  `fast`/`balanced`/`quality` alias. Search language, geography, freshness, and
  domains remain natural language in the question;
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

- The browser application is a managed `kcode-web-libs` publication named
  `kcode-kennedy-ui`. KennedyServer's `/` response is only a tiny loader page:
  it imports the floating patch-compatible `kcode-kui-loader` Web library, and
  that loader navigates to the floating patch-compatible
  `kcode-kennedy-ui` page. Exact publications remain immutable, while a newer
  patch becomes live automatically through the existing SemVer routes.
- Do not add frontend activation state, an activation or rollback CLI, a
  static-asset fallback, or a second durability mechanism. Publication
  durability and exact-version retention belong to `kcode-web-libs`; a
  rollback may publish the desired older source as a higher patch release.
- Keep the loader deliberately small. A failed frontend upgrade is repaired
  through Codex and a subsequent immutable patch publication rather than
  through server-side fallback machinery.
- The historical `Frontend/SystemPrompts` files are backend orchestration
  assets, not browser application source. Move them beneath KennedyServer when
  the browser application moves into `kcode-kennedy-ui`.
- Do not expose per-action Chatend persistence state such as “saving” or
  “saved.” Accepted updates are appended directly to the session journal; no
  separate frontend state is shown for kernel writeback or power-loss
  durability.
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
- Treat browser history refreshes as generation-scoped observations: a response
  started before local conversation creation must not remove the new record or
  change its selection, and any necessary fallback selection renders
  immediately.
- Do not rebuild the transcript and Chatend inspector when observed history and
  command state is unchanged. Hydrate the selected archive immediately, hydrate
  older summaries through a bounded background queue, and update only their
  sidebar presentation as they arrive.
- Poll Session History more slowly while idle and retain one-second observation
  while commands, ingress, self time, retries, or audio processing are active.
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
  Preserve canonical node references for the life of the session across resets
  and recovery, even when a referenced node is not retained in rebuilt context.
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
- A failed Telegram document extraction must not discard an otherwise valid
  accepted file. Stage the original object, present extraction unavailability
  as bounded attachment context, and continue the relay event so one document
  cannot block all later messages for the user.
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
- Keep canonical identifier behavior, always-loaded-root behavior,
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
- Keep memory-ingress lifecycle orchestration outside the frontend.
  Session History owns queue selection, claims, checkpoints, downstream
  retries, failures, and completion for conversation and audio ingress.
  KennedyServer serializes their Kweb writes.

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
- Preserve the intelligence library's request allowance for each exact selected
  model operation (including long Codex searches), shortened only when the remaining
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
- Recover a retained Telegram group pointer whose Conversation History record
  is missing by compare-and-swap detaching only the exact
  `(group ID, Telegram user ID, conversation ID)` match. Complete an orphaned
  silent-reset item instead, so neither kind of missing downstream session can
  remain in the retry queue.
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
  let Codex continue inference on the same provider turn. A Ktool returns raw
  text; place that text directly in its ordinary result box and native
  response without a JSON envelope, Serde rendering, or pretty-printing.
  Managed Rust library tools use the same ordinary one-box result path. Do not
  add generic mutation idempotency; mutating Ktools retain their own
  protections.
- A terminal assistant response normally completes a browser or Telegram turn.
- Use `EndSession` for history ingress, audio ingress, and self time. Terminal
  ingress prose without it receives a private controller message explaining
  that the session is solo and giving the exact native `call_ktool` arguments
  `{"name":"EndSession","arguments":{}}`. If Kennedy reaches the existing
  tool-loop round limit without ending ingress, commit the staged transaction
  instead of retrying it. `EndSession` may appear alongside other Ktool calls
  and fails if another call in its native call group fails. Its optional
  self-time handoff message retains the existing validation and rollover
  semantics.
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

## Unified AudioIngress Boundary

- This older unified-database decision is superseded by the later standalone
  library boundary below.
- `kcode-audio-ingress` owns durable audio intake, delegation to the external
  transcriber, and the final transcript. KennedyServer owns immutable ordered
  transcript pieces and submits them to Session History's existing claim,
  checkpoint, retry, failure, repair, and completion lifecycle.
- KennedyServer consumes completed transcripts through AudioIngress status and
  owns splitting, the model loop, kcode-session-log/Kweb commit, and the global
  writer lane. There is no separate audio memory-ingress queue.

## Kweb DB 1.0 Migration Direction

- Kennedy will migrate from `kweb-db-core` to the transactional
  `kcode-kweb-db`, but Kennedy's existing model-facing navigation and mutation
  tools and their behavior remain a compatibility contract. The object store
  adds capability rather than replacing an existing feature.
- In `kcode-kweb-db` 1.0, `NodeData` is the complete authoritative node
  revision and includes ordered `Vec<NodeId>` fields named exactly
  `fixed_connections` and `recent_connections` alongside object references.
  `create_node` and `update_node` set or replace the entire node state; there
  is no separate connection mutation API.
- The core preserves both connection vectors and requires uniqueness within
  each, but imposes no Kweb-policy maximum count. Kennedy alone enforces its
  three contiguous fixed positions; finite-memory protection comes from
  checked decoding and general complete-record/transaction byte bounds rather
  than a connection-count policy.
- Kennedy alone interprets the first eight recent connections as active and
  the remainder as fanout. The generic database preserves order without
  assigning active, fanout, consolidation, root, user, or HTTP meaning.
- `NodeId` and `ObjectId` remain six bytes but use exact eight-character
  URL-safe unpadded Base64 canonical text and two/six-character sharded node
  and object paths. Their generated raw domains must be disjoint so a node and
  object cannot receive the same locator.
- The append-only transaction log remains on disk for recovery and audit and
  is never fully loaded, retained, or replayed by normal primary-library
  startup. Complete current nodes and immutable objects are authoritative
  sharded files; transaction/DAG metadata, history, and gossip work use
  disk-backed indexes and queues.
- Every committed internal format is a canonical, hand-written binary encoding,
  not JSON or Serde. Fixed-width fields, explicit tags and lengths, exact
  decoding, and SHA-256 integrity checks ensure one encoding and checksum for
  one logical record.
- A transaction is not committed or projected until every transaction named in
  its `heads` is already committed. This applies recursively. Missing-head
  transactions received through the explicit source-aware gossip path retain
  their canonical signed bytes in an in-memory dependency graph, stage
  validated final object envelopes on disk, and ask the submitting peer for
  the missing IDs. Ordinary non-gossip admission rejects missing heads as a
  caller error without staging anything. Waiting transactions do not enter the
  committed log, indexes, node/history state, object store, heads, or gossip
  queue.
- Local `start_transaction` snapshots every current database head, so Kennedy
  and its frontend do not track or inject a single "latest transaction."
- Waiting object envelopes are written only once. Admission stages the final
  envelope under `incoming/`; WAL preparation and final installation use hard
  links to the same inode, with checksum reads but no payload rewrite.
- Kennedy's ordinary read/mutation boundary uses typed Rust values. Internal
  database file encodings never cross it. Gossip deliberately carries opaque
  canonical signed transaction bytes and raw object bytes so peers authenticate
  exactly the bytes the writer signed.
- Waiting state is intentionally not durable: restart clears the object spool
  and the sender must resubmit. An exact duplicate of either a waiting or
  committed transaction returns success as a no-op and never processes the
  transaction twice.
- The complete gossip protocol is deferred until after the local Kennedy
  migration. Existing gossip admission, request, and outbox code is useful
  scaffolding, but its final serving, retry, and transport APIs are not a
  prerequisite for adopting the database locally and may be redesigned once
  the peer protocol is specified.
- IDs created by a local transaction belong to that mutation-locked,
  uncommitted transaction until finalize; dropping it abandons them. A signed
  gossip transaction instead arrives with canonical IDs already fixed.
  Concurrent creation at the same canonical node ID is conflict input, while
  an immutable object-ID collision is invalid, including collision with an
  object created by an ancestor. Node and object raw type domains remain
  disjoint.
- A minimal internal WAL makes the transaction-log append, object installation,
  complete affected-node replacements, history/index/head updates, and gossip
  enqueue atomic. Startup rolls forward only prepared WAL work and resumes
  queues; it does not rebuild or validate the complete database.
- The minimal WAL is sufficient for the initial migration. Generalized WAL
  optimization and an exhaustive crash/fault-injection matrix are deferred;
  separately operated offline recovery remains the fallback if an unusual
  failure exceeds the online roll-forward path.
- Gossip sends one newly committed or still-unacknowledged transaction package
  at a time. It never reannounces the complete historical database on open.
- Version 1.0 raises both the individual owned-buffer object payload limit and
  the aggregate object payload limit for one transaction to exactly 32 GiB
  (34,359,738,368 bytes). It supports 64-bit replicas only. Object bytes move
  through build, commit, storage, and gossip without cloning; WAL and outbox
  records reference staged/immutable files instead of copying payloads.
  Streaming larger objects and 32-bit replica support are deliberately
  deferred.
- Database-wide inspection, listing, statistics, migration, backup, and
  validation functionality belongs in separate Kennedy-owned tooling rather
  than new primary-library APIs. Test changes will likewise be requested later
  through separate narrowly scoped documents.
- Publish the reviewed library as version `1.0.0`, then have Kennedy audit the
  selected compatible 1.x release recorded in `Cargo.lock` before executing the
  data migration.
- The one-time live migration assigns every legacy node a fresh random
  canonical ID and uses an in-memory old/new table only long enough to
  translate owners, connections, application roots, and active-conversation
  references. No compatibility table or permanent legacy-ID mapping survives.
- Import the 1,000 current nodes in one signed migration transaction with
  provenance fields set to `migration`. Legacy node revision history and
  provenance are intentionally not imported; the old files are the archival
  record.
- Sign that import with an ephemeral migration key generated in memory. Retain
  its public writer ID in Kweb state for verification and peer whitelisting,
  but zeroize and discard the private key after import. The permanent Kennedy
  writer is generated later by a local script, written only into the encrypted
  vault, and added at first priority in the public writer list.
- Active conversations retain translated root/reference IDs but discard their
  cached Kmap context and directly loaded set so the next turn loads fresh
  canonical data. The two terminally failed conversations and their two
  mirrored MemoryIngress jobs are purged as explicitly authorized.
- After successful import and application-reference rollover, move the live
  legacy SQLite database, WAL/SHM files, and provenance artifact tree into a
  timestamped directory under `data/archive/`. Kennedy must not read that
  archive as runtime persistence.

## Chatend Overhaul Planning Direction

- Treat the material in `chatend-overhaul/` as a proposed large architectural
  change that must be compared with the current implementation before coding.
- The Chatend overhaul may rely on Kweb object storage, but its implementation
  follows the in-progress Kweb storage release rather than inventing an
  interim object store in Kennedy.
- Design the overhaul around explicit modular boundaries so its generic event,
  box, assembly, tool-state, and history-processing components can be separated
  into publishable kcode Rust libraries. Resolve the major architectural
  contracts and migration order before beginning the broad implementation.
- Kweb participates in the Chatend as an ordinary tool implementation. The
  generic controller must not contain Kweb-specific context behavior. Its only
  session-opening conventions are tool definitions supplied through the system
  prompt and automatic `LoadNode` calls for the user and Kennedy root nodes.
- V1 may use straightforward whole-context provider requests and basic cache
  optimization. More aggressive cache optimization is an important later
  efficiency project, but must not complicate the first correct box-based
  implementation.
- Every session remains independent and is the unit processed by exactly one
  history-ingress run. Do not introduce a conversation-segment layer. One
  session contains its opening root-node tool calls, user and Kennedy activity,
  Kennedy-managed boxes, and its eventual history ingress.
- Every model-visible context item exists in a box. Each user message and each
  Kennedy message creates exactly one ordinary box containing that message;
  there is no separate message record that also creates another box.
- Session operations are processed sequentially before the Chatend projection
  is rebuilt. Do not expose an `EventBatch` as a domain concept merely to
  describe that ordering.
- Native results for ordinary tools that create result boxes include those
  boxes in normal Chatend text format, including BoxId. Kennedy can therefore
  summarize, dehydrate, or hydrate a just-created result later in the same
  provider turn without guessing an event or box identity.
- Persist incomplete sessions and history-ingress checkpoints in
  Kennedy-owned local disk storage; never use Kweb's permanent object store for
  incomplete or transient session revisions.
- After history ingress completes, map that one session to exactly one
  `kcode_kweb_db::Transaction`. Bundle every Kweb node change produced by the
  ingress, the complete immutable session archive, and all user-supplied file
  objects into that transaction. Do not split one session across transactions
  or combine sessions in one transaction.
- Conversation History should retain only the small durable catalog needed for
  session lifecycle, commands, ordering, retry state, and final Kweb
  transaction/object identifiers, rather than a second permanent copy of
  completed conversation contents.
- Stage the pending Kweb transaction plan and its read-your-writes view in the
  locally durable in-progress session. Do not hold the actual
  `kcode_kweb_db::Transaction` open across model calls or restarts; construct
  and finalize it only when history ingress completes successfully.
- Set the live-session context budget to exactly 70% of the effective context
  window. Context accounting should be as accurate as practical while
  conservatively avoiding underestimation. Before retaining a user message,
  tool call or result, or Kennedy response, project the exact resulting
  Chatend. If it would exceed 70%, reject it and retain a bounded,
  user-visible system capacity message instead. If context after that message
  exceeds 75%, force-end the source and queue history ingress.
- Treat 75% of the ingress model's effective context window as the
  history-ingress *initial fitting target*, not its runtime ceiling. Before the
  first ingress inference, reduce eligible boxes largest-first until the
  projection is at or below that target. If only protected boxes remain, dehydrate
  them largest-first as well. If a fully dehydrated projection is still above
  the target, commit the transaction without starting ingress. Once ingress is
  running, Kennedy may use the remaining headroom, including through hydration
  and tool calls, up to 100% of the effective context window; only the
  full-window boundary force-ends and commits ingress. Compaction performed
  during an already submitted provider turn affects the next rendered request
  rather than retroactively shrinking that turn's input.
- Kennedy permanently retains session events, source contents, transformations,
  history-ingress activity, and archived Kweb objects. Purge and garbage
  collection semantics must not imply that accepted session data is erased.
- A session can receive and provide arbitrary objects. Represent either
  direction as `Object provided: <ID>` inside the owning user or Kennedy
  message box, never as model-visible content outside the box model.
- Browser, Telegram, and future communication adapters own transport-specific
  object ingress and egress, including supported types, limits, metadata, and
  delivery mechanics. Kennedy must see the active session's capabilities.
- The backend takes restart-safe local custody of newly accepted object bytes
  until successful history ingress commits them in the session's sole Kweb
  transaction. Kennedy may attach the resulting object reference to any number
  of nodes, retrieve such references later, and ask a capable communication
  adapter to deliver the object to the user.
- Give each accepted object's filename and media type one authoritative value.
  A transport may synthesize a safe filename once when its source supplies
  none; after staging, the pending object's stored metadata is authoritative
  and every downstream call receives that exact value. Enrichment adapters
  must not independently reconstruct filenames or require a response to echo
  filename/media-type metadata so Kennedy can identify or render the original.
  If a standalone adapter response includes echoed metadata, treat it only as
  optional consistency evidence and keep the staged object identity and
  metadata authoritative.
- For the first bidirectional-file release, use `EmitObject` with exactly one
  canonical object ID as its model-visible input. A successful call creates a
  Kennedy object-response box and may end a turn without a prose response;
  transport delivery happens afterward from that durable response.
- Store new files as small, self-describing Kennedy file envelopes inside
  ordinary Kweb objects so an object ID alone recovers filename, media type,
  and original bytes. Do not add a Kweb metadata table or a formal file-to-node
  relationship.
- Resolve exact known pending-object tokens in completed session archives and
  staged node descriptive text after the transaction builder allocates object
  IDs but before it creates or updates nodes and finalizes. Keep this in the
  same transaction; do not follow commit with a second node update.
- Browser uploads accept arbitrary files and document extraction is
  best-effort enrichment. Telegram accepts the relay's existing voice and
  document kinds plus the additive photo, video, animation, audio, video-note,
  and sticker kinds. Native and generic-document delivery remain deliberate,
  distinct choices; a failed native send is not reinterpreted as a document.
- Request an additive, nonbreaking Telegram-relay expansion for native photo,
  video, animation, audio, video-note, and sticker ingress/egress. Kennedy
  implements the cohesive API in the local 0.3.0 relay while retaining the
  existing `/file` route unchanged for explicit generic-document delivery.
- The pinned Kweb, session-log, Codex-runtime, and document-extraction releases
  already provide the remaining primitives required by this feature and need
  no upstream changes.
- When `write-file-freeform` captures model output without a final newline,
  append one automatically before previewing and writing the file.

## Standalone AudioIngress Library Boundary

- The published package is named exactly `kcode-audio-ingress`.
  KennedyServer consumes its crates.io release; the crate is not vendored or a
  member of this repository's Cargo workspace.
- Maintain it as a conforming `kcode-rust-libs-v2` library: its manifest is
  standalone with literal package metadata and explicit dependencies, root
  `Documentation.md` is the complete agent-facing API reference, generated
  Cargo state is not package source, and the standard README, specification,
  dependency audit, and license accompany publication.
- AudioIngress is a Rust library used by KennedyServer, not an HTTP component.
  It has no Axum, multipart, route, HTTP-status, or frontend-response API;
  KennedyServer owns those adapters.
- Give AudioIngress one persistence-root path. It owns and derives every
  database and media location beneath that root.
- Submit one owned audio byte vector with its minimal source metadata. The
  library performs no streaming and imposes no input-size limit; callers own
  transport and admission limits.
- Processing runs automatically after durable submission. Retry retryable
  processing failures at most five times, then remain failed until an explicit
  manual retry grants a fresh attempt budget. This policy is fixed rather than
  configurable.
- Keep the public surface minimal: open, submit, one status operation, and
  manual retry. Do not expose individual processing steps or require callers to
  drive the worker.
- AudioIngress ends at a durably completed transcript. KennedyServer owns
  transcript splitting and submits the pieces to Session History, which owns
  memory-ingress checkpoints, downstream failures, completion, and idempotency.
  Those concepts do not appear in the standalone AudioIngress API.

## Managed Web Libraries

- Extract immutable Web-publication SemVer selection and serving from
  KennedyServer into the standalone `kcode-web-semver-routing` Rust library.
  Maintain its source as a conforming `kcode-rust-libs-v2` library under
  `data/kcode/kcode-rust-libs/` alongside the other managed Rust libraries.
  The library owns the complete `/lib` and `/module` Axum subrouter; Kennedy
  configures the publication root and merges that router into its listener.
- Consume the published `kcode-web-libs` crate and expose a Ktool family as
  identical to `kcode-rust-libs-v2` as practical: create, open, docs, complete
  write, terminal-output `write-file-freeform`, delete-file, check, and
  publish. Rust and Web libraries retain independent stateful complete-source
  boxes.
- Store managed Web source beneath `./data/kcode/kcode-web-libs/` and immutable
  publications beneath the distinct
  `./data/kcode/kcode-web-libs-published/` root, relative to KennedyServer's
  process working directory. The simplified whole-`data/` backup script covers
  both roots; do not add special backup-system integration.
- Serve every file from immutable publications. A fileless
  `/lib/<name>/<selector>` request resolves and redirects to the selected
  publication's manifest-declared entry module. A file route preserves the
  requested relative path.
- Treat `v<major>.<minor>.<patch>` as exact. Treat abbreviated `v` selectors
  and unprefixed selectors as Cargo-compatible SemVer requirements, support
  the major `semver::VersionReq` expression forms, and select the highest
  published stable matching version. Unprefixed `1.2.3` therefore has Cargo
  caret semantics.
- Redirect floating routes to exact routes. Keep floating redirects uncached
  and exact published files immutable-cacheable. The expected consumers share
  the KennedyServer origin, so no Web-library-specific CORS policy is needed.
- Retain `/module/<name>/v<exact>/<file>` as a production compatibility alias
  for exact cross-library dependency URLs accepted by the crate's Chromium
  checker; `/lib` remains the documented public selection API.
- Maintain `web-libs-education.txt` as Kennedy's complete operational guide to
  the crate API, Ktool contracts, publication workflow, route/version
  behavior, and source constraints.

## Managed Kcode Development Tools and Rust Binaries

- Never write deployment or publication scripts for managed Kcode libraries.
  Kennedy already owns their check and publication workflow. When an
  unpublished managed library version is needed to build Kennedy itself,
  create a temporary local link beneath `data/kcode/` to that library's
  current managed source, use the link as Kennedy's path dependency, start
  Kennedy, and publish through Kennedy. After publication, replace the path
  dependency with the exact crates.io version and remove the local link.
- Consolidate managed Kcode state beneath `./data/kcode/`. Use
  `kcode-rust-libs/` for Rust-library source, `kcode-web-libs/` for Web-library
  source, `kcode-web-libs-published/` for immutable Web publications,
  `kcode-rust-bins/` for Rust-binary source, and
  `kcode-rust-bin-artifacts/` for published executables.
- Keep Rust-binary source and executable publications separate. Publish
  executables immutably by name and SemVer, reject a second publication of the
  same name and version, and resolve calls with the Cargo-style SemVer behavior
  used for Web libraries. Do not add checksums, quotas, or retained build
  artifacts beyond the published executable.
- Permit runtime networking. Do not impose build or call concurrency limits.
  Give calls a two-minute default timeout while allowing Kennedy to request a
  different timeout.
- Preserve a binary's text output exactly. Store binary output objects in
  Kennedy's object store and show their object IDs without wrapping text or
  object IDs in pretty JSON.
- Put the Rust-library, Web-library, and Rust-binary Ktool adapters and their
  shared session/lease behavior in the managed `kcode-dev-tools` Rust library.
  Kennedy consumes that library rather than retaining parallel adapter
  implementations in KennedyServer.

## Kmap-First System Prompts

- Preserve `KennedyIdentity.txt` exactly. Keep only the first paragraph of the
  conversation prompt and the first and last paragraphs of the self-time
  prompt. Simplify the history- and audio-ingress prompts while retaining their
  essential solo-session, writable-Kmap, and completion semantics.
- `KmapBasics.txt` must introduce the Kmap to a model with a completely fresh
  context window and no training knowledge of Kennedy's harness. Explain what
  the graph is, what nodes and connections mean, how canonical identifiers
  work, which roots load automatically, and that further instructions and tool
  manuals live in the Kmap. Do not put individual tool contracts in this layer.
- Keep only `LoadNode`, box hydration/dehydration/summarization, and ingress
  event hydration/dehydration in the read-only critical-tools prompt. Remove
  search, fetch, media staging/annotation/transcription, document extraction,
  and object emission from the system prompt; their manuals live in the Kmap.
- Retain `WriteTools.txt` and `CodexHarness.txt` as system-prompt layers.
- Add one Telegram prompt shared by private and group Telegram sessions and a
  second group-only prompt. Never include Telegram instructions in browser,
  self-time, audio, or other non-Telegram sessions.
- Avoid JSON and other structured data in model context whenever possible.
  Render Telegram group context and similar human-readable material as prose;
  preserve exact structured values internally for validation and execution.
  Existing exact machine contracts may remain where removing them would lose
  necessary precision.
- Alongside the model and thinking mode, inject the current date and time in
  natural language. Use a twelve-hour clock with an explicit `am` or `pm` and a
  timezone label so early-morning times cannot be mistaken for an ambiguous
  twenty-four-hour timestamp.
