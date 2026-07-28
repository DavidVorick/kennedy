# Project Clarifications

This document records user intention with regard to Kennedy.

## Overall Direction

- Kennedy is a local-first personal assistant whose durable memory and working
  state remain inspectable and recoverable by the user.
- Prefer the simplest design that preserves correctness, durability, and a
  clear trust model. Remove layers, services, compatibility machinery, and
  duplicated state when they no longer have a concrete job.
- Give every piece of durable state and every workflow one clear owner. Avoid
  two components independently interpreting or mutating the same persistence.
- Same-process components should normally communicate through small typed Rust
  APIs. HTTP belongs at genuine transport boundaries, not between parts of one
  process.
- Reduce KennedyServer by extracting cohesive Rust capabilities into managed
  Kcode libraries, starting with the least-coupled boundaries. An extracted
  library should normally contain 300-3,000 lines of code, expose a lightweight
  well-defined API, and materially simplify what KennedyServer owns; do not
  create pass-through crates that merely relocate server-specific glue. Prefer
  focused new libraries over casually making an existing managed library
  larger, but optimize for the simplest total architecture: extend the clear
  owner of a capability when doing so removes duplicate implementation or
  parallel ownership and leaves a lighter boundary between components. Judge an
  extraction by total first-party implementation as well as server size: move
  code and tests rather than copying them, reuse existing transports, and do not
  add abstraction or supporting code that consumes the deduplication benefit.
- Managed libraries maintained only by LLMs are source-first: their code and
  tests are the specification. Do not add a separate `Specification.md` that
  duplicates an ingestible codebase; retain only documentation required by the
  managed-library or publication boundary.
- The Rust backend owns orchestration. Browser code owns presentation and local
  input concerns only. Do not add a server-side JavaScript runtime.
- Prefer explicit, visible behavior over hidden automation. Failures should be
  actionable, state transitions inspectable, and destructive recovery steps
  deliberate.
- Build for current needs. Do not add abstraction, configuration, indexing,
  compatibility, concurrency, or fallback machinery solely for hypothetical
  future requirements.
- When an old persistence format or architecture is retired, preserve the
  original data for offline recovery, remove its live loader, and avoid carrying
  permanent compatibility state into the new design.

## Kmap and Durable Knowledge

- Kweb is a privacy-enforcing transactional graph and object store. Every node
  and object has an immutable-user-ID owner and independently stored audience
  policy, and Kweb includes those facts in signed transactions and enforces
  them on reads and writes. It does not resolve Kennedy-specific transport
  groups, roots, fixed/recent context layout, UI policy, or session behavior;
  callers supply the exact user and model audience for an operation.
- Kweb's root files and binary database records are private to
  `kcode-kweb-db`. KennedyServer and offline application tooling must use its
  public API rather than parsing or mutating those formats behind the
  library's lock and recovery boundary.
- Kennedy owns application graph policy, including roots, fixed/recent context
  layout, application validation, and model attribution.
  Kweb owns durable resource ownership, privacy evaluation, and owner-only
  mutation enforcement.
- Fixed connections are deliberately placed references, not task slots or a
  priority system. Graph hygiene belongs to Kennedy; the harness should not
  silently promote or rearrange connections.
- The context projection gives each explicitly loaded node one full box and
  each of its fixed connections one full box, without an arbitrary
  fixed-connection count cap. There is no active-connection category. All
  recent connections included in the projection are fanout-only and appear
  together in one globally deduplicated summary box containing each node's name
  and short description; they are never automatically promoted to full nodes.
  An explicitly loaded node uses the concise header
  `[box {box_id} | Kweb loaded node | hydrated]`: its identifier and short name
  belong in the node body rather than being duplicated in the header, and its
  Kweb resource owner belongs in that node data rather than exposing Chatend's
  internal tool ownership in the header. Loaded-node, fixed-connection, and
  staged-node boxes use the same full node-body representation and differ only
  in their declared box type. Full node text has no active-connection field.
- Use canonical Kweb node and object identifiers at every Kennedy boundary.
  Do not introduce session-local aliases for already-durable resources.
- Kennedy users share one graph, but Kweb itself filters every node and object
  read against the supplied audience. Its ordinary public API must not expose
  an unfiltered read path that application code can accidentally use. Ownership
  and privacy-policy data replicate transactionally with each resource;
  Kennedy resolves transport identities and session participation into the
  exact audience supplied to Kweb.
- A resource has six independently optional privacy rules: strict whitelist,
  permissive whitelist, and blacklist for users, plus the same three rules for
  models. Every configured rule must pass. Strict whitelist requires every
  present subject of that type to match, permissive whitelist requires at least
  one present subject of that type to match, and blacklist requires no present
  subject of that type to match. A resource with no configured rules is
  visible.
- Users are represented in privacy policy only by immutable exact identifiers.
  A group is not itself a policy subject: its audience is the complete set of
  users known to be present in the session, supplied as an array by Telegram or
  another transport. Group selectors are not supported.
- Model selectors use a bounded glob-like syntax rather than general regular
  expressions. With no `*` they match exactly; one `*` may appear only at the
  beginning, the end, or both to request suffix, prefix, or substring matching
  respectively. A `*` anywhere else is invalid.
- A node hidden from an audience behaves exactly like a nonexistent node:
  direct lookup returns the ordinary not-found result, and every visible
  node's fixed and recent connections, including their summaries and derived
  context, omit hidden targets without revealing that filtering occurred.
- Kweb's audience-scoped read API always filters its results while retaining
  the ordinary read method signatures. Policy changes are separate writes whose
  policy data is included in the resulting Kweb transaction. Each object has
  its own owner and independent privacy policy rather than inheriting either
  from a node.
- Every node and object records its owning immutable user identifier directly;
  ownership is not inferred through a root node. Kweb permits only that owner
  to mutate the resource or its privacy policy, and a valid policy must not deny
  its owner access.
- Privacy changes govern future reads but do not revoke a node already loaded
  into an existing session or rewrite durable session history.
- The database privacy upgrade is intentionally deferred. Its complete handoff
  is recorded in `kweb-privacy-upgrade.txt`; do not begin the implementation
  until the user explicitly resumes it.
- Kennedy and the relevant user or group each have durable roots. Those roots
  are the starting points for memory, identity, tool discovery, and operating
  knowledge.
- Kmap changes are staged with read-your-writes behavior and become globally
  visible only through an explicit transaction. One logical session produces
  at most one Kweb transaction and one immutable session archive.
- Pending nodes and objects may refer to one another during a writable session.
  Resolve them to canonical identifiers atomically in the final transaction;
  never leak pending identifiers into durable Kweb state.
- Preserve provenance automatically. The backend, not Kennedy's tool arguments,
  records the model and execution context responsible for graph mutations.
- Keep one global Kweb writer lane until there is a demonstrated need for
  finer-grained write concurrency. Read-only sessions should remain concurrent,
  and a session that becomes a writer must revalidate what it read.
- Kmap maintenance measurements may be deliberately approximate when their
  purpose is operational guidance rather than an exact accounting system.

## Sessions, Chatend, and Context

- Session History exclusively owns active session logs, lifecycle/control
  journals, pending session objects, and completion receipts. KennedyServer uses
  its typed API and must not manipulate the same files independently.
- A source conversation and its later history ingress are one logical session
  and one ordered event history. UI and model views are projections of that
  history, not independently authoritative transcript copies.
- Retain only the small completed-session catalog needed to find immutable Kweb
  archives and commit receipts. Do not keep a second permanent transcript store.
- Every item visible to Kennedy belongs to the box model. Canonical content,
  its current representation, and the history of changes to that representation
  are distinct concerns.
- When updating a box moves its current representation to a later position in
  the projected history, leave a minimal generic `[box updated]` placeholder at
  its earlier occurrence. This preserves visible tool-call/result continuity
  and makes clear that the call succeeded but its stateful output was superseded
  and moved forward. The marker is generic Chatend projection behavior, not a
  Kmap-specific notice box.
- Hydration, dehydration, and Kennedy-authored summaries must never destroy
  canonical content. If canonical state changes behind a compact
  representation, preserve Kennedy's representation choice and mark it stale.
- Stateful tool results, especially complete managed-library source, should
  revise one stable box rather than repeatedly adding complete copies. Failed
  writes must not revise that box. Exact invocation arguments remain durable
  without being needlessly projected into every later model request.
- Do not reintroduce a reset operation that discards the logical context model.
  Context should be managed through explicit box representations while the
  complete event history remains recoverable.
- Do not rely on hidden provider conversation state or silent compaction.
  KennedyServer constructs the provider-visible projection, enforces capacity,
  and retains enough exact boundary data to explain what the provider received.
- Capacity limits should reject or end work predictably rather than silently
  dropping accepted content. History ingress may reduce initial context while
  preserving canonical material for selective inspection.
- Treat current context occupancy and cumulative provider usage as different
  measurements. Do not present lifetime token consumption as though it were the
  amount currently occupying the model window.
- Conversation sessions read the Kmap but do not mutate it. Durable graph
  changes happen in explicitly writable history-ingress, audio-ingress,
  self-time, or other backend-owned sessions.
- Accepted session history and committed objects are permanent. A cleanup or
  UI convenience must not imply that durable accepted history was erased.
- Tool results should remain plain and direct. Avoid redundant JSON envelopes,
  pretty-printing, or duplicate generic result boxes when a tool already
  updates a canonical stateful box.

## Intelligence and Provider Boundaries

- All model-backed operations go through one typed intelligence boundary.
  Audio and other background libraries receive callbacks rather than owning
  provider clients or credentials.
- Provider API libraries own provider-specific transport, validation, and
  response normalization. `kcode-intelligence-router` is the application-facing
  entry point for every direct model call and owns provider selection,
  credentials, cancellation, model limits, and durable usage accounting.
  Higher-level agent and subagent libraries build on that router and own agent
  loops, context projection, and tool-callback semantics; they do not construct
  parallel provider clients or persist a second usage ledger.
- Kennedy exposes image creation and image modification as one model-call tool,
  analogous to media annotation. The caller chooses an exact supported image
  model and may supply existing image objects as references; generated images
  return through Kennedy's ordinary object and delivery paths. Provider-specific
  image transport and normalization remain below the intelligence router, and
  the router retains user attribution, cancellation, limits, and accounting.
- Generated and modified image objects must describe their encoded bytes
  truthfully end to end. Provider adapters must request a supported output
  format and must not label or name JPEG bytes as PNG, or vice versa.
- Kennedy chooses exact supported models and capabilities. Avoid vague
  fast/balanced/quality aliases and do not infer media capability solely from a
  MIME string.
- Kennedy may delegate a focused task through an explicit subagent tool. Each
  subagent uses Kennedy's chosen exact model through any configured
  tool-capable provider, not only the Codex harness, and begins with an
  intentionally box-free application context: the current long descriptions of
  Kennedy-selected canonical Kmap nodes, in Kennedy's selected order, followed
  by Kennedy's task prompt. The backend assigns no special role to any selected
  node; Kennedy controls whether a node's text acts as identity, system-like
  instruction, operating knowledge, a tool manual, or ordinary task context
  through her selection and ordering.
- Ordinary API model identifiers keep their provider-native names:
  `gpt-*`/other OpenAI identifiers use the OpenAI API and `gemini-*` identifiers
  use Gemini. Codex-harness models use the explicit `codex/<catalog-id>`
  namespace so an API model can never silently run through Codex. Do not limit
  API subagents to a hard-coded model allowlist: resolve model availability
  against the configured provider, and fail unavailable or ambiguous names
  explicitly.
- A subagent does not inherit the parent Chatend, boxes, transcript,
  automatically loaded roots or connections, ordinary Kennedy prompt layers,
  ambient host instructions, or provider conversation state. The backend
  resolves the exact selected nodes at launch and records an inspectable
  context manifest.
- Every Ktool otherwise permitted by the parent session is callable through
  the normal bridge, but the subsystem does not inject a Ktool catalog or any
  tool manuals. Kennedy must teach a subagent the relevant calls through the
  selected node descriptions or task prompt. Subagents cannot launch
  subagents; delegation strategy remains with Kennedy because she has the Kmap
  context needed to choose it.
- Stateful Ktools retain exact canonical history while projecting only one
  current value into the box-free subagent context. When a successful tool call
  revises that value, keep the earlier call in its existing concise successful
  representation when suitable, replace only the superseded bulky payload, and
  move the latest complete value to the bottom of the context after the call
  that produced it. Do not invent a special marker syntax when the ordinary
  representation is already compact, do not duplicate complete old and new
  values, and do not revise state after a failed call. This projection applies
  generically to managed source and any other box-aware tool.
- A successful subagent call returns its one terminal assistant response as the
  plain Ktool result to Kennedy. Do not wrap it in a redundant report, inject
  the child context or trace into Kennedy's active context, or treat the
  subagent's claim as proof that its task succeeded. Kennedy decides what to
  inspect or verify through her own context and tools. Child failure or
  cancellation returns an actionable tool error and must disclose when effects
  may already have occurred.
- Subagent effects remain subject to the parent session's permissions and
  transaction boundaries, and stopping the parent operation stops its active
  subagent work.
- Attribute every provider call to a stable Kennedy user and persist one usage
  receipt per call, including failed calls for which complete metering is
  unavailable. Cached input, uncached input, reasoning, and visible output must
  remain distinguishable where the provider reports them.
- Discover effective model limits from the provider boundary and fail closed
  when they cannot be verified. Do not invent local context limits or allow
  provider-side automatic compaction to silently remove Kennedy's Kmap context.
- Kennedy conversation generation uses the provider's native turn and tool
  protocol with one `call_ktool` bridge. Preserve native call identity and raw
  tool results rather than inventing a parallel tool protocol.
- The provider-visible prompt should contain Kennedy's intentional Chatend, not
  ambient repository instructions, plugins, shell state, credentials, or
  unrelated host capabilities.
- Codex runs through the constrained persistent launcher. Do not expose the
  host user's personal workspace, Cargo credentials, or broader filesystem just
  to make generation convenient.
- Web research and media analysis use explicit Kennedy-authored questions or
  prompts. Do not hide substantive judgment in fixed server-side prompts.
- A hosted-search answer can still be useful when the provider returns a
  URL-less live-data result. Preserve citations whenever they are available.
- Provider and tool failures should be sanitized but actionable. Operational
  logging should summarize calls and timing without duplicating the complete
  Chatend into noisy logs.

## Files, Media, and Audio

- Take restart-safe custody of every accepted original before enrichment.
  Transcription, extraction, and annotation are supplementary fallible text;
  they never replace or modify the authoritative original bytes.
- Once staged, filename, media type, transport kind, and object identity have
  one authoritative value. Downstream adapters must not independently
  reconstruct them.
- Keep raw media bytes out of model-readable prose. Kennedy sees bounded
  metadata and an object reference, then explicitly chooses an appropriate tool
  when the contents matter.
- Browser uploads may accept arbitrary files. Document extraction is optional
  enrichment and failure to extract must not discard an otherwise valid file.
- User-supplied voice and media reach Kennedy as originals without eager
  transport-generated interpretation. Kennedy chooses whether and how to
  transcribe or annotate them, including the exact model and bounded prompt.
- `AnnotateMedia` accepts any authorized compatible media object, whether it is
  staged in the current session or already canonical in the object store.
  Annotation availability must not depend on when or how the original entered
  storage.
- Model-backed media work must validate the actual media kind and capability.
  Treat transcription, translation, visual interpretation, and speaker
  identification as potentially wrong and preserve uncertainty.
- Store committed files in self-describing Kweb object envelopes so an object
  identifier is sufficient to recover safe metadata and exact bytes. Payload
  readers return the exact original bytes; the application storage envelope is
  never part of the returned payload. Resolve known pending-object references
  in the same transaction that commits the session.
- Application-level file and provenance payload envelopes stored inside opaque
  Kweb objects have one small typed library owner. KennedyServer decides when
  to store and transport them, but does not duplicate their binary codecs.
- Envelope codecs are canonical and fail closed: every encoder output must be
  accepted by its decoder, filenames are bounded and platform-independent,
  reserved markers never fall back to raw-payload interpretation, and
  untrusted declared sizes use checked arithmetic and fallible allocation.
- Native media delivery and generic-document delivery are intentional distinct
  actions. A failed native send must remain visible as that failure rather than
  silently changing the semantic delivery type.
- AudioIngress owns durable intake, retained originals, processing state,
  retries, and the completed transcript behind a small typed library API.
  KennedyServer owns transport admission, transcript splitting, and submission
  of pieces to Session History for memory ingress.
- Vnote scanning should avoid rereading unchanged large recordings. Durable
  acceptance is the handoff boundary; the recording does not need to wait for
  later transcription or memory work.
- Preserve the original recording time independently of upload, processing, and
  ingress times. Kennedy needs that distinction to reason about historical or
  superseded statements.

## Telegram Identity, Groups, and Transport

- The Telegram library is transport plus a durable ordered queue. It does not
  compose Kennedy prompts, run tools, mutate Kmap, own Kennedy users, or unlock
  credentials.
- Kennedy owns the user directory, whitelist, root assignments, and
  authorization policy. Bootstrap a handle through trust-on-first-use, then
  treat the stable numeric Telegram identity as authoritative.
- Telegram users are trusted participants in the shared Kmap. Do not bolt a
  separate confidentiality model onto Telegram sessions.
- Group identity is opaque and stable. Group roots belong to Kennedy's user
  directory; Telegram transport storage must not contain Kmap identifiers.
- Group participation fails closed. Kennedy may interact only when the bot's
  required administrator status and the group's historical human membership
  are sufficiently known and every observed participant is authorized.
- While a group is quarantined, discard message content before inspecting,
  downloading, logging, or archiving it. Membership updates may still be used
  to establish whether the group can become eligible.
- Keep a separate durable session for each participant and group, distinct from
  direct messages and from the participant's sessions in other groups. Preserve
  strict order within a stream while allowing unrelated streams to progress.
- Kennedy's main agent may initiate a private Telegram message to any explicitly
  targeted authorized user from any session, but only after that user has
  opened a private chat with the bot. The tool accepts the stable numeric user
  identity; Telegram transport resolves the private chat without exposing chat
  IDs to Kennedy or KennedyServer.
- An initiated private message belongs to the targeted user's already-active
  direct Kennedy session when one exists, preferring the transport's current
  session and then another private Telegram session; create a new private
  Telegram session when none is active. Record the Kennedy-authored message in
  that session and atomically bind successful delivery to it.
- Invocation controls when Kennedy responds, not which accepted bounded group
  context she may inspect. Passive group discussion and retained media should
  be available to an open participant session without pretending that messages
  from other participants belong to that participant.
- Raw retained group media stays behind an authorized staging tool. It should
  not be copied eagerly into every participant's model context.
- Reset and timeout handling must release stuck streams so later events can
  proceed. Preserve accepted work for ingress where possible and surface a
  useful failure to the Telegram chat.
- Telegram sessions end through their explicit transport boundary; browser
  inactivity rules must not silently close them.

## Browser and Presentation

- The browser is an observer and durable-command client. Backend conversations,
  Telegram work, self time, retries, and ingress continue without an open tab.
- Multiple tabs are views of the same backend state, not competing
  orchestrators. Reconcile stale observations monotonically so a late response
  cannot overwrite newer state or selection.
- Allow multiple durable live conversations. The user may create or switch to
  another conversation while unrelated backend work continues.
- The frontend is a managed immutable Web-library publication loaded through a
  deliberately tiny loader. Do not add a second activation database, rollback
  mechanism, static fallback application, or parallel durability scheme.
- Repair a bad frontend release by publishing a corrected immutable version.
  Exact publications remain immutable; floating compatible selection makes a
  newer patch live.
- Do not present accepted journal writes as a separate vague “saving” state.
  Surface real failures rather than suggesting that durable state is pending
  when it is not.
- Keep the composer editable while Kennedy works so the user can draft, but do
  not send another turn until the current one completes. Preserve one local
  draft per live conversation.
- A live operation needs an explicit stop control. Stopping must cancel the
  active model or tool work without losing the already accepted user command.
- Keep a closed conversation selected while its history ingress continues.
  Do not automatically create or select a replacement conversation.
- Show source and ingress activity as one continuous session history. Internal
  prompts, boxes, tools, and diagnostics may be collapsed in the ordinary view
  but must remain inspectable.
- Preserve an exact provider-boundary view for debugging. Human-friendly views
  may summarize presentation, but they must not rewrite the recorded bytes and
  claim that the result is exact.
- Do not inject backend-controlled content as HTML. Render text and untrusted
  metadata through safe DOM operations.

## Autonomous and Background Work

- Self time is genuinely autonomous and may use the complete read, research,
  and Kmap-write tool set within an explicit user-selected run.
- At 00:00, 04:00, 08:00, 12:00, 16:00, and 20:00 UTC, create one autonomous
  wakeup opportunity for each authorized user who has already opened a private
  Telegram chat. The acquired per-user marker, rather than the later model
  start time, supplies the time and date in the opening message.
- Wakeup opportunities are deliberately ephemeral scheduling: if the server is
  not running at a marker, do not backfill it or add scheduler state. Once
  created, the ordinary durable session may finish after the marker and must
  retain that acquired marker.
- A wakeup session has the autonomous read, research, direct-message, and
  Kmap-write capabilities. Silence is a complete and valid outcome; Kennedy
  should send a message only when confident that something is worth sending
  that user at that hour.
- A self-time run has one persisted absolute deadline shared by all of its
  clean-slate slices. Starting a new slice must not reset or shorten that
  deadline.
- `EndSession` may carry a bounded handoff message into a fresh slice when
  enough time remains. Ordinary prose does not silently roll the session over.
- Persist and restore active self-time work, serialize starts across tabs, and
  prevent overlapping runs.
- Give Kennedy a visible warning near the run deadline and enough bounded time
  to wrap up. Do not impose an unrelated short timeout on otherwise valid
  long-running research.
- Completed self-time work does not need a second ordinary history-ingress pass
  because the session already performs its own Kmap work.
- Background queues must release retrying or failed claims so one bad item does
  not block unrelated conversation, Telegram, audio, or ingress work.

## Managed Code and Publication

- Kennedy's managed Rust libraries, Web libraries, and Rust binaries use one
  shared development-tool boundary with small typed adapters and consistent
  session/lease behavior.
- Editable source and immutable publications are separate. Published versions
  cannot be overwritten, and floating compatible selection resolves to an
  exact immutable artifact.
- Keep one complete active source box per managed project. Successful complete
  writes revise it; exact writes remain durable; failed writes leave it alone.
- Kennedy's managed check and publication tools own the workflow. Avoid adding
  deployment scripts or another publication path around them.
- Managed Rust checks and publications automatically apply rustfmt to their
  disposable source before the remaining validation. Formatting differences
  that rustfmt can repair must not become model-facing failures or consume
  another Kennedy turn; failure to run rustfmt or source that rustfmt cannot
  parse remains actionable.
- Managed binaries preserve text output exactly and place binary output in the
  object store. Do not pretty-print or wrap exact output merely for consistency
  with an unrelated API.
- Managed-binary inputs may name canonical objects or objects staged in the
  current session. Kennedy resolves those references at the session boundary;
  binary execution receives exact ordered payload bytes and does not own Kweb
  or Session History lookup. Do not commit or expose staged inputs early merely
  to execute a binary.
- Managed-binary outputs become ordinary canonical objects that can immediately
  flow into later binary calls or normal channel delivery. Preserve recognizable
  file payloads exactly so existing file decoding and transport selection can
  identify and emit them without a binary-specific wrapper or conversion step.
- Prefer headless Chromium's browser-native rendering for fidelity-sensitive
  HTML-to-PDF work instead of a standalone HTML layout library. The managed
  Rust-binary runtime must provide Chromium and baseline fonts so a rendering
  binary can depend on that capability without bundling its own browser.
- Runtime networking and timeout choices belong to the managed binary call,
  not to a broad build-time restriction or an unnecessary global concurrency
  policy.
- Standalone libraries should own one coherent domain and avoid Kennedy,
  frontend, HTTP, vault, or persistence concerns outside that domain.

## System Prompts and Learned Strategy

- Preserve `KennedyIdentity.txt` as Kennedy's identity statement.
- Keep live system prompts small, layered, and single-purpose: identity, one
  session type, channel-specific context where applicable, Kmap basics,
  critical navigation tools, writable tools when allowed, the native harness
  boundary, and dynamic runtime facts.
- Kennedy's strategy, detailed tool manuals, user preferences, and evolving
  operating knowledge belong in Kmap rather than an ever-growing static system
  prompt.
- New capability manuals, including delegation and subagent tooling, must be
  delivered as standalone ingress material for Kmap and must not be added to
  runtime system-prompt files. Never modify any runtime system-prompt file
  without the user's explicit permission; a feature implementation does not
  imply that permission. Keeping manuals in Kmap lets Kennedy revise them,
  connect them to related operating knowledge, and select them only when
  relevant. Runtime prompts may retain only the small bootstrapping and
  navigation instructions needed for Kennedy to reach Kmap.
- The Kmap introduction must be sufficient for a model with no prior knowledge
  of Kennedy to find the automatic roots, understand canonical identifiers,
  navigate connections, and discover further manuals.
- Keep Telegram instructions out of non-Telegram sessions, and group-only
  instructions out of private Telegram sessions.
- Prefer concise natural language to JSON in model context when no exact machine
  protocol requires structure. When a critical navigation tool does require a
  strict machine protocol, give Kennedy a compact canonical example showing the
  bridge nesting and exact argument keys rather than requiring her to infer
  them from prose.
- State the current model, thinking mode, date, time, and timezone
  unambiguously in dynamic context.

## Persistence, Backup, and Security

- Keep Kennedy-owned runtime state beneath the repository-local ignored
  `data/` tree by default so it remains available inside the development
  sandbox. Retain individual path overrides rather than inventing a second
  data-root abstraction.
- Back up the complete opaque `data/` tree while Kennedy is stopped. Do not
  interpret or selectively copy current formats; unknown future files, SQLite
  sidecars, Kweb objects, recovery material, and legacy archives all belong in
  the backup.
- Write backup archives outside `data/`, publish them only after successful
  completion, and include the source revision and dirty-tree status needed for
  source-assisted recovery.
- The whole backup is not automatically encrypted merely because the credential
  vault is encrypted. Durable off-machine storage remains the user's
  responsibility.
- Keep reusable credentials in the human-unlocked encrypted vault. Never expose
  secret values through HTTP, frontend state, model context, logs, or source.
- A library that receives a reusable credential is security-critical. Pin and
  inspect the exact source used, and repeat that review before upgrading it.
- Maintenance operations that require exclusive persistence access must fail
  closed while Kennedy may still be running.
