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
- Do not hold a persistence lock or other shared-state guard across a call into
  a separately stateful capability, provider or network work, or an `await`.
  When a multi-step workflow must serialize, give it one explicit owner lane
  and keep each persistence critical section local to the store it protects.
  Speaker-review persistence and classifier work must not occupy async runtime
  workers or prevent unrelated browser views from loading.
- Same-process components should normally communicate through small typed Rust
  APIs. HTTP belongs at genuine transport boundaries, not between parts of one
  process.
- Keep Kennedy capabilities in cohesive managed Kcode libraries. The target
  size for each managed Rust, Web, or binary library is below 500 lines of
  first-party code including tests, with a clean API boundary and a minimal
  number of operations and inputs. New libraries must meet that target. When
  feature work touches a pre-existing larger library, keep the work focused and
  make one obvious capability extraction that prevents the library from moving
  farther away from the target; comprehensive unrelated cleanup is not
  required. Do not satisfy the target through pass-through libraries,
  duplicated tests, or artificial boundaries whose documentation and call
  sequencing are more complicated than the code they move. A managed
  capability library's
  public operations should complete the capability's internal workflow rather
  than require callers to sequence planning, application, or other intermediate
  steps. Return only outcomes needed for decisions that genuinely remain
  outside the boundary. Do not create pass-through crates that merely relocate
  server-specific glue. When decomposing an existing managed library, keep its
  caller-facing boundary stable: existing consumers must not need a new direct
  dependency, changed imports, or newly exposed internal sequencing solely
  because of the split. Preserve existing type paths through re-exports or a
  substantive facade while giving the extracted mechanics one clear owner.
  Prefer focused new libraries over casually making an existing managed library
  larger, but optimize for the simplest total architecture: extend the clear
  owner of a capability when doing so removes duplicate implementation or
  parallel ownership and leaves a lighter boundary between components. Judge an
  extraction by total first-party implementation as well as server size: move
  code and tests rather than copying them, reuse existing transports, and do not
  add abstraction or supporting code that consumes the deduplication benefit.
  Treat capability extraction as an ownership-only refactor unless the user
  explicitly requests a behavior change: preserve observable APIs, state
  transitions, persistence contracts, limits, filtering, retry semantics,
  failure isolation, and compatibility with existing durable state. Do not
  rewrite these clarifications to legitimize an incidental behavior change
  introduced during extraction.
- Judge API simplicity by the amount of documentation needed to explain the
  complete contract, not by raw function count. Prefer plainly named,
  complete domain operations over generic request enums, string dispatch, or
  multiplexed `read` and `execute` entry points. Shared types should clarify
  repeated data without hiding which operations a caller may perform.
- `kcode-kennedy-app` is Kennedy's top-level application bootstrap and lifecycle
  composition library. It owns CLI parsing and maintenance commands, vault
  prompting and application secret names, service construction, application
  root selection, the shared in-process capability graph, readiness ordering,
  listener binding, and runtime supervision. KennedyServer deliberately treats
  the latest published `kcode-kennedy-app` release as its application update
  boundary and accepts the highest available version rather than exact-pinning
  it. Kennedy's canonical operator launch refreshes the locked resolution of
  that dependency and its SemVer-compatible dependency graph before building
  and running the optimized server, so an ordinary launch adopts newly
  published compatible capability releases without a separate maintenance
  step. The KennedyServer executable is a
  one-line-equivalent wrapper that invokes this sole direct dependency and owns
  no parallel application behavior.
- `kcode-http-api` owns the complete browser-facing HTTP router and API as a
  focused presentation library over typed capability libraries, including
  route composition, wire DTOs, multipart admission, HTTP errors, response and
  cache headers, request limits, tracing middleware, the Web-library router,
  and the deliberately tiny root loader. Its handlers must not call back into
  application implementation modules through callbacks, traits, or other
  dependency inversion. Any transport-independent workflow shared by HTTP and
  backend orchestration must have a cohesive typed library owner.
- Managed libraries maintained only by LLMs are source-first: their code and
  tests are the specification. Do not add a separate `Specification.md` that
  duplicates an ingestible codebase; retain only documentation required by the
  managed-library or publication boundary.
- The Rust backend owns orchestration. Browser code owns presentation and local
  input concerns only. Do not add a server-side JavaScript runtime.
- Start dependent background workers only after their prerequisite services
  have positively reported readiness. Expected startup sequencing must not be
  surfaced as retry warnings or handled by launching workers into known
  unavailable dependencies.
- Prefer explicit, visible behavior over hidden automation. Failures should be
  actionable, state transitions inspectable, and destructive recovery steps
  deliberate.
- Build for current needs. Do not add abstraction, configuration, indexing,
  compatibility, concurrency, or fallback machinery solely for hypothetical
  future requirements.
- When an old persistence format or architecture is retired, preserve the
  original data for offline recovery, remove its live loader, and avoid carrying
  permanent compatibility state into the new design. Accepted permanent history
  is the narrow exception: when preserved legacy fields are sufficient to honor
  a current user-visible contract, prefer bounded read-time interpretation over
  destructively rewriting every historical record. Keep that compatibility in
  the current capability owner and do not revive the retired persistence owner.

## Kmap and Durable Knowledge

- Kweb is a privacy-enforcing transactional graph and object store. Every node
  and object has an immutable-user-ID owner and independently stored audience
  policy, and Kweb includes those facts in signed transactions and enforces
  them on reads and writes. It does not resolve Kennedy-specific transport
  groups, roots, fixed/recent context layout, UI policy, or session behavior;
  callers supply the exact user and model audience for an operation.
- Kennedy must use `kcode-kweb-db`'s public API for live database access
  rather than parsing or mutating Kweb's binary formats behind the library's
  lock and recovery boundary. When the server is stopped, offline diagnostics
  and narrowly scoped recovery may inspect or repair persisted records in
  `data/`, provided the original records are backed up and the format's
  checksums and invariants are validated.
- Kennedy owns application graph policy, including roots, fixed/recent context
  layout, application validation, and model attribution.
  Kweb owns durable resource ownership, privacy evaluation, and owner-only
  mutation enforcement.
- One small typed Kweb-context library owns the in-session loaded/fixed node
  model and mechanically reconciles Kennedy's chosen projection into Chatend,
  including stable box identity, typed metadata, permanent bounded
  recent-connection boxes, and preservation of representation choices.
  `kcode-kennedy-kweb-loader` owns atomic batch fetching and projection of
  durable nodes into that context plus legacy stored-node decoding; it remains
  read-only and owns neither roots nor session policy. `kcode-kennedy-sessions`
  owns selection of node IDs within a session, the staged mutation plan,
  synchronization timing, model-facing tool-result presentation, and final
  commit mechanics. The Kennedy application selects the roots and must not
  retain a parallel Kweb-box parser, slot reconciler, staged plan, or durable
  node loader.
- One focused Kennedy-root library owns the mechanics for bootstrapping and
  validating the application system roots and reconciling user and Telegram
  group root assignments. The top-level application supplies the configured
  persistence locations and root policy and receives the selected canonical
  roots; the root library completes mutations through typed Kweb Manager and
  identity-directory APIs rather than exposing intermediate SQL or Kweb steps.
- After the Kennedy application bootstraps and validates its
  application-owned roots,
  `kcode-kweb-manager` exclusively owns the live `KwebDb` handle. The manager
  owns typed node reads, idempotent provenance and ordinary node mutations,
  opaque object storage, and session commits; the top-level application retains
  root policy and supplies the selected roots and manager handle to
  `kcode-http-api` for
  genuine browser presentation. Do not retain a parallel database handle or
  route same-process Kmap calls through HTTP paths or JSON values. Kweb's signed
  creating transaction is the sole durable source of object provenance; the
  manager must not copy that provenance into a sidecar table, and it deletes
  the retired duplicate table when opening an existing receipt database.
- Fixed connections are deliberately placed references, not task slots or a
  priority system. Graph hygiene belongs to Kennedy; the harness should not
  silently promote or rearrange connections.
- The context projection gives each explicitly loaded node one full box and
  each of its fixed connections one full box, without an arbitrary
  fixed-connection count cap. There is no active-connection category. All
  recent connections included in the projection are fanout-only and accumulate
  in globally deduplicated summary boxes containing each node's name and short
  description, with at most eight unique connections per box. Only the newest
  box may contain fewer than eight; newly discovered connections fill that box
  before another is created. A recent-connections box is never retired once
  created, and a full box's membership and canonical snapshot are frozen,
  though any such box may still be dehydrated or summarized. Recent connections
  are never automatically promoted to full nodes. An explicitly loaded node
  uses the concise header
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
- The administrator may edit Kmap through one browser-facing durable command
  lane. Each accepted command is persisted before it waits for the global Kweb
  writer lane and is recovered and resubmitted after process restart. The lane
  exposes its queued, applying, committed, and failed records rather than
  hiding retry state; in-memory notification is only an optimization. Writer
  unavailability retries with backoff, while stale expected revisions and
  invalid operations fail visibly. Every successful command is exactly one
  signed Kweb transaction, even when the Kennedy operation atomically creates
  or updates several nodes. Commands are not batched, and committed Kweb
  history remains the authoritative replay record. Until the deferred privacy
  upgrade is explicitly resumed, this API and UI are administrator-only and do
  not add a parallel ownership, privacy, or authorization policy.
- Administrator Kmap commands preserve the exact behavior of Kennedy's current
  `CreateNode`, `UpdateNode`, `ConnectNodes`, `ConsolidateFanout`, and
  `SetFixedConnection` operations. The browser supplies the expected visible
  transaction of every affected durable node, and the writer revalidates those
  expectations only after acquiring its lane. Provenance identifies the
  durable administrator command automatically; browser arguments do not
  manufacture model or session provenance.
- Kmap maintenance measurements may be deliberately approximate when their
  purpose is operational guidance rather than an exact accounting system.

## Tasks and Credits

- Kennedy's task board is a graph-shaped work organizer. Categories and tasks
  have stable identifiers, and tasks may belong to multiple categories, have
  one assigned authenticated user, carry a nonnegative credit value, and link
  to child and related tasks. Child links express strict sub-requirements for
  presentation but do not impose execution order. Child links may form cycles,
  including self-cycles. Related links are symmetric.
- Kennedy alone creates, updates, completes, or removes task-board state. Her
  task operations are create, look up by known identifier, update, and remove;
  category operations are create, look up by known identifier, and remove.
  Assignment, credit values, categories, child links, and related links are
  fields of task creation or update. Kennedy has no global task/category list
  or search operation and navigates through identifiers and graph neighbors.
  Ordinary users may page and browse their visible categories and tasks but
  may not mutate them.
- Task identifiers have the exact form `task-id-` followed by twelve lowercase
  hexadecimal characters. Browser conversation transcripts recognize only
  exact identifiers, expose them as safe task-opening controls, and let users
  copy an identifier for discussion with Kennedy.
- Completing or removing a task removes it from the ordinary board, detaches
  its graph links, and preserves completed tasks only in a disk-backed archive
  that has no browsing UI. Completion is allowed regardless of child state.
  Any child that loses its final parent through completion or removal is pushed
  onto a durable LIFO orphan stack.
- Orphan-stack membership is the sole orphan marker. A zero-parent task outside
  the stack is an ordinary independent task. Kennedy may inspect only the top
  orphan. If she does nothing it remains on top; deleting it removes it; any
  successful nonterminal update removes that top task from the stack and puts
  it back in the board either independently or with the parents she chose.
  Lower queued orphans are not resolved out of order, and newer pushes become
  the top.
- Credit balances live in a separate SQLite capability keyed by immutable
  authenticated user identifiers. Its domain API consists of `balance` and
  `award`; it intentionally has no ledger, history, spending, transfer,
  idempotency, or outbox mechanism. Task completion commits first and then
  attempts a best-effort credit award. An award failure is surfaced as a
  warning while the task stays completed; a process interruption may lose that
  award because credits are gamification points rather than financial value.
- The task UI shows the current user's balance and reuses the ordinary Kennedy
  conversation and composer. Tasks are a mode of the conversation-history
  sidebar rather than a separate global view: selected-task detail appears
  above the category and paginated task explorer while the active conversation,
  composer, and Chatend remain mounted and visible. Opening an exact task ID
  from a conversation selects that sidebar mode and task. The UI lazily loads
  the selected task neighborhood, follows graph links, and hides completed,
  removed, and queued-orphan tasks. The existing browser-selected user root is
  the current identity boundary; adding browser authentication or a new
  conversation/session type is outside this capability.
- At tens of thousands of tasks, the browser and server must avoid periodic
  full-board snapshots. Poll only small task and credit revisions, stop polling
  while the page is hidden, and fetch paginated lists and selected
  neighborhoods on demand. SQLite uses WAL, indexed relationship and ownership
  lookups, separate short read connections, one short writer lane, and
  set-based completion/orphan updates. Do not hold its writer lane during JSON
  processing, model work, or credit calls. Kennedy is responsible for avoiding
  pathological fanout; the task board adds no degree limit or cycle check.

## Sessions, Chatend, and Context

- `kcode-session-history` exclusively owns the caller-facing Session History
  capability: active session creation and discovery, lifecycle/control
  journals, pending session objects, and completion receipts. Its focused
  `kcode-chatend` implementation dependency owns the provider-independent
  box/event state machine, exact context projection, replay, and mechanical
  synchronization of mutable Chatend state through `kcode-session-log`.
  `kcode-session-history` creates and opens those sessions and re-exports the
  original Chatend types and paths; existing consumers must neither depend on
  `kcode-chatend` directly nor manipulate the same files independently.
- Treat the lifecycle/control journal as the cheap active-session index.
  Frequent discovery, summary listing, and command/stop-head polling must
  project bounded summaries directly from control state and must not open or
  replay transcript logs. The control projection preserves the fields needed
  for list decisions and presentation, including a bounded first-user title
  and current box and event counts, at normal mutation or checkpoint
  boundaries. Missing legacy summary fields remain optional and may be
  enriched only when that particular session is explicitly opened, never by
  bulk enumeration. Enumeration ignores transcript files whose control journal
  is absent or has no lifecycle. Transcript checksum validation remains
  mandatory when a full session or transcript-dependent operation explicitly
  opens it, or when its transcript is recovered.
- One focused `kcode-kennedy-sessions` library owns the complete mechanical
  lifecycle of a logical Kennedy session: Session History and Chatend
  projection, staged Kweb state and revalidation, tool authorization and
  dispatch, object resolution, provider-loop hosting, checkpoints, recovery,
  and final archive/commit mechanics. Its boundary is one concrete capability
  service plus complete session operations, not a callback for every tool.
  A typed Kennedy orchestration service composes the application prompt,
  selects roots and runtime for each session, and owns the shared checkpoint,
  stop, cancellation, and external-turn mechanics used by browser and
  transport workflows. Focused runtimes schedule outer work and decide which
  external workflow should start or finish; none may retain a parallel session
  implementation.
- Every running logical session may receive authorized external user turns,
  including conversation, Telegram, history-ingress, audio-ingress, self-time,
  wakeup, and other backend-owned session kinds. Session History owns their
  durable ordered pending state, while Kennedy orchestration owns transport
  submission, notification, idempotent draining, and completion races. The
  session stages each accepted turn as ordinary user-owned Chatend content at
  a safe semantic boundary before the next provider inference; never mutate a
  live Session concurrently, cancel valid work merely because input arrived,
  or let session completion strand an already accepted turn. Kennedy normally
  continues working while a response is absent. She may explicitly await a
  response when useful, and that wait is satisfied by the next authorized user
  turn routed to the same source session whether it arrives through its main UI
  or Telegram path; waiting is an explicit session action, not an implicit
  blocking mode for outbound delivery.
- One small context-capacity policy library owns the complete mechanical
  preparation of Chatend representations: initial history-ingress hydration,
  summary preservation, live overflow recovery, protected-content ordering,
  largest-first reduction, durable application, and final capacity
  measurement. Its public workflows are outcome-level preparation and recovery
  calls rather than separately exposed planning or application steps.
  `kcode-kennedy-sessions` owns application of the visible overflow warning,
  Kweb revalidation, ingress lifecycle events, and mechanical continuation or
  finalization after recovery. Kennedy orchestration retains prompt and runtime
  selection plus outer transport delivery and job scheduling.
- Session completion synchronizes its immutable receipt before removing active
  files. Concurrent readers must treat a journal that disappears after
  enumeration as a completed lifecycle transition, not as storage corruption
  or a request-wide failure; other I/O failures remain visible and actionable.
- A source conversation and its later history ingress are one logical session
  and one ordered event history. UI and model views are projections of that
  history, not independently authoritative transcript copies.
- Retain only the small completed-session catalog needed to find immutable Kweb
  archives and commit receipts. Do not keep a second permanent transcript store.
- Every item visible to Kennedy belongs to the box model. Canonical content,
  its current representation, and the history of changes to that representation
  are distinct concerns.
- Chatend box headers expose the box identity, name/type, representation, and
  staleness needed to manage context, but never expose Chatend's internal box
  owner. Every projected user message and user-visible Kennedy message carries
  its recorded timestamp including the year.
- Kennedy may deliberately preserve the complete text of selected active boxes
  as durable objects. Each selected box becomes one separate UTF-8 plain-text
  object containing its latest canonical text, regardless of whether the
  visible representation is hydrated, summarized, dehydrated, or stale. Do not
  substitute a summary, serialize a box header or metadata, concatenate
  independently selected boxes, or create a second Kweb transaction; stage the
  objects in Session History and resolve their pending references in the
  logical session's ordinary commit. Repeating the operation for the same
  canonical box revision should reuse its staged object.
- When updating a box moves its current representation to a later position in
  the projected history, leave a minimal generic `[box updated]` placeholder at
  its earlier occurrence. This preserves visible tool-call/result continuity
  and makes clear that the call succeeded but its stateful output was superseded
  and moved forward. The marker is generic Chatend projection behavior, not a
  Kmap-specific notice box.
- Hydration, dehydration, and Kennedy-authored summaries must never destroy
  canonical content. If canonical state changes behind a compact
  representation, preserve Kennedy's representation choice and mark it stale.
- Routine context reduction is a batch operation. `DehydrateBoxes` accepts any
  nonempty set of distinct active box IDs, including a one-item set, validates
  the complete selection before changing it, and dehydrates all selected boxes
  in one call. Do not expose or retain a singular `DehydrateBox` command.
- Kmap navigation is likewise a batch operation. `LoadNodes` accepts any
  nonempty set of distinct canonical node IDs, including a one-item set, loads
  the complete selection before synchronizing the shared Kweb boxes, and has no
  fixed count limit. Do not expose or retain a singular `LoadNode` command.
- Model-facing context control operates on boxes, not individual journal
  events. Persisted events remain available for audit and replay, but do not
  expose `HydrateEvent` or `DehydrateEvent` commands or create temporary
  event-inspection boxes. Ingress-only guidance must not appear in other session
  prompts.
- Stateful tool results, especially complete managed-library source, should
  revise one stable box rather than repeatedly adding complete copies. Failed
  writes must not revise that box. Exact invocation arguments remain durable
  without being needlessly projected into every later model request.
- Do not reintroduce a reset operation that discards the logical context model.
  Context should be managed through explicit box representations while the
  complete event history remains recoverable.
- Do not rely on hidden provider conversation state or silent compaction.
  Kennedy's typed session capability constructs the provider-visible projection,
  enforces capacity,
  and retains enough exact boundary data to explain what the provider received.
- The UI's Context View is a transparent rendering of the exact UTF-8 Chatend
  string supplied as the model input for the current state. It may render
  newlines, tabs, and other whitespace normally for readability and may show
  labels or a byte count outside the payload, but it must not trim, reflow,
  annotate, unescape, or semantically reconstruct the payload itself. Provider
  transport envelopes and JSONL request records are distinct diagnostics and
  must never be substituted for model context. Active, history-ingress, audio,
  Telegram, self-time, and immutable completed views derive this string through
  the same authoritative backend projection.
- Crossing the active context limit is a recoverable condition in every
  top-level session kind, not a reason to reject newly accepted user input,
  Kennedy output, tool calls or results, or managed-state changes. Preserve the
  new canonical material, automatically dehydrate active boxes until the
  complete projected Chatend fits, and expose this exact hydrated system
  message both in Chatend and through the user-facing transcript or transport:
  `Context size was exceeded, some context has been dehydrated. The session is
  now at risk of destabilizing, please perform any cleanup tasks and end the
  session`. The warning and its projection cost are part of the recovered
  context; do not silently compact or immediately force the otherwise
  recoverable session into history ingress.
- Automatic overflow recovery dehydrates the largest rendered candidates
  first while preserving canonical contents. Protect the three most recent
  user-message boxes and the ten most recent Kennedy-result boxes until
  unprotected candidates have been exhausted. A Kennedy result is a semantic
  box class covering anything Kennedy produced or caused to be produced,
  including Kennedy messages, tool invocations, tool results, and managed tool
  state; classify it semantically rather than deriving it solely from any
  storage enum or representation detail. If protected content must also be
  reduced, exhaust other protected classes
  before touching those ten recent Kennedy results, retaining largest-first
  ordering within each protection tier.
- Treat current context occupancy and cumulative provider usage as different
  measurements. Do not present lifetime token consumption as though it were the
  amount currently occupying the model window.
- Every Chatend exposes one continuously refreshed session status, regardless
  of transport, session kind, or lifecycle phase (including source
  conversation, history ingress, and completed archive). It visibly reports
  current estimated context occupancy, the estimated context size if every
  active box used its latest fully hydrated canonical body, the active
  failure-avoidance limit and progress toward it, and exact cumulative cached
  input, non-cached input, thinking, and output tokens. The same visible status
  reports cumulative estimated session cost at standard API rates in pennies
  with three digits after the decimal point, including the count of any
  provider calls that remain unpriced. The fully hydrated estimate uses the
  same projection ordering, markers, footer, and latest provider calibration
  as current occupancy; it is a hypothetical capacity measurement, not
  lifetime usage. Those cumulative totals cover every
  model-backed provider call causally owned by the session, not only Kennedy's
  top-level turns: delegated-agent rounds and their tool-triggered inference,
  hosted search, media annotation, transcription, generation, and any other
  descendant or background inference all contribute. Local operations that do
  not call a model, such as ordinary fetch or document extraction, do not.
  Token-metered calls advance the token categories; calls for which a provider
  reports only non-token metering or no metering remain visibly accounted for
  as such rather than disappearing or receiving invented token counts.
- Anchor current context occupancy to the newest input-token measurement
  returned by intelligence for Kennedy's own Chatend input. Descendant
  inference contributes to cumulative usage but must not replace that context
  anchor. Record the exact UTF-8 byte length of the rendered context at the
  anchor moment, then adjust the measurement by the signed change in current
  rendered bytes at four bytes per token as boxes are added, revised,
  summarized, hydrated, dehydrated, or retired. Before the first provider
  measurement, use the same four-bytes-per-token approximation. Cumulative
  usage categories are never estimated: advance them only from new provider
  usage results, including multiple inference steps within one Kennedy turn,
  and reconcile live updates with the final call receipt without double
  counting.
- The model-visible context footer lists stale boxes first and context size
  last. Its final line shows only the current estimated context size and the
  active failure-avoidance limit, labeled `current context size` and
  `max context size`; do not expose the larger provider window as an
  `effective` value that Kennedy could mistake for usable capacity.
  Immediately above that line, show the current time including the year,
  refreshed whenever Chatend is projected. Recalculate and attach the footer
  after every Ktool result before allowing the provider's next inference;
  sequential tool use must expose the current time and remaining capacity
  between calls rather than showing only the values from the beginning of the
  turn.
- Conversation sessions read the Kmap but do not mutate it. Durable graph
  changes happen in explicitly writable history-ingress, audio-ingress,
  self-time, or other backend-owned sessions.
- Stopping work is an orchestration control, not a request for history ingress.
  Session History owns the typed durable stop request, derives its scope from
  the durable lifecycle and session kind, marks queued turn work for
  cancellation, and notifies a same-process listener before accepting a stop
  aimed at live work. Browser HTTP requests this owner-level operation directly
  and must not depend on a particular orchestration runtime. The workflow owner
  cancels descendant execution, unwinds the live turn, and completes the stop
  through the existing durable lifecycle.
  In an interactive user session, stop only the current Kennedy turn: cancel
  its descendant work, preserve accepted input and completed effects, close any
  interrupted invocation visibly, and leave the session active and ready for
  new user input. In a non-user session, stop terminates that session through
  its ordinary durable completion path without queuing another history-ingress
  phase. The control must not mutate Session History files behind their typed
  owner or report success before the running work has begun to unwind.
- Accepted session history and committed objects are permanent. A cleanup or
  UI convenience must not imply that durable accepted history was erased.
- Recovery derives external-turn completion from the authoritative session
  journal. Once that journal contains Kennedy's response, a stale lifecycle
  checkpoint must not keep the turn pending or block later transport events.
- Tool results should remain plain and direct. Avoid redundant JSON envelopes,
  pretty-printing, or duplicate generic result boxes when a tool already
  updates a canonical stateful box. When a tool takes more than three seconds
  to return, include its elapsed duration alongside the result in the model's
  context; omit timing noise for faster calls.

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
- A Ktool invoked by a subagent must not create, revise, summarize, hydrate,
  dehydrate, or otherwise manipulate the parent agent's provider-facing
  Chatend. Keep the tool invocation, ordinary result, and any tool-owned
  current-state projection inside the subagent context. Durable audit facts,
  usage accounting, pending objects, transactional Kmap mutations, managed
  source mutations, and other authorized application effects may remain owned
  by the parent session without becoming parent prompt material. The terminal
  subagent response is the ordinary parent-visible tool result.
- Every Ktool otherwise permitted by the parent session is callable through
  the normal bridge except nested `RunSubagent`, parent-session lifecycle
  control, and box-presentation controls that have no meaning in a box-free
  child. The subsystem does not inject a Ktool catalog or any tool manuals.
  Kennedy must teach a subagent the relevant calls through the selected node
  descriptions or task prompt. Delegation strategy remains with Kennedy
  because she has the Kmap context needed to choose it.
- `EmitObject`, `SendTelegramDM`, and `SendTelegramGroupMessage` are explicitly
  delegable effects. A successful delegated object emission reaches the user
  through the same ordinary Kennedy-message presentation and transcript record
  as a direct invocation. Delegated Telegram tools retain their ordinary cold
  delivery, source-session audit, eligibility, attachment, and reply-bridge
  semantics without projecting their tool output into the parent context or
  creating a target session.
- Stateful Ktools retain exact canonical history while projecting only one
  current value into the box-free subagent context. When a successful tool call
  revises that value, keep the earlier call in place and replace its superseded
  output with the generic marker `[Tool output was displayed here, but has since
  been updated and now appears elsewhere in the context]`. Move the latest
  complete value to the bottom of the context after the call that produced it,
  so only one complete rendering of a tool-owned state is visible and its
  chronology is unambiguous. Do not duplicate complete old and new values, and
  do not revise state after a failed call. This projection applies generically
  to managed source and any other box-aware tool; exact audit history remains
  unchanged.
- A successful subagent call returns its one terminal assistant response as the
  plain Ktool result to Kennedy, followed only by a concise estimated cost line
  in pennies with three digits after the decimal point. The cost is the
  session-accounting delta causally owned by that subagent, including
  tool-triggered descendant inference, and discloses any calls that could not
  be priced. Do not otherwise wrap it in a redundant report, inject the child
  context or trace into Kennedy's active context, or treat the subagent's claim
  as proof that its task succeeded. Kennedy decides what to inspect or verify
  through her own context and tools. Child failure or cancellation returns an
  actionable tool error and must disclose when effects may already have
  occurred.
- Subagent effects remain subject to the parent session's permissions and
  transaction boundaries, and stopping the parent operation stops its active
  subagent work.
- Attribute every provider call to a stable Kennedy user and persist one usage
  receipt per call, including failed calls for which complete metering is
  unavailable. Each receipt retains a stable call identity and operation
  lineage sufficient to attribute all direct and descendant inference to its
  originating session and to project that session's live and replayed totals
  idempotently. The router's receipts remain the canonical usage ledger;
  session history stores their accounting projection rather than creating a
  competing ledger. Cached input, uncached input, reasoning, and visible output
  must remain distinguishable where the provider reports them.
- Keep the application handoff from intelligence accounting into Session
  History in one narrow mechanical adapter library. Its only stateful workflow
  reconciles cumulative live and final usage for one top-level Kennedy call;
  descendant projection consumes the router's canonical usage receipt, and
  subagent projection consumes a self-contained typed runtime audit event.
  The adapter records those projections through Session History's typed API and
  does not expose event builders or raw JSON for application orchestration to assemble. It
  does not own provider calls, pricing policy, canonical receipt persistence,
  session lifecycle, or context preparation. Router successes and failures
  surface the exact canonical receipt with operation lineage, and agent-runtime
  events carry their own correlation data so application orchestration need not reconstruct
  provider accounting from ordered strings or partial events.
- The intelligence boundary owns one dated, versioned catalog of the published
  standard-tier prices for every model-backed endpoint Kennedy invokes.
  Calculate cost from the actual model and provider-reported billing
  dimensions, including cache reads and writes, reasoning at output rates,
  modality-specific audio and image tokens, long-context tiers, duration
  metering, and hosted-search charges where the provider reports them.
  Preserve exact provider detail when available; label documented fallbacks
  as estimates and quota-dependent search charges as conservative. A model,
  failed call, or historical receipt that cannot be priced remains explicitly
  unpriced rather than being treated as free. A historical receipt whose
  preserved actual model and metering are sufficient must be priced on read
  through the intelligence boundary's applicable dated catalog without
  rewriting the immutable receipt or session history; compatibility may use
  mechanically associated session, subagent, or model-backed-tool metadata but
  must not guess an unavailable model or billing dimension. Cost amounts are
  estimates based on public direct-API rates even when the provider is reached
  through a subscription-authenticated harness.
- At the transition into history ingress, Kennedy receives a fixed
  model-visible snapshot of the session cost accumulated before ingress began,
  expressed in pennies with three digits after the decimal point and with any
  unpriced call count disclosed. Later ingress inference must not retroactively
  change that boundary snapshot.
- Discover effective model limits from the provider boundary and fail closed
  when they cannot be verified. Do not invent local context limits or allow
  provider-side automatic compaction to silently remove Kennedy's Kmap context.
- Kennedy conversation generation uses the provider's native turn and tool
  protocol with one `call_ktool` bridge. Preserve native call identity and raw
  tool results rather than inventing a parallel tool protocol.
- `kcode-agent-runtime` owns the provider-round loop shared by primary and
  delegated agents, including streamed output, native `call_ktool` parsing,
  continuation, cancellation, captures, receipts, and the round limit. The
  session supplies prepared context and executes authorized calls through one
  small host interface; the runtime does not own Kennedy prompts, Kmap, tool
  policy, or session persistence.
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
- Once staged, stored filename, media type, transport kind, and object identity
  have one authoritative value. Downstream adapters must not independently
  reconstruct them. A deliberate recipient-visible delivery filename is
  separate presentation metadata: Kennedy may choose one when displaying or
  sending an object without mutating, copying, or changing the identity of the
  stored object. When omitted, delivery uses the stored filename.
- Apply one selected delivery filename consistently to the session transcript,
  browser download, and transport request for that outbound item. Delivery
  filenames are bounded, path-free basenames; they do not override MIME type,
  transport kind, bytes, provenance, or object identity.
- Whenever Kennedy receives a user-supplied file, every model-facing session
  type shows a bounded metadata block sourced from that authoritative custody
  record: original filename, filename extension, MIME type, and exact byte
  size, plus the object reference when one exists. Accompanying message text
  must not hide file metadata, and client-repeated descriptors are not a second
  source of truth.
- Keep raw media bytes out of model-readable prose. Kennedy sees bounded
  metadata and an object reference, then explicitly chooses an appropriate tool
  when the contents matter.
- Browser uploads may accept arbitrary files. Document extraction is optional
  enrichment and failure to extract must not discard an otherwise valid file.
  Kennedy's document tool exposes the extraction library's supported
  searchable PDF, Word, spreadsheet, and text-family formats, including plain
  text, CSV, TSV, Markdown, JSON, YAML, and XML; do not retain a narrower
  advanced-document-only gate in Kennedy application code.
- User-supplied voice and media reach Kennedy as originals without eager
  transport-generated interpretation. Kennedy chooses whether and how to
  transcribe or annotate them, including the exact model and prompt. Kennedy's
  session, intelligence-routing, and provider-adapter layers must preserve a
  nonblank caller prompt unchanged without imposing an arbitrary character or
  byte ceiling; only a provider's actual request or context constraint may
  reject its size.
- Every object-consuming Ktool accepts both a pending object staged in the
  current logical session and an authorized canonical object from the object
  store. Resolve both forms through the same session boundary before applying
  tool-specific capability or size checks; object availability must not depend
  on its storage phase. This includes Ktools run by subagents, which share the
  parent session's pending objects and may return any useful object ID in their
  ordinary text response.
- Model-backed media work must validate the actual media kind and capability.
  Treat transcription, translation, visual interpretation, and speaker
  identification as potentially wrong and preserve uncertainty.
- Keep speaker classification inside Kennedy's application process as one small,
  typed capability. Its application-facing mutation contract is deliberately
  limited to opening one store and identifying, training, or deleting an
  observation; audio review may also enumerate the distinct known speaker names
  through the same typed handle. The
  new 24-value feature representation and statistical classifier are internal
  implementation details, not reasons to expose source objects, segments,
  attempts, samples, recording quality, artifact lineage, datasets, evaluation,
  or model-lifecycle orchestration. Do not interpose a separately operated
  socket or network sidecar. AudioIngress and Kennedy's explicit classification
  Ktools share the same classifier handle and database; neither opens a private
  or parallel speaker store.
- Audio speaker identity comes from the shared classifier, not from GPT.
  Gemini performs diarization, writes a complete transcript with stable
  first-appearance speaker labels, translations faithful to the original
  uncertainty and vulgarity, useful contextual annotations, and useful
  correction, language, and accent coaching for audibly non-native speech, then
  provides the 24-value feature analysis. Gemini is not told about recording
  chunking and must not be made to serialize the conversation as a large
  collection of transcript JSON objects. After every Gemini result is durable,
  one recording-wide GPT call parses only the feature analyses into narrowly
  internal machine data for classifier coordination. Do not replace this one
  batch with per-chunk GPT calls or prompt-caching machinery absent a concrete
  need. The browser presents each complete raw Gemini result rather than
  labeling or dumping internal parsed JSON as its transcript. Only after every
  chunk has human-approved speaker resolutions may one second recording-wide
  GPT call produce the final transcript from the original raw Gemini results
  prefixed by those authoritative mappings. That final call reconciles overlap,
  preserves appropriate translations, annotations, and coaching, filters out
  feature analysis, and must not guess, expand, or invent real speaker
  identities.
- Treat the classifier as practically strong once a person has more than five
  representative samples, while retaining the explicit possibility of error.
  Human review is the authority for final audio labels. Review and signoff are
  chunk-scoped. The review must provide an audio player for each exact source
  interval beside the complete raw Gemini result and its observations; never
  ask a human to identify a speaker from labels, classifier scores, or text
  without letting them hear the retained audio. For each observation, prefill
  the classifier's best name, show only its zero-relative score and the
  runner-up score, provide a dropdown of known speakers and a textbox for a new
  name, and allow an explicit unknown resolution. The background score is
  always zero and is neither persisted nor displayed. Human-review players
  receive only their exact source intervals as native WAV slices read by
  seeking into the retained original. Do not reread the complete
  recording, resample it, or transcode it to another codec for browser review.
  A named resolution is trained when a compatible feature row exists; an
  unknown resolution is not added to the classifier and does not block final
  transcription. Do not show a
  clean/unclean judgment in the UI. Every observation in a chunk must be
  resolved before that whole chunk is signed off, and every chunk must be
  signed off before final transcript generation, Session History submission,
  or Kmap ingress. Automatic clean-chunk signoff and unsigned training remain
  intentionally deferred until real training behavior supports explicit
  thresholds. Treat accepted ingress as binding the final label set: exact
  confirmation retries remain idempotent, while a conflicting relabel after
  handoff is rejected rather than diverging from immutable history. A finalized
  recording with an obsolete unsigned correction packet is resolved at the
  audio-to-History boundary. If accepted History ingress exists, preserve the
  recording as complete and never present that packet for relabeling. If no
  ingress exists, archive the exact old transcript and packet for offline
  recovery, clear the active result, and run the retained WAV through the
  current analysis and human-review pipeline so approved current-schema
  datapoints and a properly reconciled transcript can be produced. Completed
  recordings without correction packets retain their established behavior.
- Outside the finalized-recording recovery above, a retained correction packet
  from a retired, incompatible speaker-feature schema must not make audio
  ingress globally unavailable. Preserve the packet and its human-review gate,
  accept the final human labels through the ordinary confirmation operation,
  and do not invent a translation into the current classifier feature schema
  or train an incompatible historical row into the current classifier.
- Give Kennedy exactly three explicit speaker-classification actions: identify,
  train, and delete. Preserve the old classifier's direct observation-key,
  cohort, feature-row, and outcome semantics: identify scores a row and
  atomically retains an accepted result, train adds or corrects a labelled row,
  and delete is idempotent. The library owns each complete operation, including
  its exact argument decoding and result rendering; Kennedy retains
  authorization, blocking-task scheduling, and placement of the result in the
  session, and must not duplicate the feature schema or outcome translation.
  Name the Ktools `kcode-speaker-system/identify`,
  `kcode-speaker-system/train`, and `kcode-speaker-system/delete`; do not add
  speaker or datapoint suffixes. Keep their standalone Kmap-ingress manual
  focused on the callable API rather than restating all 24 values. Training
  accepts caller-supplied classification data, including arbitrary speaker
  names; do not impose a server-side “known labels only” rule, provenance gate,
  or other restriction that narrows the operation.
- Store committed files in self-describing Kweb object envelopes so an object
  identifier is sufficient to recover safe metadata and exact bytes. Payload
  readers return the exact original bytes; the application storage envelope is
  never part of the returned payload. Resolve known pending-object references
  in the same transaction that commits the session.
- Application-level file and provenance payload envelopes stored inside opaque
  Kweb objects have one small typed library owner. Kennedy orchestration decides when
  to store and transport them, but does not duplicate their binary codecs.
- Envelope codecs are canonical and fail closed: every encoder output must be
  accepted by its decoder, filenames are bounded and platform-independent,
  reserved markers never fall back to raw-payload interpretation, and
  untrusted declared sizes use checked arithmetic and fallible allocation.
- Native media delivery and generic-document delivery are intentional distinct
  actions. A failed native send must remain visible as that failure rather than
  silently changing the semantic delivery type.
- AudioIngress owns durable intake, retained originals, processing state,
  retries, and the completed transcript behind a small typed library API. One
  separate audio/session-ingress coordinator library owns the idempotent
  application handoff into Session History: deterministic piece identities,
  transcript splitting, complete file metadata, combined recording and ingress
  state, submission of missing pieces, and ingress retries. Bound each
  transcript piece dynamically from the effective context window of the model
  doing the ingress when its session is created. This segmentation calculation
  is ephemeral: do not persist the model context, derived limit, piece length,
  fingerprint, or a segmentation version merely to revalidate historical
  pieces, and do not make newly invented metadata mandatory for existing
  durable records. Already-ingressed pieces remain authoritative through their
  deterministic identities. The coordinator exposes transport-neutral typed
  outcomes and does not own an HTTP or serialization contract. `kcode-kennedy-app`
  retains application service construction, while `kcode-http-api` owns browser
  transport admission and HTTP presentation.
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
- `kcode-telegram-transport-state` exclusively owns Telegram's SQLite handle,
  schema and migrations, polling cursor, accepted-event queue and batching,
  private and group session pointers, stable group and membership ledger,
  quarantine evaluation from caller-supplied live observations, retained
  working context and media, edit reconciliation, and delivery state
  transitions. `kcode-tg-kennedy-bot` remains the unchanged application-facing
  facade and owns the Bot API client, polling and update decoding, live identity
  and roster acquisition, downloads, retries, and actual sends. Their boundary
  uses typed records and opaque consuming admission or delivery capabilities at
  the unavoidable external-I/O seams; it must not expose SQLite, Teloxide,
  arbitrary JSON, batch internals, or caller-assembled persistence steps.
  Existing callers continue to use only `kcode-tg-kennedy-bot`'s public types
  and complete operations.
- `kcode-telegram-session-coordinator` owns Kennedy-specific session delivery
  mechanics over the typed Telegram transport and identity directory: delivery
  argument validation, per-user and per-group serialization, fresh group-root
  resolution, attachment limits and caption/native-media presentation, retained
  group-context rendering, and retained-media lookup. It does not own prompts,
  Kmap objects, the directory, transport persistence, or logical sessions.
- One separate `kcode-kennedy-telegram-runtime` library owns the complete
  application workflow from the Telegram transport's accepted queue items to
  Kennedy logical sessions: typed event normalization, session lookup and
  binding, turn execution through the shared orchestration capability, bounded
  retry and timeout handling, atomic response delivery through typed Telegram
  capabilities, reset and interruption, six-hour rollover, detached-session
  repair, group-context refresh, and group-ingress handoff. It owns neither the
  raw Telegram queue nor the logical-session/Kmap persistence and must not
  duplicate their formats. General Kennedy orchestration and this Telegram
  runtime share one typed session-control capability rather than independently
  implementing checkpoints, stop handling, cancellation, or ingress semantics.
- Telegram is an in-process Kennedy application capability, not a loopback
  microservice. Its library exposes a typed service handle plus the supervised
  Telegram polling runtime; the application must call that handle directly
  rather than encode routes, JSON bodies, multipart requests, or HTTP status
  errors for same-process work. The main application listener exclusively owns
  browser-visible health at `/health`; Telegram must not expose an application
  listener or health endpoint. Telegram's external Bot API remains a genuine
  HTTP transport boundary.
- Kennedy owns the user directory, whitelist, root assignments, and
  authorization policy. A preauthorized Telegram handle is sufficient
  temporary authority before it has a stable numeric-ID binding and must not
  by itself block private or group admission. On the first observation whose
  normalized current handle exactly matches that unresolved entry, atomically
  bind the observed stable numeric Telegram identity through trust-on-first-use
  and thereafter treat that numeric identity as authoritative. A missing or
  mismatched handle must still fail closed, and a later handle change or reuse
  must never transfer an established numeric identity's authority.
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
- Treat an eligible Telegram group whose active roster is exactly Kennedy and
  one human as conversationally equivalent to a direct message: every accepted
  message from that human invokes Kennedy without requiring a mention, command,
  or reply to Kennedy. Keep the group session, root, and thread-capable
  transport identity distinct rather than collapsing it into the human's
  private session.
- Debounce ordinary inbound messages in direct chats and those two-person
  groups for 20 seconds after the newest accepted message. Each further message
  accepted on that stream during the interval restarts the full 20-second wait;
  once the stream is quiet, deliver the accumulated messages to Kennedy in
  order as one user turn, preserving every message and attachment. Keep this
  gathering state durable so a restart neither loses the batch nor bypasses its
  remaining wait. Larger groups retain explicit invocation behavior and are
  not subject to this direct-message batching rule.
- Telegram transport does not own permanent message history; Kennedy's ordinary
  session and Kmap lifecycle does. Retain only the transport working state
  needed for bounded live group context, media access, and pending Kmap ingress,
  and reclaim it once no live transport workflow needs it. Do not add an
  outbound transcript archive or durable outbox.
- Preserve media captions as message text associated with the media delivery in
  both directions. Inbound captions must reach Kennedy alongside the retained
  original, including captions on voice notes. Outbound text accompanying an
  attachment should be sent as that attachment's exact caption when Telegram
  supports captions for the media kind and the text fits Telegram's caption
  limit; otherwise deliver the complete text separately without truncating,
  discarding, or duplicating it. A caption is delivery presentation metadata,
  not part of the stored object's identity.
- Kennedy may initiate a private Telegram delivery directly or through a
  subagent, containing text, one or more staged or canonical Kweb object
  attachments, or both, to any explicitly targeted authorized user from any
  session, but only after that user has opened a private chat with the bot. The
  tool accepts the stable numeric user identity and object references, with an
  optional recipient-visible delivery filename for each attachment; Telegram
  transport resolves the private chat without exposing chat IDs to Kennedy or
  the Kennedy application. Apply the ordinary outbound-media size, filename, MIME,
  and native-media rules rather than creating a separate attachment store or
  embedding raw bytes in tool arguments. Do not impose a tool-specific
  attachment-count ceiling or reject repeated object references; existing
  request, session, object-size, and provider constraints are the relevant
  bounds.
- An out-of-band private delivery made through `SendTelegramDM` is a cold
  Telegram transport action. Do not create, select, bind, checkpoint, or
  otherwise mutate a target Kennedy session for it, do not add its text or
  attachment references to the target's Session History, and do not queue
  history ingress on its behalf. Preserve any current private-session pointer
  unchanged so an already-operating session in that chat continues to receive
  later inbound events normally; when none is active, only a later accepted
  inbound event may create a session through ordinary Telegram intake. This is
  distinct from Kennedy replying within an active private Telegram session:
  ordinary reply text, emitted objects, native media, filenames, and captions
  remain owned by and recorded in that session and use its bound event-delivery
  workflow.
- Preserve a short-lived reply bridge from every successful
  `SendTelegramDM` item to the already-existing source session. When the
  authorized recipient uses Telegram's direct-reply action on any exact
  outbound message or attachment produced by that delivery, route the accepted
  response into the source session's external-turn path rather than the
  recipient's ordinary private Telegram session. This source correlation does
  not make the cold send bind, select, or mutate the recipient's target-session
  pointer. Telegram transport owns only the provider message-ID correlation and
  accepted transport event needed until handoff; it is bounded working state,
  not an outbound transcript or durable delivery outbox. Session History owns
  the pending external turn and the source Chatend remains the durable record.
  A direct reply must remain accepted across restart, become visible to
  Kennedy at the next safe inference boundary without interrupting her current
  provider or tool work, and fall back to ordinary authorized Telegram intake
  if its source session has already become immutable before handoff.
- Kennedy may likewise initiate a Telegram delivery directly or through a
  subagent to any explicitly targeted known group from any session, including
  browser, private or group Telegram, history-ingress, audio-ingress, wakeup,
  and self-time sessions. The group tool has the same text, attachment,
  delivery filename, size, MIME, and native-media capabilities as initiated
  private delivery and targets the group's canonical Kmap root rather than
  exposing a raw Telegram chat ID. The Telegram session coordinator resolves
  that root through the group directory, while Telegram
  transport resolves the current chat and must freshly enforce the ordinary
  administrator, complete-roster, and historical-whitelist requirements before
  sending. The tool invocation and ordinary source-session/Kmap lifecycle are
  the durable record; do not create or select a participant session merely to
  own a group-wide delivery. Successful items may be followed by a later failed
  item without rollback. Use only ordinary bounded in-memory transport retries:
  failure ends the attempt, and no durable replay or exactly-once mechanism is
  required.
- Invocation controls when Kennedy responds, not which accepted bounded group
  context she may inspect. Passive group discussion and retained media should
  be available to an open participant session without pretending that messages
  from other participants belong to that participant.
- Raw retained group media stays behind an authorized staging tool. It should
  not be copied eagerly into every participant's model context.
- Reset and timeout handling must release stuck streams so later events can
  proceed without sacrificing accepted work. Never acknowledge or abort a
  bound Telegram event in a way that detaches its active Kennedy session before
  that session has durably entered history ingress; retry the ingress handoff
  first, then reset the transport stream and surface a useful failure to the
  Telegram chat.
- Bound every direct and group Telegram session to six hours from its creation.
  At that boundary, durably queue the complete session for history ingress and
  let the next event begin a fresh session. A turn already running at the
  boundary may finish through its separately bounded operation timeout, but an
  idle or recovered session must not remain open past the boundary. Repair
  detached active sessions into the same ingress path instead of abandoning
  their archives or staged Kmap transaction.
- Telegram sessions otherwise end through their explicit transport boundary;
  browser inactivity rules must not silently close them.

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
- Background observation must not rebuild an unchanged interactive browser
  view. Polling preserves live media playback, native controls, local form
  state, focus, disclosures, and scroll until presentation state truly changes.
- Conversation-history enumeration uses only bounded summaries. Load a
  completed immutable archive only for the currently selected record; never
  hydrate completed history in the background or retain unselected archives.
- Do not present accepted journal writes as a separate vague “saving” state.
  Surface real failures rather than suggesting that durable state is pending
  when it is not.
- Keep the composer editable while Kennedy works so the user can draft, but do
  not ordinarily send another turn until the current one completes. When
  Kennedy has explicitly begun an external-response wait for that source
  session, permit the authorized user to submit the awaited response through
  the same composer without opening a competing turn. Preserve one local draft
  per live conversation.
- A live operation needs an explicit stop control. Stopping must cancel the
  active model or tool work without losing the already accepted user command.
- Keep a closed conversation selected while its history ingress continues.
  Do not automatically create or select a replacement conversation.
- Order conversation history by actionable lifecycle state before recency:
  active work first, then queued or currently updating memory, then terminal
  ingress failures that require a manual retry, and only then fully ingressed
  read-only history. A failed ingress must remain conspicuous without
  displacing work that is still live or making forward progress.
- Show source and ingress activity as one continuous session history. Internal
  prompts, boxes, tools, and diagnostics may be collapsed in the ordinary view
  but must remain inspectable.
- Preserve an exact provider-boundary view for debugging. Human-friendly views
  may summarize presentation, but they must not rewrite the recorded bytes and
  claim that the result is exact.
- The Memory Explorer presents one local chronological journey through Kmap as
  a literal ordered stack of unique canonical nodes. The first entry is the
  selected root and has no incoming pointer. A node is appended only on its
  first visit in the current journey, with exactly one downward pointer from
  the earlier stack entry whose fixed or recent connection was actually used;
  unrelated graph edges are not rendered as journey pointers. Revisiting an
  existing entry selects it without appending, moving, or changing its pointer.
  Stack entries select the main node view and do not expand in place.
- Truncating a journey entry removes every later entry and pointer. Dehydrating
  an entry preserves its stack position and pointer but removes its outgoing
  fixed and recent connections from the aggregate open-connection view and
  makes it non-selectable until a rehydration refetches its current backend
  state. The selected hydrated node is shown in the main panel; beneath it, the
  explorer shows all fixed and recent connections from every hydrated journey
  entry, grouped by source. Fixed traversal pointers are blue and recent
  traversal pointers are yellow. Connection details may reveal target text
  without counting as navigation. The journey is browser-local presentation,
  while Kmap mutations and pending command state remain backend-durable.
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
- A user stop during self time ends the entire selected self-time run. It must
  cancel the active slice and suppress every later clean-slate slice, regardless
  of how much time remains on the original deadline.
- Persist and restore active self-time work, serialize starts across tabs, and
  prevent overlapping runs.
- Give Kennedy a visible warning near the run deadline and enough bounded time
  to wrap up. Do not impose an unrelated short timeout on otherwise valid
  long-running research.
- Use a threefold safety factor when setting finite production timeout
  allowances across agent turns, descendant intelligence, managed tools,
  provider transport, and persistence lock waits, so valid long-running work
  is not cut off prematurely. Keep explicit user-selected durations exact, and
  do not mistake retry backoff, polling cadence, leases, or diagnostic
  thresholds for work timeouts.
- Completed self-time work does not need a second ordinary history-ingress pass
  because the session already performs its own Kmap work.
- Background queues must release retrying or failed claims so one bad item does
  not block unrelated conversation, Telegram, audio, or ingress work. Retrying
  work must use explicit backoff rather than hot-looping the same operation and
  warning; keep failures actionable without flooding operational logs.

## Managed Code and Publication

- Kennedy's managed Rust libraries, Web libraries, and Rust binaries use one
  shared development-tool boundary with small typed adapters and consistent
  session/lease behavior.
- Every managed Rust library must include a root `Documentation.md` that is a
  complete agent-browsable reference for its current public API. It identifies
  every exported type, field, variant, function, method, trait callback, and
  important return/error contract, then explains ownership and invariants
  needed to use the boundary correctly without rediscovering them from source.
  Update this documentation in the same managed generation as every API change;
  a partial overview is not sufficient for check or publication readiness.
- `kcode-dev-tools-chatend` is the narrow adapter between that development-tool
  boundary and Chatend. It owns freeform source capture and reconciliation of
  successful managed-source snapshots into one stable Chatend box per project.
  Keep its API mechanical and minimal; it must not become another service or
  orchestration layer. `kcode-kennedy-sessions` retains tool authorization and
  execution plus Kweb object resolution; the agent-runtime and context-capacity
  owners retain their respective mechanical workflows, and Kmap retains
  storage.
- Editable source and immutable publications are separate. Published versions
  cannot be overwritten, and floating compatible selection resolves to an
  exact immutable artifact.
- Keep one complete active source box per managed project. Successful complete
  writes revise it; exact writes remain durable; failed writes leave it alone.
- Managed Web libraries may attach any staged or canonical Kweb object as an
  opaque file at a chosen safe relative path. Fonts, images, icons, and other
  static assets use this one general path rather than format-specific tools or
  base64 source workarounds. The filename extension determines whether a path
  is opaque, so Kennedy must choose an asset extension that reflects the
  resource and retain that convention in her Kmap guidance; do not add private
  per-generation metadata to remember the distinction. Source generations,
  browser checks, and immutable publications include the exact asset bytes
  atomically, while model-facing source snapshots identify opaque assets only
  by path and byte size instead of injecting their contents or hashes into
  context. Do not impose arbitrary file-count, per-file-size, or whole-tree
  size ceilings on managed Web libraries; Kennedy chooses practical project
  boundaries.
- Dependency declarations accept SemVer-compatible upgrades by default. Use an
  exact version pin only when a dependency is directly entrusted with API
  credentials or comparably critical data and the narrower trust boundary has
  a concrete justification. Ordinary source, rendering, routing, and Web
  library dependencies are not exact-pinned merely for predictability. The
  explicitly trusted `kcode-kennedy-app` application-update boundary defined
  under Overall Direction is the sole exception.
- Every Kennedy-owned Cargo project consumes published managed Rust libraries
  from crates.io, including the production workspace, auxiliary utilities,
  inactive crates, and development or repair tools. Keep editable Kcode source
  separate from Cargo dependency resolution: do not use dependency paths,
  `[patch]` entries, Cargo source replacement, or other local-generation
  tracking to wire managed libraries into any Kennedy-owned Cargo project.
  Publish a required managed-library version before a Kennedy project depends
  on it.
- Kennedy's managed check and publication tools own each library's validation
  and release operation. One offline operator utility may coordinate a batch of
  current managed Rust-library releases: it unlocks Kennedy's ordinary
  credential vault for the crates.io key, skips exact versions that are already
  published so interrupted runs resume safely, orders unpublished libraries by
  their managed-library dependencies, and waits for each release to become
  registry-visible before publishing its dependents. This coordinator must use
  the managed publication operation rather than duplicate its checks, fail
  closed while Kennedy is running, and never expose the publication credential
  in output, arguments, or source.
- Managed Rust checks and publications automatically apply rustfmt to their
  disposable source before the remaining validation. Formatting differences
  that rustfmt can repair must not become model-facing failures or consume
  another Kennedy turn; failure to run rustfmt or source that rustfmt cannot
  parse remains actionable.
- Managed checks and publications must provide the first-party runtimes needed
  by their release tests and return bounded actionable diagnostics from both
  stdout and stderr. A generic command-runner footer must not hide the actual
  test failure emitted on the other stream.
- A nested runtime may disable an incompatible inner sandbox only when its
  owning runner positively places it inside a hardened, disposable outer
  sandbox. Keep the override scoped to that image; do not weaken the same
  runtime when it executes directly on the host.
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
- One focused `kcode-kennedy-prompts` library owns the exact static runtime
  prompt-layer text, including `KennedyIdentity.txt`, and exposes it through a
  read-only typed API. Kennedy opens those bundled layers from the library and
  must not locate or read a configurable system-prompt directory at runtime.
  Kennedy orchestration retains selection and composition of session and
  channel layers, dynamic context, and current runtime facts; the prompt
  library owns none of that policy.
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
- Keep the canonical operator CPU profile at `data/kennedy-cpu.svg`. Capture it
  by sampling the single running Kennedy server, without adding profiler logic
  to the application or changing its workflow.
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
- One small typed credential-vault library owns encryption, payload validation,
  plaintext zeroization, and durable atomic file replacement. `kcode-kennedy-app`
  owns the vault path, human passphrase prompts, application secret names,
  feature policy, maintenance exclusion, and backup decisions; it must not
  duplicate or bypass the library's encrypted persistence format.
- A library that receives a reusable credential is security-critical. Pin and
  inspect the exact source used, and repeat that review before upgrading it,
  except for the explicitly trusted `kcode-kennedy-app` application-update
  boundary defined under Overall Direction.
- Maintenance operations that require exclusive persistence access must fail
  closed while Kennedy may still be running.
