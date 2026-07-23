# Chatend Overhaul Clarifications

This document records design clarifications supplied while reviewing the
context-box overhaul. These constraints govern the overhaul where the earlier
proposal is ambiguous or implies a different architecture.

## Session Boundary

- A session remains the complete unit of conversation, persistence, history
  ingress, and permanent Kweb provenance.
- Sessions are independent. Do not introduce a segment layer or combine
  several sessions into one history-ingress run.
- Exactly one history-ingress run processes exactly one session.
- The complete session includes its initial context, automatic root-node
  loads, user activity, Kennedy activity, tool activity, context
  transformations, and history-ingress activity.
- History ingress changes how the completed session is processed; it does not
  change the identity or boundary of the session.

## Boxes

- Every model-visible component of the Chatend is contained in a box. There is
  no model-visible message, tool result, context fragment, or transformation
  outside the box model.
- Each user message creates exactly one user-owned message box.
- Each Kennedy message creates exactly one Kennedy-owned message box.
- A user or Kennedy message does not also create a separate parallel message
  record in the Chatend domain model.
- Box operations are processed sequentially before rebuilding the model-facing
  Chatend projection.
- `EventBatch` is not a Chatend domain concept. An implementation may still use
  one storage transaction or commit boundary to prevent a crash from exposing
  part of one accepted multi-box tool transition.
- When one stateful tool result changes several slots, the controller processes
  the tool's existing ordered slot sequence from front to back. This is merely
  the chronological application rule; it does not introduce a separate
  user-visible abstraction.

## Box Representation Terminology

- A hydrated box exposes its selected contents in active context.
- A dehydrated box has had its substantive contents removed from active
  context while retaining compact identity, lineage, revision, and restoration
  information.
- A summarized box exposes replacement text authored by Kennedy instead of the
  complete source contents.
- The term `blanched` is retired. A box whose contents are pulled out of
  context is dehydrated.
- The term `dirty` is retired. A dehydrated or summarized representation whose
  canonical source has advanced is stale.
- Continuation markers remain in active context. When a box's current
  occurrence moves to a later event, its preceding occurrence becomes a compact
  immutable marker pointing to that next occurrence. This gives Kennedy useful
  chronological evidence without repeating the earlier contents.
- Only Kennedy may ordinarily dehydrate, summarize, or rehydrate boxes. The
  controller performs automatic dehydration only when preparing a terminated
  session for history ingress.

## Kweb Is an Ordinary Tool

- Kweb is not a special context subsystem in the generic Chatend controller.
  It participates through the same tool and box interfaces as other tools.
- Kweb tool behavior is described by the tool definitions supplied in
  Kennedy's system prompt.
- Session startup automatically invokes `LoadNode` for the user root and the
  Kennedy root. These are ordinary tool calls and produce ordinary tool and box
  history.
- Kweb-specific node rendering, canonical committed identifiers, pending
  session identifiers, connection policy, and transaction construction belong
  in the Kennedy Kweb tool adapter rather than the reusable context-box
  controller.

## In-Progress Durability

- An in-progress session must survive process and machine restarts.
- In-progress session state is stored in Kennedy-owned local disk storage.
- Do not put incomplete sessions, working checkpoints, or transient session
  revisions into Kweb's permanent immutable object store.
- The local durable state must retain everything required to resume the session
  exactly, including its events, boxes, canonical revisions, active
  representations, tool state, context accounting, provider receipts, and
  history-ingress progress.
- Session History keeps the one local completed-session object list described
  below. In-progress lifecycle, ordering, commands, retry state, and session
  contents live in the session's own append-only file rather than a second
  session database.

## One Session Maps to One Kweb Transaction

- After history ingress completes successfully, the complete session is
  permanently committed through exactly one `kcode_kweb_db::Transaction`.
- Each completed session maps to exactly one Kweb transaction, and each such
  session transaction maps to exactly one session.
- All Kweb node creations, node updates, connections, and other knowledge
  changes produced by that session's history ingress are bundled into that one
  transaction.
- The complete session archive is created as an immutable object in the same
  transaction.
- Files supplied by the user, including images, PDFs, and other attachments,
  become immutable Kweb objects in that same transaction.
- Nodes created or updated by the transaction may reference the session archive
  and attachment objects as required by the final provenance model.
- Because all durable knowledge changes and source objects are committed
  together, every update made by the session is intrinsically associated with
  that session. A separate cross-transaction association mechanism is not
  needed.
- A failed or incomplete history-ingress run must not finalize a partial Kweb
  transaction. Its recoverable working state remains local until ingress
  resumes and reaches successful completion.
- Kennedy must not keep a live `kcode_kweb_db::Transaction` open across model
  calls or restarts. The Kweb tool adapter therefore maintains a locally
  durable staged transaction plan and read-your-writes view during history
  ingress, then constructs and finalizes the one real Kweb transaction at
  completion.

## Objects and Communication Adapters

- A session may receive and provide arbitrary objects. The Chatend represents
  either direction inside the owning user or Kennedy message box as
  `Object provided: <ID>`; object transfer does not create model-visible
  content outside a box.
- Object bytes do not belong in the model-facing box text. The identifier is
  cheap to retain in the session and may be attached to any number of Kweb
  nodes after the object is committed.
- The communication adapter for the session owns transport-specific ingress
  and egress. Browser UI, Telegram, and later surfaces may support different
  media kinds, limits, metadata, and delivery mechanisms.
- Kennedy is told the active session's object capabilities and may provide an
  object only through behavior supported by that communication adapter.
- Kennedy may follow an object reference found on a Kweb node and ask the
  communication adapter to deliver that object to the user.
- The backend takes durable local custody of newly received object bytes for
  the complete in-progress session. They remain local until successful history
  ingress commits them as immutable objects in that session's one Kweb
  transaction.
- A restart must neither lose an accepted incoming object nor duplicate an
  accepted outgoing delivery. Object custody and delivery therefore participate
  in the same recoverable session state as boxes and tool activity.

## Permanent Retention

- Successfully completed session data is retained permanently.
- The session archive, user attachments, event history, source contents,
  Kennedy transformations, history-ingress work, and resulting Kweb mutations
  are not garbage-collected.
- Conversation deletion or hiding must not claim that accepted permanent Kweb
  data has been erased.

## Context Budget

- The live Chatend input budget is exactly 70 percent of the selected model's
  effective context window.
- History ingress may use 100 percent of the selected model's effective context
  window and does not reserve a separate output allowance.
- The exact integer calculation, rounding behavior, and refusal boundary must
  be specified and tested.
- Context accounting should be as accurate as practical and conservative
  enough that underestimating usage is highly unlikely.
- Actual provider input measurements should replace estimates whenever they
  are available and must be tied to the exact inference manifest that produced
  them.
- Estimation between measurements must include all provider-visible material,
  including control text, tool definitions, box framing, continuation markers,
  stale-box information, and budget status.

## Provider Caching

- V1 should prioritize a correct, reliable box system and may use the simplest
  whole-context provider strategy.
- Stable rendering and straightforward prefix-cache friendliness are
  sufficient for the first implementation.
- More aggressive provider-cache and continuation optimization is explicitly
  expected later because token cost is important, but it must not complicate
  the V1 correctness model.

## V1 Object Staging and Kweb Commit

- V1 does not require a more complex prepared-object or staged-file handoff API
  in `kcode-kweb-db`.
- The Chatend controller takes durable local custody of session objects and
  writes their bytes to its own staging storage.
- At final session commit, Kennedy reads each staged object from local storage
  and supplies its bytes through the existing Kweb transaction API. Kweb then
  writes the accepted object into its own object store.
- The resulting second disk write is an accepted V1 cost. A future optimization
  may add staged-file adoption or another zero-copy handoff after the system is
  mature enough to justify the extra API complexity.
- The finalization code collects the canonical node and object IDs allocated
  while constructing the real Kweb transaction and returns their mappings to
  the session controller after a successful commit.
- V1 does not add an API for preparing or persisting an exact
  `TransactionPackage` before submission. The locally staged session and
  transaction plan are flushed and `fsync`ed, after which Kennedy constructs
  and finalizes the real Kweb transaction through the existing API.
- There is an accepted brief crash window after Kweb may have committed but
  before Kennedy has recorded local completion. Restart cannot prove the exact
  outcome or reconstruct byte-identical signed transaction bytes through the
  current API. V1 accepts the resulting small corruption or duplicate-commit
  risk; prepared-package retry is deferred.

## Temporary Identities

- A session uses one shared monotonically increasing temporary-identity counter
  for boxes, pending nodes, and pending objects. Allocating any one of those
  identities consumes the next value, so their numeric portions never overlap.
- A pending Kweb resource is rendered in the compact form `pending:<number>`,
  for example `pending:47`.
- Pending identities are session-local, stable across restart, never reused,
  and may be referenced by later staged operations in the same session.
- Existing committed Kweb nodes and objects continue to use their canonical
  eight-character IDs. On final commit, the controller resolves every pending
  reference using the canonical IDs returned while constructing the
  transaction.

## In-Progress Session Persistence

- Each in-progress session has exactly one append-only local file.
- Chatend records within that file use JSON because the session and box model
  is expected to evolve rapidly. Additive fields should ordinarily remain
  readable without a state-schema migration.
- The file uses a small stable framing envelope so it can also append raw object
  payload records without Base64 expansion. JSON event/transition records and
  raw object records share one monotonically ordered journal.
- One accepted atomic transition is appended as one JSON record containing its
  ordered events. Large incoming objects are appended as raw records and are
  referenced from later JSON by their pending IDs and journal locations.
- Accepted messages, box changes, tool activity, staged transaction-plan
  changes, inference receipts, history-ingress progress, object custody, and
  finalization state are all recoverable from this one file.
- A record is acknowledged only after its complete frame is flushed and
  `fsync`ed. Recovery accepts complete frames in order and discards only an
  incomplete final frame.
- The append-only session file remains the restart authority until history
  ingress succeeds and the complete session is committed to Kweb.

## Local Session Object List

- The application domain currently called Conversation History becomes Session
  History.
- Session History maintains only the small local file already proposed by the
  user: a list of permanent Kweb object IDs for successfully committed session
  archives. No richer application index is required for V1.
- The complete contents and display metadata of a historical session are not
  duplicated locally. The frontend loads the named immutable object from Kweb
  when it needs that session's details.
- The session archive does not contain its own permanent object ID. Final
  transaction construction allocates that ID, and the local list records it
  only after the Kweb transaction succeeds, so there is no circular archive
  dependency.

## V1 Kweb Write Lane

- V1 has one global lane for sessions with Kweb write access. History ingress,
  self time, and any other read-write session types acquire that lane, and only
  one such session runs at a time.
- Kweb read-only sessions remain fully parallel and do not acquire the global
  write lane.
- Self time acquires the write lane before it loads Kweb state, so its initial
  view cannot become stale while waiting to write.
- When another session changes from read-only to read-write, it acquires the
  lane and revalidates every Kweb node it previously loaded.
- If a loaded node changed since the session read it, its canonical box
  revision advances and its dehydrated or summarized representation becomes
  stale. The corresponding system-authored update occurrence identifies the
  affected box but contains no change explanation or diff. Kennedy hydrates the
  box if she cares what changed.
- A conversational session terminates before history ingress acquires the
  write lane. In preparing that terminated session for ingress, the controller
  automatically dehydrates its existing boxes.
- Kweb update notices begin dehydrated, and Kennedy chooses which updates to
  hydrate before authoring the staged transaction plan.
- The controller explicitly hydrates the history-ingress system-prompt boxes,
  Kennedy's root node, and the user's root node. Ingress therefore always begins
  with its basic instructions and Kweb navigation anchors even if Kennedy
  dehydrated those boxes during the source session.

## System and Tool Boxes

- System prompts are ordinary persisted boxes and form part of the complete
  session history.
- Kennedy may summarize, dehydrate, or rehydrate system-prompt boxes using the
  same box operations as other model-visible material.
- Definitions for critical controller behavior, including Ktools, are supplied
  in system-prompt boxes. Kennedy is allowed to dehydrate those boxes and is
  expected to learn not to discard instructions she still needs.
- The provider-level `call_ktool` function remains registered throughout every
  Kennedy inference even if Kennedy dehydrates the system-prompt box that
  explains how to use it.
- When one Kennedy response invokes several tools, each invocation and each
  tool's resulting owned boxes are recorded independently in their actual
  chronological order.
- Only Kennedy may manage active box representations. The user interface may
  inspect the durable and active views but cannot summarize, rehydrate, or
  dehydrate boxes.

## Main and Ingress Context Budgets

- A session whose event history will later be supplied to history ingress may
  use at most 70 percent of the selected model's effective context window.
- History ingress may use the full effective context window. The remaining 30
  percent is reserved for ingress's additional reasoning, hydration, tools,
  and summaries rather than for extending the source session.
- A hydration or tool operation whose projected result would exceed the
  applicable context limit is rejected and produces a visible capacity error.
- If even that error cannot be added without taking the source session over 72
  percent, the source session is forcibly ended and queued for history ingress.
  This is an emergency boundary for a session that has repeatedly failed to
  reduce its context, not an ordinary rollover mechanism.
- If history ingress reaches its 100-percent limit and can no longer make
  progress, it ends at its current state. Kennedy constructs and submits the
  session's one Kweb transaction from the staged work completed so far.
- V1 does not reserve part of the effective window for provider output in
  either calculation.

## Self-Time Handoff

- `ResetContext` is removed; dehydration, summarization, and rehydration provide
  the replacement context-management operations.
- Self time retains its existing ability to leave one message for the next
  self-time session so Kennedy can state what to do next.
- Other V1 session types do not introduce a general handoff or segment
  mechanism. More elaborate multi-agent handoffs are deferred.

## Legacy Sessions

- At overhaul cutover, every still-open legacy browser, Telegram, group, or
  other session is removed from live persistence and written as one complete
  text file under `data/archive/unfinished-sessions/` for manual ingress.
- All completed legacy session records are also archived and removed from live
  persistence. They are destined for off-machine archival storage rather than
  a legacy compatibility reader.
- The overhaul contains no legacy session loader and does not invent box events
  or synthetic history for old records.

## V1 Object Memory Limit

- Keep the existing Kweb limit of 32 GiB for an individual object and for the
  aggregate object payload of one transaction.
- Kennedy does not impose a smaller application-side limit merely to reserve
  RAM. Target machines have ample memory for the V1 implementation.
