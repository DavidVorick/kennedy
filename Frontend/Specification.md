# Frontend Specification

## 1. Scope

The frontend is a browser-native HTML, CSS, and JavaScript application. It has
no Node.js dependency, package manager, bundler, TypeScript, or compile step.
The Kweb backend serves it on localhost.

The frontend owns Kennedy's UI and all ephemeral orchestration: the clean
transcript, the complete chatend, context glue, prompt composition, agent tool
loops, and the memory explorer. It does not own durable data or Kweb mutation
rules.

## 2. Source Layout

```text
Frontend/
  Specification.md
  SystemPrompts/
    SystemPromptKmapAgentManual.txt
    ConversationAgentManual.txt
    HistoryIngressAgentManual.txt
  public/
    index.html
    css/
      styles.css
    js/
      api.js
      app.js
      chatend.js
      conversation.js
      history_ingress.js
      intelligence.js
      kweb_context.js
      memory_explorer.js
      prompt_composer.js
      render.js
      tools.js
```

Files under `public/js` are browser-native ES modules. Module boundaries may
change during implementation, but UI rendering, API transport, chatend state,
and tool execution must remain separate concerns.

## 3. Backend Addresses

The frontend defaults to:

- Kweb API and static origin: `http://127.0.0.1:4321`
- Intelligence API: `http://127.0.0.1:4322`

The values are defined once in `app.js` so alternate local ports do not require
changes throughout the codebase.

## 4. Application State

All application state is held in JavaScript memory:

```js
{
  mode: "conversation", // conversation | ingesting | explorer
  transcript: [],       // user and Kennedy text only
  chatend: [],           // complete normalized LLM messages
  context: {
    loadedNodeIds: [],   // directly loaded durable IDs; root included
    fullNodeIds: new Set(),
    nodesById: new Map(),
    shortToDurable: new Map(),
    durableToShort: new Map(),
    nextShortId: 1
  },
  loadCalls: 0,
  conversationStartedAt: null,
  currentProvenanceId: null,
  toolLog: [],
  explorer: {
    currentNodeId: null,
    back: [],
    forward: []
  },
  busy: false
}
```

The frontend does not use local storage, IndexedDB, cookies, or service-worker
caches for persistence. Reloading or abruptly closing the page may lose the
current conversation.

## 5. Chatend Model

The chatend is the exact normalized message sequence submitted to the
intelligence backend. It contains:

- the composed system prompt,
- the clean conversation transcript,
- the current Kweb context,
- tool calls and tool results that remain in context.

The frontend submits the entire chatend on every generation request. The
context inspector renders the same data structure, so it shows what Kennedy is
actually sent rather than an approximation.

The clean transcript is maintained separately. It contains only user and final
Kennedy messages and is the source used to create conversation provenance.

### 5.1 Context Rebuild

The frontend can rebuild a chatend from:

1. the active system-prompt manuals,
2. session content that must survive reset—the clean conversation transcript
   for conversation sessions or provenance data for history ingress,
3. freshly materialized Kweb context.

`ResetContext` first resolves all supplied short identifiers to durable IDs.
It then clears loaded Kweb data and short-ID mappings, reloads the user root and
the supplied nodes, and rebuilds the chatend. Previous Kweb context and tool
activity are omitted. The clean transcript or provenance input remains.
During an active tool loop, the rebuilt chatend ends with the assistant's
ResetContext call and a correlated tool result containing the newly loaded
context, allowing generation to continue normally.

## 6. Context Glue

The Kweb API returns durable IDs. The frontend converts every node exposed to
Kennedy into an in-context node:

```json
{
  "identifier": 3,
  "shortName": "Example Node",
  "shortDescription": "Short description.",
  "longDescription": "Long description.",
  "activeConnections": [
    {"identifier": 4, "shortName": "Related Node"}
  ],
  "fanoutConnections": []
}
```

Short identifiers are positive integers allocated in first-seen order. A
durable node receives one short identifier per context, even if it appears in
multiple LoadNode results. Durable IDs are never included in LLM-visible tool
results or prompt context.

`fullNodeIds` distinguishes nodes whose complete long description is in context
from nodes that appear only as connection summaries. Summary-only nodes still
receive short identifiers so Kennedy can load them, but they are not treated as
fully materialized knowledge.

A directly loaded node is one for which the frontend executed the LoadNode
operation. Active connections returned alongside it are in context and receive
short identifiers, but do not count toward the seven-loaded-node limit.

The user root is directly loaded at session start, is always the first loaded
node after reset, and counts toward the limit.

## 7. Prompt Composition

The frontend fetches manuals from `/system-prompts/{filename}`.

Conversation instructions are composed, in order, from:

1. `SystemPromptKmapAgentManual.txt`,
2. `ConversationAgentManual.txt`.

History-ingress instructions are composed from:

1. `SystemPromptKmapAgentManual.txt`,
2. `HistoryIngressAgentManual.txt`.

Manual contents are inserted without rewriting. The prompt composer may add
small machine-generated delimiters and the current context block, but must not
duplicate behavioral instructions already present in the manuals.

## 8. Agent Tools

The intelligence backend only transports tool calls. The frontend validates
the tool name and argument shape, resolves short IDs, calls the Kweb API, and
returns an LLM-visible result.

### 8.1 `LoadNode`

LLM schema:

```json
{
  "identifier": 3
}
```

Execution:

1. Reject the call if the session's LoadNode call budget is exhausted.
2. Reject the call if seven nodes are already directly loaded.
3. Resolve the short identifier.
4. Call `GET /api/v1/nodes/{durable_id}/context`.
5. Add the requested durable ID to the directly loaded set.
6. Mark the requested node and returned active-connection nodes as full.
7. Assign short IDs to every newly seen node or connection summary.
8. Convert the payload to in-context node shapes and return it as the tool
   result.

Every model-requested LoadNode invocation consumes one call from the session or
turn budget, including failed calls. Loads performed internally while starting
or resetting a context do not consume that budget.

### 8.2 `ResetContext`

LLM schema:

```json
{
  "identifiers": [3, 8]
}
```

The frontend resolves the identifiers before clearing the old map, then reloads
the root followed by the supplied nodes in their given order. The root must not
also appear in the argument list, and the resulting direct-load set must not
exceed seven nodes.

Reset preserves the current LoadNode counter. Its tool result contains the
complete newly loaded Kweb context.

### 8.3 `ConnectNodes`

LLM schema:

```json
{
  "identifiers": [2, 3, 8]
}
```

Every identifier must refer to a node whose full payload is in context. This
includes directly loaded nodes, their full active-connection expansions, and
full nodes returned by create or update operations; it excludes summary-only
connection references. The frontend resolves the IDs and calls
`POST /api/v1/connections`. Returned nodes refresh matching frontend context
records. The tool result reports the updated in-context node shapes.

### 8.4 `CreateNode`

Available only during history ingress.

```json
{
  "parentIdentifiers": [2, 3],
  "shortName": "New Memory",
  "shortDescription": "Short description.",
  "longDescription": "Long description."
}
```

The frontend resolves the parents and calls `POST /api/v1/nodes`, supplying the
current provenance ID. The created node is assigned a short identifier and
marked as full before it is returned to Kennedy. Creation does not make the
node directly loaded unless a later LoadNode call loads it.

### 8.5 `UpdateNode`

Available only during history ingress.

```json
{
  "identifier": 3,
  "newShortName": "Updated Memory",
  "newShortDescription": "Updated description.",
  "newLongDescription": "Updated long description."
}
```

The frontend resolves the node and calls `PUT /api/v1/nodes/{durable_id}` with
the current provenance ID. It refreshes the in-context representation and
returns the updated node.

### 8.6 Tool Failures

Unknown tools are never executed. Invalid arguments, exhausted budgets, missing
short IDs, and backend failures are returned to Kennedy as failed tool results
with a short error code and message. They are also recorded in the diagnostic
tool log.

## 9. Conversation Flow

### 9.1 Start

1. Fetch all system-prompt manuals.
2. Check both backend health endpoints.
3. Fetch `GET /api/v1/providers` from the intelligence backend.
4. Fetch `GET /api/v1/user`.
5. Create empty transcript and context state.
6. Load the user root and build the initial chatend.
7. Render an empty conversation ready for input.

If the intelligence backend is unavailable, chat is disabled but the memory
explorer remains usable.

### 9.2 User Turn

1. Append the user message to the clean transcript and chatend.
2. Set the per-turn LoadNode counter to zero.
3. Submit the complete chatend, conversation tools, and configured model to
   `POST /api/v1/generate` on the intelligence backend.
4. If Kennedy requests tools, append the assistant tool-call message, execute
   every call sequentially in response order, append correlated tool-result
   messages, and generate again. State changes from one call are visible to the
   next call in the same response.
5. Continue until Kennedy returns final text.
6. Append final text to the clean transcript and chatend.

A conversation permits at most 20 model-requested LoadNode calls per user turn.
The UI remains in a busy state for the entire tool loop.

### 9.3 End

When the user ends the conversation or starts a new one:

1. disable further input for the old conversation,
2. serialize only the clean user/Kennedy transcript,
3. create a provenance node with source `conversation` and the conversation's
   start time as `source_created_at`,
4. run history ingress using the returned provenance ID,
5. show completion or failure,
6. create a fresh conversation when requested.

## 10. History-Ingress Flow

History ingress has a new chatend and context. It does not reuse the ended
conversation's Kweb tool history.

1. Fetch `GET /api/v1/provenance/{provenance_id}` from the Kweb backend.
2. Compose history-ingress instructions.
3. place the provenance data into the retained session content,
4. load the user root,
5. set the session LoadNode counter to zero,
6. generate with all five history-ingress tools,
7. execute tools until Kennedy returns final text,
8. discard the final text and mark ingress complete.

At most 50 model-requested LoadNode calls are allowed across the whole ingress
session. Zero CreateNode or UpdateNode calls is valid.

## 11. User Interface

### 11.1 Conversation Panel

The left panel contains only:

- user and Kennedy messages,
- multiline input,
- Send and End Conversation controls,
- busy and ingress status,
- visible errors.

Tool calls and internal context never appear in the clean transcript.

### 11.2 Context Inspector

The right panel renders:

- composed instructions,
- complete normalized chatend messages,
- directly loaded nodes and their active expansions,
- current short-ID mappings,
- LoadNode usage and limit,
- tool calls, results, duration, and failure state,
- selected provider and model.

### 11.3 Memory Explorer

The explorer starts at the user root and supports:

- viewing a full knowledge node with `GET /api/v1/nodes/{node_id}`,
- following active and fanout connections,
- viewing the node's history chain with
  `GET /api/v1/nodes/{node_id}/history`,
- opening a history entry's source with
  `GET /api/v1/provenance/{provenance_id}`,
- browser-local back and forward navigation.

The explorer does not edit durable data.

## 12. Rendering and Error Requirements

- Insert untrusted text with `textContent`, never `innerHTML`.
- Keep user input disabled while a generation or tool loop is active.
- Preserve the diagnostic record of failed calls.
- Display backend error messages without exposing stack traces.
- Use semantic controls and visible keyboard focus.
- Support `Ctrl+Enter` and `Cmd+Enter` for message submission.
- Use no remote scripts, fonts, stylesheets, or other CDN assets.

## 13. Verification

Frontend tests may run directly in a browser or with lightweight Rust-served
fixtures; they must not introduce a production build step. At minimum verify:

- stable short-ID assignment and reset,
- seven-direct-load enforcement,
- conversation and ingress LoadNode budgets,
- short-to-durable translation for every tool,
- chatend rebuilding after ResetContext,
- clean-transcript serialization,
- HTML escaping,
- recovery from failed intelligence and Kweb requests.
