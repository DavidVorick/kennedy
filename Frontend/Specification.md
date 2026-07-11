# Frontend Specification

## 1. Scope

The frontend is a pure HTML, CSS, and JavaScript application served over localhost by the kweb Rust backend. It has no NodeJS, TypeScript, bundler, or compile step. It provides the user interface for chatting with Kennedy, inspecting Kennedy's full context window, and exploring the kweb memory graph.

## 2. Responsibilities

- Render the chat interface between the user and Kennedy.
- Render Kennedy's full context window, including frontend-composed system prompts, loaded kweb nodes, tool calls, and tool results.
- Render a memory explorer for durable kweb nodes, connections, history, and provenance.
- Orchestrate API calls to the kweb backend and intelligence backend.
- Manage conversation transcripts, context glue, loaded-memory state, and any prompt-facing identifier mappings.
- Compose system prompts from the kmap agent manual and the active session agent manual.
- Preserve browser-side UI state for the current page session.
- Display errors clearly without hiding failed tool calls or backend failures.

The frontend must not enforce kweb invariants beyond basic form validation. The kweb backend remains authoritative.

## 3. File Layout

The eventual frontend implementation should use this shape:

```text
Frontend/
  Specification.md
  public/
    index.html
    css/
      styles.css
    prompts/
      SystemPromptKmapAgentManual.txt
      ConversationAgentManual.txt
      HistoryIngressAgentManual.txt
    js/
      api.js
      app.js
      chat.js
      context_inspector.js
      memory_explorer.js
      intelligence.js
      prompt_composer.js
```

Additional plain JavaScript modules are allowed if they remain browser-native ES modules.

## 4. UI Layout

### 4.1 Top-Level Shell

The app uses a two-column layout:

- **Left column**: clean conversation between the user and Kennedy.
- **Right column**: Kennedy's full context window and diagnostic activity.

A tab or mode switch exposes the memory explorer. The memory explorer may replace the right column or use a full-width route-style view, but the chat must remain easy to return to.

### 4.2 Chat Panel

The chat panel must include:

- Scrollable message transcript with user and Kennedy messages.
- Multiline message input.
- Send button.
- End conversation button.
- Conversation status indicator.
- Loading indicator while Kennedy is thinking or tools are executing.
- Error banner for failed API calls.

The visible transcript must be unpolluted: normal chat display shows only user and Kennedy dialog, not tool calls or context internals.

### 4.3 Context Inspector

The context inspector must show:

- Active system prompt text sent to the intelligence backend.
- Current frontend conversation ID and intelligence session ID.
- Loaded in-context knowledge nodes, including frontend display identifiers, short names, short descriptions, long descriptions, active connections, and fanout connections.
- Tool-call log with request payloads, response payloads, timing, and status.
- Current `LoadNode` counters and loaded-node limits.
- Provider/model metadata returned by the intelligence backend.

The context inspector is for transparency. It must update after every tool call and every assistant turn.

### 4.4 Memory Explorer

The memory explorer must include:

- Open current user root node button.
- Node detail view for short name, descriptions, durable ID, timestamps, and connections.
- Clickable active and fanout connections.
- History list for the current node.
- Provenance detail viewer for selected history entries.
- Back/forward navigation within the explorer's in-page navigation stack.

The memory explorer is for viewing and navigation only.

## 5. Browser State Model

The frontend keeps this state in JavaScript memory:

```js
{
  kwebBaseUrl: "http://127.0.0.1:4321",
  intelligenceBaseUrl: "http://127.0.0.1:4322",
  conversationSessionId: null,
  intelligenceSessionId: null,
  transcript: [],
  contextGlue: {
    loadedNodes: [],
    displayIdToNodeId: {},
    nodeIdToDisplayId: {}
  },
  context: null,
  toolLog: [],
  selectedExplorerNodeId: null,
  explorerBackStack: [],
  explorerForwardStack: [],
  isBusy: false
}
```

Browser reload may lose in-memory frontend state in v1. Conversation transcript durability is a frontend concern until a later specification introduces persistence for transcripts outside provenance records.

## 6. Frontend API Client

All backend calls must be wrapped in small JavaScript functions that:

- Set `Content-Type: application/json` for JSON requests.
- Parse JSON success and error envelopes.
- Throw typed frontend errors with status, code, message, and details.
- Avoid duplicating fetch logic in UI rendering code.

## 7. User Flows

### 7.1 App Startup

1. Load `index.html` from the kweb backend.
2. Call kweb `GET /health` and intelligence `GET /health`.
3. Call kweb `POST /api/bootstrap`.
4. Initialize a frontend conversation ID and in-memory transcript.
5. Fetch the root node with `GET /api/user` and `POST /api/nodes/load`, then build the initial frontend context glue.
6. Load `SystemPromptKmapAgentManual.txt` and the active session manual from static assets.
7. Compose the system prompt in the frontend.
8. Start an intelligence session with `POST /api/intelligence/sessions`.
9. Render the initial transcript and context inspector.

If the intelligence backend is unavailable, the frontend must still allow memory explorer usage and display a clear warning that chat is unavailable.

### 7.2 Sending a Chat Message

1. User types a message and clicks Send.
2. Disable input and show busy state.
3. Append the user message to the frontend transcript state.
4. Send the user turn to intelligence via `POST /api/intelligence/sessions/{llm_session_id}/turns`.
5. If the intelligence backend returns tool calls, translate frontend display identifiers to durable kweb node IDs, execute the corresponding kweb backend calls, update frontend context glue, and send tool results back using `POST /api/intelligence/sessions/{llm_session_id}/tool-results`.
6. Repeat until final assistant text is returned.
7. Append final assistant text to the frontend transcript state.
8. Render transcript/context.
9. Re-enable input.

### 7.3 Ending a Conversation

1. User clicks End Conversation.
2. Confirm intent if there is unsent text.
3. Create a kweb provenance record for the transcript with `POST /api/provenance` if the conversation should be ingested.
4. Start or display the frontend-managed history-ingress workflow status.
5. Disable further sends in the ended conversation.
6. Offer a button to start a new conversation.

### 7.4 Exploring Memory

1. User opens memory explorer.
2. Frontend calls `GET /api/user` for the root node ID.
3. Frontend calls `GET /api/nodes/{node_id}`.
4. Render node details and connections.
5. On connection click, load the target node and push the previous node onto the back stack.
6. On history click, call `GET /api/nodes/{node_id}/history` and optionally `GET /api/provenance/{provenance_id}`.

## 8. Intelligence Backend API Usage

The frontend consumes these intelligence endpoints:

- `GET /health`
- `POST /api/intelligence/sessions`
- `POST /api/intelligence/sessions/{session_id}/turns`
- `POST /api/intelligence/sessions/{session_id}/tool-results`
- `DELETE /api/intelligence/sessions/{session_id}`

The frontend must treat intelligence tool-call requests as untrusted until successfully validated by the kweb backend. Unknown tool names must be returned to the intelligence backend as failed tool results rather than executed. The frontend owns any display-ID translation before calling the kweb backend.

## 9. Rendering Requirements

- Escape all user, model, node, and provenance text before inserting into HTML.
- Use semantic HTML for buttons, forms, headings, and panels.
- Support keyboard submission with `Ctrl+Enter` or `Cmd+Enter`.
- Keep chat scroll pinned to bottom unless the user has manually scrolled upward.
- Show durable IDs in monospace and allow copy-to-clipboard.
- Do not use external CDN assets in v1.

## 10. Error Handling

Frontend errors must show:

- Human-readable message.
- Backend error code when present.
- Retry action when safe.
- Diagnostic details in the context inspector.

If a tool call fails, the frontend must preserve the failed call in the tool log and send the failure back to the intelligence backend so Kennedy can recover or explain.
