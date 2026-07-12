# Kennedy Technical Design

## 1. Purpose and Authority

Kennedy is a local-first memory application built around the kweb described in
`UserSpecification.md`. The user specification defines product behavior. This
document defines the architecture used to implement that behavior, and the
component specifications define the detailed contracts.

The MVP has three runtime components:

1. **Frontend**: a browser-native HTML/CSS/JavaScript application. It owns the
   user interface, ephemeral sessions, the chatend, short identifiers, prompt
   composition, and agent tool orchestration.
2. **Kweb backend**: a Rust HTTP service. It owns SQLite, durable kweb data, and
   every graph and history invariant.
3. **Intelligence backend**: a Rust HTTP service. It translates a complete LLM
   request into a provider request and normalizes the provider response. It is
   stateless between generation requests.

System-prompt manuals are frontend source assets under `Frontend/SystemPrompts`.

## 2. Design Principles

- The frontend is the single authority for the current chatend.
- The Kweb backend is the single authority for durable memory.
- The intelligence backend never needs to understand Kennedy, the kweb, or its
  tools.
- The frontend sends the complete chatend on every generation request. It does
  not depend on opaque provider-side conversation state.
- The OpenAI adapter uses stateless Responses API requests. Provider output
  items required to continue a reasoning/tool turn are round-tripped inside
  the frontend-owned chatend; `previous_response_id` is not used.
- `ResetContext` rebuilds the chatend from retained session content and newly
  loaded kweb nodes, so unloaded node content is genuinely absent afterward.
- Short identifiers never cross the Kweb backend API boundary. The frontend
  resolves them to durable identifiers before making backend calls.
- All Kweb mutations that affect more than one row are SQLite transactions.
- Behavior absent from `UserSpecification.md` is outside the MVP unless it is
  necessary to realize an explicitly specified behavior.

## 3. Runtime Topology

```text
                         Remote LLM provider
                                  ^
                                  |
                                  | HTTPS
                                  |
Browser frontend -------------- Intelligence backend
       |
       | HTTP on localhost
       |
       v
Kweb backend ------------------ SQLite
       |
       +-- serves Frontend/public
       +-- serves Frontend/SystemPrompts
```

Default addresses:

| Component | Address |
| --- | --- |
| Kweb backend | `http://127.0.0.1:4321` |
| Intelligence backend | `http://127.0.0.1:4322` |

The browser calls both services directly. The intelligence backend permits
requests from the Kweb frontend origin. Both services bind to loopback by
default.

## 4. Ownership Boundaries

### 4.1 Frontend

The frontend owns:

- the clean user/Kennedy transcript,
- the current chatend sent to the LLM,
- directly loaded nodes and their expanded active connections,
- durable-ID to short-ID mappings,
- conversation and history-ingress call budgets,
- agent tool definitions and tool loops,
- prompt composition from system-prompt manuals,
- the context inspector and memory explorer state.

The context inspector's JSON body renders the chatend itself. Operational
diagnostics may inform its compact summary but do not wrap or replace the
displayed chatend.

The frontend has no persistent state. A reload or abrupt close may discard an
active conversation.

### 4.2 Kweb Backend

The Kweb backend owns:

- creation and migration of the SQLite schema,
- the hardcoded MVP user and root node,
- knowledge, provenance, and history nodes,
- connection ordering, promotion, and demotion,
- atomic create, update, and connect operations,
- read APIs used by context loading and the memory explorer,
- serving the frontend and prompt-manual files.

It knows nothing about short identifiers, chatends, LLM messages, session call
budgets, or provider APIs.

### 4.3 Intelligence Backend

The intelligence backend owns:

- loading provider credentials and model configuration,
- validating the normalized generation request,
- translating normalized messages and tools to the selected provider,
- translating provider text, tool calls, errors, and usage into one response
  shape.

It executes no tools and stores no LLM session. The frontend continues a tool
loop by appending tool calls and results to its chatend and submitting another
complete generation request.

## 5. Session Model

### 5.1 Conversation

At conversation start, the frontend creates a fresh transcript and chatend,
loads the user root node, and assigns short identifiers to every in-context
node. For each user turn it appends the user message, calls the intelligence
backend, executes allowed tools, and continues until Kennedy returns final
text. Only user and Kennedy text is added to the clean transcript.

The Kweb portion of the chatend accumulates during the conversation. A
`ResetContext` call resolves its arguments, removes all Kweb context, resets
short identifiers, reloads the root and requested nodes, and rebuilds the
chatend while retaining the clean transcript and the current turn's LoadNode
counter.

Ending a conversation creates one data provenance node containing the clean
transcript, then starts a history-ingress session using that provenance node.

### 5.2 History Ingress

History ingress uses a separate chatend composed from the Kmap and
HistoryIngress manuals, the provenance data, and the loaded user root node.
Kennedy may navigate the kweb and create or update knowledge nodes. The current
provenance identifier is held by the frontend and supplied implicitly when it
translates CreateNode and UpdateNode tool calls into Kweb API requests.

The session ends when Kennedy returns final text. The text is not shown as a
chat message. Completing with zero knowledge mutations is valid.

## 6. Kweb Data Model

SQLite stores exactly the three durable node types from the user specification:

- **Knowledge node**: the current human-readable memory and its connection
  lists, with a pointer to the newest history node.
- **Data provenance node**: immutable source material, its source, and its
  source creation time.
- **Data history node**: an append-only link from one knowledge node to one
  provenance node and the previous history node.

Connections are represented in a relational table as an implementation detail
of knowledge nodes. Each directed connection has an `active` or `fanout` tier
and a deterministic recency order. `ConnectNodes` promotes every ordered pair
in the supplied set. If a source exceeds the active limit, its least recently
active connections are demoted. Fanout overflow remains permitted in the MVP.

## 7. API Conventions

Both services expose versioned JSON APIs under `/api/v1` and an unversioned
`GET /health` endpoint.

- Durable identifiers are lowercase hexadecimal encodings of 20 random bytes.
- Timestamps are RFC 3339 UTC strings.
- Requests and responses use `application/json`.
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
BackendKweb/
  Specification.md
Frontend/
  Specification.md
  SystemPrompts/
    SystemPromptKmapAgentManual.txt
    ConversationAgentManual.txt
    HistoryIngressAgentManual.txt
  public/
IntelligenceBackend/
  Specification.md
```

Implementation code belongs under its owning component directory.

## 9. MVP Non-Goals

- Multiple users or access control.
- Network deployment beyond the local machine.
- Durable frontend conversation state.
- Search, deletion, manual knowledge editing, or fanout pruning.
- Self-action sessions.
- Streaming generation.
- Provider-side conversation persistence.
- A frontend build system, Node.js, or TypeScript.
