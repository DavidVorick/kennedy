# Kennedy Kweb Technical Design

## 1. Purpose

Kennedy is a local-first memory application built around a directed knowledge web, or kweb. Long-term memory is stored as immutable provenance records, append-only history records, and mutable knowledge nodes in SQLite. A browser UI served from localhost lets the user chat with Kennedy, inspect the prompt/context assembled by the frontend, and explore memory.

The system has three runtime components plus frontend-owned prompt assets:

1. **Kweb Rust backend**: owns SQLite persistence, kweb invariants, and APIs for reading and updating kweb nodes.
2. **JS/HTML/CSS frontend**: pure static web app served over localhost; owns UI state, conversation transcripts, context glue, prompt composition, and orchestration.
3. **Intelligence backend**: separate Rust service that loads `config.yaml` and exposes a generic API for sending prompts to remote LLM APIs.
4. **Agent manuals**: frontend-loaded system-prompt text snippets that explain available kmap and session tools to the agent.

No implementation code is defined in this document. This file defines the technical architecture and the APIs made available to the frontend by the backend services.

## 2. Component Boundaries

### 2.1 Kweb Backend

The kweb backend owns durable memory and graph update rules only. It does not manage frontend sessions, conversation transcripts, context windows, short identifiers, or prompt composition.

The kweb backend provides APIs for the frontend to:

- bootstrap and fetch the hardcoded user root node,
- fetch knowledge nodes and their connections,
- fetch node history and provenance records,
- create data provenance records,
- create and update knowledge nodes using a provenance record,
- connect groups of knowledge nodes according to kweb active/fanout rules.

### 2.2 Frontend

The frontend owns all product/session orchestration. It manages:

- chat transcript state,
- context glue and any identifier mapping shown to the agent,
- loaded-memory context,
- tool-call loops between the agent and the kweb backend,
- prompt composition from agent-manual text files,
- memory explorer navigation state.

The frontend must not reimplement durable kweb invariants. It calls the kweb backend to perform graph mutations.

### 2.3 Intelligence Backend

The intelligence backend is a generic local bridge to remote LLM APIs. It has no kweb-specific responsibilities. It provides APIs for the frontend to:

- inspect service health,
- list configured providers/models,
- create an LLM conversation/session,
- send input messages or prompt payloads,
- continue generation after tool results,
- delete an LLM session.

### 2.4 Agent Manuals

Agent manuals are static text files loaded by the frontend and inserted into system prompts as-is. The kmap manual explains the kmap data structure and common kmap tool mechanics. Session manuals explain which tools exist for a particular session type.

## 3. Runtime Topology

```text
Browser frontend
  | HTTP localhost
  v
Kweb Rust Backend ------------------ SQLite database
  |
  | HTTP localhost
  v
Intelligence Rust Backend ---------- config.yaml ---------- Remote LLM APIs
```

The kweb backend serves the frontend assets. The frontend calls both local services directly.

## 4. Default Local APIs

Default local endpoints are configurable, but v1 defaults are:

| Component | Default URL | Frontend-facing API responsibility |
| --- | --- | --- |
| Kweb backend | `http://127.0.0.1:4321` | Static frontend, node/provenance/history APIs, kweb mutation APIs |
| Intelligence backend | `http://127.0.0.1:4322` | Generic remote-LLM session and generation APIs |

Both services must expose `GET /health` returning `200 OK` and JSON health metadata.

## 5. Shared API Conventions

All APIs use JSON over HTTP.

### 5.1 Common Headers

Requests:

```http
Content-Type: application/json
Accept: application/json
```

Responses:

```http
Content-Type: application/json; charset=utf-8
```

### 5.2 IDs

- Durable kweb IDs are lowercase hex strings representing 20 random bytes: `^[0-9a-f]{40}$`.
- Intelligence backend IDs are opaque strings.
- Any short or display identifiers used inside prompts are frontend-owned and are not part of the kweb backend contract.

### 5.3 Timestamps

All timestamps use RFC 3339 UTC strings, for example `2026-07-11T00:00:00Z`.

### 5.4 Error Envelope

All non-2xx JSON errors use:

```json
{
  "error": {
    "code": "string_machine_code",
    "message": "Human readable message.",
    "details": {}
  }
}
```

Common status codes:

| Status | Meaning |
| --- | --- |
| `400` | Request validation failed |
| `404` | Resource not found |
| `409` | State conflict or invariant violation |
| `422` | Semantically valid JSON but invalid operation |
| `500` | Internal server error |
| `503` | Dependency unavailable |

## 6. Repository Specification Layout

```text
UserSpecification.md
TechnicalDesign.md
INSTRUCTIONS.md
BackendKweb/Specification.md
Frontend/Specification.md
IntelligenceBackend/Specification.md
Frontend/SystemPrompts/SystemPromptKmapAgentManual.txt
Frontend/SystemPrompts/ConversationAgentManual.txt
Frontend/SystemPrompts/HistoryIngressAgentManual.txt
```

Future implementation work should place code under component-appropriate directories while keeping these specification files current.

## 7. Non-Goals for V1

- Multi-user authentication or access control.
- Network deployment beyond localhost.
- NodeJS, TypeScript, or frontend build tooling.
- User editing of memories through the UI beyond exploration unless later specified.
- Full fanout overflow enforcement.
- Self-action implementation.
