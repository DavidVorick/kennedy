# Intelligence Backend Specification

## 1. Scope

The intelligence backend is a separate Rust HTTP service that loads provider credentials and runtime settings from `config.yaml` and exposes a provider-agnostic API for sending prompts to remote LLM APIs.

It is a generic LLM bridge. It does not know about application memory systems, domain-specific tools, frontend UI concepts, or product-specific agent names. The frontend supplies prompts, messages, tool definitions, and tool results.

## 2. Responsibilities

- Parse and validate `config.yaml`.
- Manage LLM provider credentials without exposing secrets to the frontend.
- List configured providers and models.
- Create, continue, inspect, and delete LLM sessions.
- Preserve provider-side or backend-side context across turns where supported.
- Return provider tool-call requests to the frontend.
- Accept frontend-supplied tool results and continue generation.
- Provide provider/model metadata, usage, and errors in a stable API shape.

## 3. Configuration File

The service loads `config.yaml` from the working directory by default, or from `--config` / `INTELLIGENCE_CONFIG`.

Example:

```yaml
server:
  host: 127.0.0.1
  port: 4322

providers:
  default: openai
  openai:
    api_key_env: OPENAI_API_KEY
    base_url: https://api.openai.com/v1
    default_model: gpt-5.5
    timeout_seconds: 120

sessions:
  max_active_sessions: 32
  idle_ttl_minutes: 120
  preserve_remote_context: true
  local_transcript_fallback: true

logging:
  level: info
  redact_prompts: false
  redact_api_keys: true
```

### 3.1 Required Semantics

- API keys may be provided directly only for local development, but `api_key_env` is preferred.
- Secrets must never be returned by any API.
- Missing required provider configuration must fail service startup with a clear error.
- Provider names are implementation-defined, but the public API must remain provider-agnostic.

## 4. Context Preservation Design

The intelligence backend must support multi-turn LLM interactions without requiring the frontend to resend the entire prompt and transcript every turn when the selected provider supports remote context preservation.

The backend stores an `LlmSession`:

```json
{
  "session_id": "llm_01j00000000000000000000000",
  "provider": "openai",
  "model": "gpt-5.5",
  "remote_context_id": "provider_response_or_thread_id",
  "local_message_log": [],
  "pending_tool_calls": [],
  "created_at": "2026-07-11T00:00:00Z",
  "last_used_at": "2026-07-11T00:00:00Z"
}
```

The implementation should use provider-native previous-response, thread, cache, or conversation identifiers when available. If a provider lacks native remote context preservation, the backend must maintain a local compact transcript and resend the minimum required context.

Tool results returned to the intelligence backend become part of the preserved model context, allowing later turns to refer to prior tool outputs without the frontend resending those outputs unless the LLM session expires.

## 5. Tool Call Contract

The intelligence backend does not execute tools. It passes model-requested tool calls to the frontend and accepts tool results from the frontend.

Tool definitions are supplied by the frontend when creating a session. Tool-call names, descriptions, schemas, arguments, and result payloads are opaque to the intelligence backend except for validation needed to correlate pending tool-call IDs.

Tool-call shape:

```json
{
  "tool_call_id": "tool_01j00000000000000000000000",
  "name": "ToolName",
  "arguments": {
    "key": "value"
  }
}
```

The backend must preserve pending tool calls until corresponding results arrive.

## 6. API Reference

All endpoints are relative to the intelligence backend base URL.

### 6.1 Health

#### `GET /health`

Response `200`:

```json
{
  "service": "intelligence-backend",
  "status": "ok",
  "provider_default": "openai",
  "version": "0.1.0"
}
```

### 6.2 Providers and Models

#### `GET /api/intelligence/providers`

Returns configured providers and public model metadata. Secrets are never included.

Response `200`:

```json
{
  "default_provider": "openai",
  "providers": [
    {
      "name": "openai",
      "default_model": "gpt-5.5",
      "models": ["gpt-5.5"]
    }
  ]
}
```

### 6.3 Create Session

#### `POST /api/intelligence/sessions`

Creates a provider-backed LLM session.

Request:

```json
{
  "provider": "openai",
  "model": "gpt-5.5",
  "instructions": "System prompt text...",
  "initial_messages": [
    {
      "role": "user",
      "content": "Hello."
    }
  ],
  "tool_definitions": [
    {
      "name": "ToolName",
      "description": "Tool description.",
      "json_schema": {
        "type": "object",
        "properties": {
          "key": {"type": "string"}
        },
        "required": ["key"]
      }
    }
  ]
}
```

Response `201`:

```json
{
  "session_id": "llm_01j00000000000000000000000",
  "provider": "openai",
  "model": "gpt-5.5",
  "remote_context": {
    "preserved": true,
    "strategy": "provider_previous_response_id"
  },
  "created_at": "2026-07-11T00:00:00Z"
}
```

### 6.4 Submit Turn

#### `POST /api/intelligence/sessions/{session_id}/turns`

Sends an input message to the LLM session.

Request:

```json
{
  "input": {
    "role": "user",
    "content": "Continue."
  }
}
```

Response `200` when the model returns final text:

```json
{
  "status": "completed",
  "message": {
    "role": "assistant",
    "content": "Response text."
  },
  "tool_calls": [],
  "usage": {
    "input_tokens": 1000,
    "output_tokens": 250,
    "total_tokens": 1250
  },
  "remote_context": {
    "preserved": true,
    "context_id": "opaque_provider_context_id"
  }
}
```

Response `200` when the model requests tools:

```json
{
  "status": "requires_tool_results",
  "message": null,
  "tool_calls": [
    {
      "tool_call_id": "tool_01j00000000000000000000000",
      "name": "ToolName",
      "arguments": {
        "key": "value"
      }
    }
  ],
  "usage": {
    "input_tokens": 1000,
    "output_tokens": 100,
    "total_tokens": 1100
  },
  "remote_context": {
    "preserved": true,
    "context_id": "opaque_provider_context_id"
  }
}
```

### 6.5 Submit Tool Results

#### `POST /api/intelligence/sessions/{session_id}/tool-results`

Continues generation after frontend-executed tools complete.

Request:

```json
{
  "tool_results": [
    {
      "tool_call_id": "tool_01j00000000000000000000000",
      "name": "ToolName",
      "ok": true,
      "result": {
        "value": "result payload"
      },
      "error": null
    }
  ]
}
```

Response is identical to `POST /turns`: either `completed` or `requires_tool_results`.

Tool failure result example:

```json
{
  "tool_call_id": "tool_01j00000000000000000000000",
  "name": "ToolName",
  "ok": false,
  "result": null,
  "error": {
    "code": "tool_failed",
    "message": "The frontend tool call failed."
  }
}
```

### 6.6 Get Session

#### `GET /api/intelligence/sessions/{session_id}`

Response `200`:

```json
{
  "session_id": "llm_01j00000000000000000000000",
  "provider": "openai",
  "model": "gpt-5.5",
  "created_at": "2026-07-11T00:00:00Z",
  "last_used_at": "2026-07-11T00:00:00Z",
  "pending_tool_call_count": 0,
  "remote_context": {
    "preserved": true,
    "strategy": "provider_previous_response_id"
  }
}
```

### 6.7 Delete Session

#### `DELETE /api/intelligence/sessions/{session_id}`

Deletes local session state and releases provider resources when supported.

Response `204` with no body.

## 7. Error Handling

All errors use the common envelope from `TechnicalDesign.md`.

Additional intelligence-specific error codes:

| Code | Meaning |
| --- | --- |
| `provider_unavailable` | Provider request failed or timed out |
| `provider_auth_failed` | API key rejected |
| `session_expired` | LLM session no longer exists |
| `tool_results_required` | New input submitted while tool calls are pending |
| `unknown_tool_call_id` | Tool result does not match pending call |
| `model_not_configured` | Requested model/provider is unavailable |

## 8. Security

- Never return API keys or provider secrets.
- Redact secrets in logs.
- Bind to localhost by default.
- Do not accept arbitrary remote tool URLs.
- Treat all provider output as untrusted; the frontend validates tool names and arguments.

## 9. Observability

Log per request:

- request ID,
- session ID when present,
- provider/model,
- status,
- latency,
- token usage when available,
- error code when failed.

Prompt and tool-result logging is configurable because inputs may contain private data.
