# Intelligence Backend Specification

## 1. Scope

The intelligence backend is a generic Rust HTTP bridge between local browser
applications and remote LLM providers. It accepts a complete normalized LLM
request, sends an equivalent request to a configured provider, and normalizes
the provider response.

It is stateless between requests. It does not store conversations, execute
tools, compose prompts, allocate short identifiers, or know anything about
Kennedy or the kweb.

## 2. Responsibilities

- Load provider credentials and model defaults from `config.yaml`.
- Keep credentials out of browser-visible responses.
- Expose configured provider and model names.
- Validate normalized messages and tool definitions.
- Translate generation requests into provider-specific requests.
- Normalize final text, tool calls, token usage, and provider failures.
- Allow the configured Kweb frontend origin to call the API.

## 3. Configuration

The service loads `./config.yaml` by default. `--config PATH` selects another
file.

```yaml
server:
  bind: 127.0.0.1:4322
  max_request_bytes: 10485760
  allowed_origins:
    - http://127.0.0.1:4321

default_provider: primary

providers:
  primary:
    kind: openai
    api_key_env: OPENAI_API_KEY
    base_url: https://api.openai.com/v1
    default_model: configured-model-name
    models:
      - configured-model-name
    timeout_seconds: 120
```

Provider entry names such as `primary` are public API identifiers. `kind`
selects the internal adapter. API keys are read from the named environment
variable. Startup fails if the default provider, its default model, or its
credential is missing.

The service may initially implement one provider adapter. Adding another
adapter must not change the public request and response shapes.

## 4. Normalized Message Model

The request `messages` array is ordered and supports these shapes.

### 4.1 Text Message

```json
{
  "role": "system",
  "content": "Instructions or context."
}
```

`role` is `system`, `user`, or `assistant`.

### 4.2 Assistant Tool-Call Message

```json
{
  "role": "assistant",
  "content": null,
  "tool_calls": [
    {
      "id": "call_opaque_id",
      "name": "LoadNode",
      "arguments": {
        "identifier": 3
      }
    }
  ]
}
```

An assistant message may contain text, tool calls, or both.

### 4.3 Tool-Result Message

```json
{
  "role": "tool",
  "tool_call_id": "call_opaque_id",
  "name": "LoadNode",
  "content": {
    "ok": true,
    "result": {}
  }
}
```

`content` may be any JSON value. The adapter serializes it without changing its
meaning. Every tool-result ID must match an earlier assistant tool call in the
same request.

## 5. Tool Definitions

Tools use a provider-neutral JSON Schema subset:

```json
{
  "name": "LoadNode",
  "description": "Load a Kweb node into context.",
  "input_schema": {
    "type": "object",
    "properties": {
      "identifier": {"type": "integer"}
    },
    "required": ["identifier"],
    "additionalProperties": false
  }
}
```

The intelligence backend validates the definition structure but does not
interpret tool names or application semantics.

The portable schema subset supports `type`, `properties`, `required`,
`additionalProperties`, `items`, and the scalar types `string`, `integer`,
`number`, and `boolean`, plus nested `object` and `array` schemas. The five
Kennedy tools must be expressible using this subset.

## 6. API

### 6.1 Health

#### `GET /health`

```json
{
  "service": "intelligence",
  "status": "ok"
}
```

Health checks local configuration only; they do not make a paid provider
request.

### 6.2 Providers

#### `GET /api/v1/providers`

```json
{
  "default_provider": "primary",
  "providers": [
    {
      "name": "primary",
      "default_model": "configured-model-name",
      "models": ["configured-model-name"]
    }
  ]
}
```

Credentials, environment-variable names, base URLs, and other private
configuration are omitted.

### 6.3 Generate

#### `POST /api/v1/generate`

Request:

```json
{
  "provider": "primary",
  "model": "configured-model-name",
  "messages": [
    {
      "role": "system",
      "content": "System prompt."
    },
    {
      "role": "user",
      "content": "Hello."
    }
  ],
  "tools": []
}
```

`provider` and `model` may be omitted to use configured defaults. `messages`
must be non-empty. `tools` may be omitted when no tools are available.

Final-text response:

```json
{
  "status": "complete",
  "message": {
    "role": "assistant",
    "content": "Hello.",
    "tool_calls": []
  },
  "usage": {
    "input_tokens": 100,
    "output_tokens": 20
  }
}
```

Tool-call response:

```json
{
  "status": "tool_calls",
  "message": {
    "role": "assistant",
    "content": null,
    "tool_calls": [
      {
        "id": "call_opaque_id",
        "name": "LoadNode",
        "arguments": {
          "identifier": 3
        }
      }
    ]
  },
  "usage": {
    "input_tokens": 100,
    "output_tokens": 20
  }
}
```

The backend returns the assistant message exactly once. The frontend appends
that message and any tool results to its chatend, then sends another complete
generation request. `usage` is null when the provider does not report it.

## 7. Validation

Reject with `400` when:

- the provider or model is not configured,
- messages are empty or use an unknown role,
- required message fields are absent,
- tool names are duplicated,
- a tool definition is not a supported object JSON Schema,
- assistant tool-call IDs are duplicated,
- a tool-result message does not match an earlier tool call,
- more than one result is supplied for the same tool call.

The backend does not validate application-specific tool arguments against the
tool schema after the provider returns them; the frontend performs that check
before execution. A provider response whose tool arguments cannot be normalized
to a JSON object is a `502 provider_error`.

## 8. Provider Adapter Contract

Every adapter implements the same internal operation:

```text
generate(provider_config, model, messages, tools)
    -> assistant message + optional usage
```

An adapter must:

- preserve message order and roles,
- preserve assistant/tool-call/result correlations,
- send all supplied system content with system-level priority supported by the
  provider,
- require tool arguments to decode as a JSON object,
- generate opaque unique call IDs when the provider omits them,
- preserve provider call IDs when available,
- return all tool calls from one assistant response,
- avoid using provider-side previous-response or conversation identifiers.

Provider-specific response IDs may be discarded. The complete request from the
frontend is always authoritative.

## 9. Errors

Errors use the shared envelope from `TechnicalDesign.md`.

| Status | Code | Meaning |
| --- | --- | --- |
| `400` | `invalid_request` | Normalized request validation failed |
| `400` | `provider_not_configured` | Unknown provider or model |
| `401` | `provider_auth_failed` | Remote provider rejected credentials |
| `429` | `provider_rate_limited` | Remote provider rate limit |
| `502` | `provider_error` | Remote provider returned an unusable response |
| `503` | `provider_unavailable` | Provider could not be reached |
| `504` | `provider_timeout` | Configured timeout elapsed |
| `500` | `internal_error` | Unexpected local failure |

Provider error bodies are not forwarded verbatim. The backend returns a stable
message and logs provider diagnostics without credentials.

## 10. HTTP Requirements

- Bind to loopback by default.
- Accept JSON request bodies only.
- Set `Access-Control-Allow-Origin` only for an exact configured origin.
- Handle CORS preflight for `POST /api/v1/generate`.
- Enforce the configured request-body limit, which defaults to 10 MiB, and
  reject larger requests with `413`.
- Apply the configured provider timeout to every remote request.
- Do not retry generation automatically; a retry could duplicate paid output
  or produce a different tool decision.

## 11. Logging

Log:

- request identifier,
- provider and model,
- response status,
- latency,
- token usage when available,
- normalized error code.

Do not log API keys, complete prompts, tool-result contents, or provider
authorization headers.

## 12. Verification

At minimum include:

- request-shape validation tests,
- tool-call and tool-result correlation tests,
- provider-response normalization tests,
- final-text and multiple-tool-call fixtures,
- provider timeout and error mapping tests,
- CORS tests for allowed and disallowed origins,
- an adapter test proving that a complete second request does not depend on
  state retained from the first.
