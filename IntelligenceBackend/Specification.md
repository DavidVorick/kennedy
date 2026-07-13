# Intelligence Backend Specification

## 1. Scope

The intelligence backend is a Rust HTTP bridge between the local browser,
public web pages, and the OpenAI Responses API. It accepts normalized text
messages plus continuation and prompt-cache controls, supports bounded hosted
web research and safe page retrieval, and normalizes results and errors.

The service keeps no local conversation state and does not understand the kmap.
Provider-side response state is addressed by an opaque `previous_response_id`
supplied and retained by the frontend. Web research runs are independent,
stateless provider requests and never join Kennedy's main response chain.

It is an independent library backend hosted by the `kennedy-server` binary.
It has no dependency on and receives no state or handles from the Kweb or
conversation history backends.

## 2. Responsibilities

- Load provider credentials, model defaults, reasoning effort, and optional
  context-window overrides from `config.yaml`.
- Keep credentials out of browser-visible responses and logs.
- Expose configured provider/model names and model context limits.
- Validate text-only normalized generation requests.
- Translate continuation and cache keys into OpenAI Responses requests.
- Normalize response text, response IDs, cache/read/write usage, reasoning
  tokens, and provider failures.
- Execute provider-backed web research questions and return an answer with
  deduplicated source URLs.
- Fetch one public web page safely and return bounded readable text.
- Allow the configured frontend origin to call the API.

The normal generation endpoint deliberately sends no provider tools. Kennedy's
tool protocol remains ordinary assistant and user text in the chatend. Only the
separate web-research endpoint uses the provider's hosted `web_search` tool.

## 3. Configuration

```yaml
server:
  bind: 127.0.0.1:4322
  max_request_bytes: 10485760
  allowed_origins:
    - http://127.0.0.1:4321

web:
  search_context_size: high
  search_reasoning_effort: high
  max_search_sources: 12
  search_timeout_seconds: 600
  search_poll_interval_milliseconds: 1000
  fetch_timeout_seconds: 30
  max_fetch_bytes: 2000000
  max_fetch_characters: 50000
  max_redirects: 5

default_provider: primary

providers:
  primary:
    kind: openai
    api_key: "replace-with-your-openai-api-key"
    base_url: https://api.openai.com/v1
    default_model: gpt-5.6-sol
    models:
      - gpt-5.6-sol
    reasoning_effort: xhigh
    context_window_tokens: 1050000
    max_input_tokens: 922000
    timeout_seconds: 120
    response_timeout_seconds: 600
    response_poll_interval_milliseconds: 1000
```

`context_window_tokens` and `max_input_tokens` are optional overrides. The
adapter knows the current limits for `gpt-5.6-sol`; an unknown model reports
zero unless its limits are configured explicitly. Web settings are
intelligence-layer policy and are not exposed as Kennedy tool arguments.

## 4. Normalized Message Model

Every message is readable text:

```json
{
  "role": "assistant",
  "content": "KENNEDY_TOOL_CALLS\n{\"calls\":[{\"name\":\"LoadNode\",\"arguments\":{\"identifier\":3}}]}"
}
```

`role` is `system`, `user`, or `assistant`; `content` is a non-empty string.
There are no normalized function-call, function-result, call-ID, provider-item,
or tool-definition structures.

## 5. API

### 5.1 Health

`GET /health` checks local configuration only and makes no paid request.

### 5.2 Providers

`GET /api/v1/providers` returns public model metadata:

```json
{
  "default_provider": "primary",
  "providers": [{
    "name": "primary",
    "default_model": "gpt-5.6-sol",
    "models": ["gpt-5.6-sol"],
    "context_window_tokens": 1050000,
    "max_input_tokens": 922000
  }]
}
```

### 5.3 Generate

`POST /api/v1/generate` accepts:

```json
{
  "provider": "primary",
  "model": "gpt-5.6-sol",
  "messages": [{"role": "user", "content": "New text for this round."}],
  "previous_response_id": "resp_previous",
  "prompt_cache_key": "kennedy-conversation-stable-key"
}
```

For the first request in a provider chain, `messages` is the complete chatend
and `previous_response_id` is omitted. Continuations contain only text appended
after the referenced provider response. Conversation and ingress use separate
deterministic `prompt_cache_key` values that are reused across sessions,
including across user-requested context resets.

Successful responses contain:

```json
{
  "status": "complete",
  "response_id": "resp_current",
  "message": {"role": "assistant", "content": "Readable model output."},
  "usage": {
    "input_tokens": 1000,
    "output_tokens": 80,
    "cached_tokens": 768,
    "cache_write_tokens": 128,
    "reasoning_tokens": 30
  }
}
```

`usage` is null if the provider omits usage data. The backend does not interpret
whether assistant text is a final answer or a Kennedy tool request.

### 5.4 Web Search

`POST /api/v1/web/search` accepts only provider/model selection plus a natural
language research question:

```json
{
  "provider": "primary",
  "model": "gpt-5.6-sol",
  "question": "What are the strongest current brunch recommendations in El Salvador, and what evidence supports them?"
}
```

The endpoint starts a fresh Responses request with hosted web search required.
The intelligence layer chooses search context, reasoning effort, query count,
languages, filters, source count, and page-opening behavior. It returns the
provider's evidence-focused answer, deduplicated HTTP(S) source records, public
provider/model names, and optional usage. Cited sources are retained first and
additional consulted sources are capped by `max_search_sources`. It fails if
the provider supplies no answer or no source URLs.

### 5.5 Web Fetch

`POST /api/v1/web/fetch` accepts exactly one absolute public HTTP(S) URL and
returns its final URL, title when found, media type, retrieval timestamp,
bounded readable content, and a truncation flag. It supports HTML, XHTML,
plain-text types, and JSON. It does not execute JavaScript or act as an
authenticated browser.

## 6. OpenAI Adapter

The adapter uses `POST /v1/responses` with:

- `background: true`, so long-running reasoning is not tied to one provider
  HTTP connection;
- `store: true`, so `previous_response_id` can continue the chain;
- `reasoning.effort` from configuration;
- `reasoning.context: all_turns`;
- a stable `prompt_cache_key` supplied by the frontend;
- `prompt_cache_options.mode: implicit` and a `30m` minimum TTL whenever a
  cache key is supplied;
- no `tools`, `tool_choice`, function-call outputs, custom tools, or encrypted
  manual-replay items.

Implicit mode places a cache breakpoint on the latest message. This favors
append-only conversation and tool loops: a later request can read the previous
prefix and write the newly extended prefix. Cache writes and reads are both
reported so the UI can show the actual behavior.

After background creation, the adapter polls `GET /v1/responses/{id}` while
the response is `queued` or `in_progress`. Polling is bounded by
`response_timeout_seconds` and paced by
`response_poll_interval_milliseconds`. A transient transport failure or HTTP
408, 409, 429, 500, 502, 503, or 504 during retrieval is retried up to three
consecutive times because the stored provider job remains addressable. Failed,
cancelled, incomplete, and unknown terminal states become actionable errors;
only a completed response is normalized into Kennedy's Chatend.

`ResetContext` is implemented by the frontend. It stops supplying the old
`previous_response_id` and sends the rebuilt full chatend, which guarantees
removed Kmap content is absent from the new provider chain. No automatic reset
or compaction is performed.

The web-search endpoint uses the same background-response runner, bounded by
`search_timeout_seconds`. Search requests have no continuation or cache key
and use a required hosted
`web_search` tool, live access, and the configured search context and reasoning
effort. Web search output is normalized before it is returned to Kennedy's
visible text-tool loop.

## 7. Validation

Reject with `400 invalid_request` when:

- messages are empty;
- a role is not `system`, `user`, or `assistant`;
- message content is empty;
- `previous_response_id` is present but empty;
- `prompt_cache_key` is empty or exceeds 64 bytes;
- the requested provider or model is not configured;
- a web question or URL is empty or exceeds its length limit;
- a fetch URL is not absolute HTTP(S), embeds credentials, uses a nonstandard
  port, or names an obviously non-public destination.

A provider response without an ID, output array, or non-empty assistant text is
returned as `502 provider_error`.

## 8. Errors and Logging

Provider errors use the shared error envelope with a local request ID.
Credential messages remain generic; bounded provider validation details may be
included when safe.

Logs include request ID, provider, model, latency, status, and reported token
usage. They never include API keys, prompts, Kennedy tool text, response IDs, or
authorization headers.

The service does not retry generation automatically because a retry can
duplicate paid output or produce a different model decision.

WebFetch disables automatic redirects and validates every redirect itself. It
resolves and pins the destination, rejects private, loopback, link-local,
reserved, benchmark, multicast, and documentation address ranges, caps
redirects and downloaded bytes, and never returns binary content. Page content
is untrusted evidence and no instructions in it are executed by the backend.

## 9. HTTP Requirements

- Bind to loopback by default.
- Accept JSON bodies only and enforce the configured size limit.
- Permit only configured exact CORS origins.
- Apply the configured timeout to every remote request.

## 10. Verification

Tests cover message and cache-key validation, stateful request translation,
absence of provider tools on normal generation, hosted-search request shape,
search answer/source normalization, fetch URL safety and output bounds,
cache/read/write normalization, model limits, provider error mapping, and
example-config validation.
