# Intelligence Backend Specification

## 1. Scope

The intelligence backend is a Rust HTTP bridge between the local browser, a
host launcher for the Podman-sandboxed Codex CLI, OpenAI's paid transcription
API, and public web pages. Model generation and web
research use the user's ChatGPT-authenticated Codex subscription. Audio
transcription is the deliberately separate billed path and reads
the generic vault name in `audio.api_key_secret`; `kennedy-server` resolves and
passes that credential directly to the trusted transcription connector.
Ordinary generation never receives that key and does not call the Responses
HTTP API.

The service stores no Kennedy or Kmap state. It accepts normalized text
messages, resumes opaque Codex thread IDs, returns normalized text and usage,
and safely fetches bounded public pages. It is an independent backend hosted by
`kennedy-server` and neither imports nor calls the Kweb or conversation-history
backends.

## 2. Responsibilities

- Load the Codex sandbox launcher, working directory, allowed models, reasoning effort,
  timeouts, and model-context limits from `config.yaml`.
- Require the configured launcher's successful `login status` response to
  identify ChatGPT login inside the sandbox.
- Remove API-key environment variables from spawned Codex processes so
  generation cannot silently switch to direct API billing.
- Validate normalized, text-only generation requests.
- Start or resume non-interactive Codex threads and normalize the last
  `agent_message`, thread ID, token usage, and failures.
- Execute fresh, ephemeral Codex web-research turns and return deduplicated
  HTTP(S) links found in the grounded answer.
- Fetch one public web page safely and return bounded readable text.
- Publish the selected model transport's input modalities and forward a
  bounded multipart recording to OpenAI `gpt-4o-transcribe` only when that
  transport does not support native audio.
- Keep local paths, authentication material, and raw process output out of
  browser-visible errors and logs.

Kennedy's Kmap and web tools remain ordinary visible text in the Chatend. The
normal generation path gives Codex no shell, file-mutation, app, multi-agent,
or internet tools. Only `/api/v1/web/search` enables Codex web search.

## 3. Configuration

```yaml
credentials:
  vault_path: ./kennedy-secrets.age

server:
  bind: 127.0.0.1:4322
  max_request_bytes: 27262976
  allowed_origins:
    - http://127.0.0.1:4321

web:
  search_context_size: high
  search_reasoning_effort: xhigh
  max_search_sources: 12
  search_timeout_seconds: 600
  fetch_timeout_seconds: 30
  max_fetch_bytes: 2000000
  max_fetch_characters: 50000
  max_redirects: 5

audio:
  api_base: https://api.openai.com/v1/
  api_key_secret: openai-api-key
  transcription_model: gpt-4o-transcribe
  transcription_prompt: "Transcribe faithfully, including discernible relevant non-speech audio."
  timeout_seconds: 120
  max_upload_bytes: 26214400

telegram:
  bot_token_secret: telegram-bot-token

default_provider: primary

providers:
  primary:
    kind: codex
    executable: codex-safe
    working_directory: /tmp
    default_model: gpt-5.6-sol
    models: [gpt-5.6-sol]
    reasoning_effort: xhigh
    context_window_tokens: 1050000
    max_input_tokens: 922000
    native_audio_input_models: []
    timeout_seconds: 600
```

This tracked top-level `config.yaml` contains secret names, not credential
values. `kennedy-server` unlocks the separate encrypted vault before starting
the backends. `codex-safe` is a host executable on Kennedy's `PATH` that forwards all
arguments and piped stdin/stdout to Codex inside a persistent Podman sandbox.
The host does not need a directly installed `codex` binary. The launcher must
runs. For noninteractive Kennedy calls it must use `podman run -i`, not require
a TTY, and preserve stdout for Codex JSONL; it may add `-t` only for an
interactive terminal invocation. The launcher must mount its Codex state
directory so login and thread resumes survive container
runs. `working_directory` must exist. It should contain no project instructions or
user data; `/tmp` is the default. `context_window_tokens` and
`max_input_tokens` are optional overrides. Legacy Responses polling fields are
accepted only for configuration compatibility and are ignored by the active
Codex runtime.

`native_audio_input_models` is empty for the configured `gpt-5.6-sol` Codex
transport. A model belongs in that list only when its active Kennedy transport
can actually forward native audio. The transcription API rejects rather than
transcribes requests for a listed model, ensuring that paid transcription is a
fallback and not an accidental downgrade.

## 4. Codex Execution

Generation asks the `codex-safe` launcher to run `codex exec --json` with:

- the selected model and configured reasoning effort (`gpt-5.6-sol`, `xhigh`);
- saved CLI authentication but ignored user/project configuration and rules;
- approval policy `never` and a read-only sandbox;
- multi-agent, apps, shell, unified-exec, and web search disabled;
- the normalized Chatend serialized through stdin rather than command-line
  arguments;
- a bounded total deadline and child termination on timeout.

The first call starts a persisted Codex thread. Later calls use
`codex exec resume <thread-id>` and contain only newly appended normalized
messages. `ResetContext` is implemented by the frontend: it omits the old
thread ID and sends the rebuilt full Chatend to a new thread.

Codex JSONL may contain intermediate agent messages. Only the last completed
`agent_message` is Kennedy's response. `thread.started` supplies
`response_id`; `turn.completed.usage` supplies input, cached-input, output, and
reasoning tokens. Cache-write tokens are reported as zero because Codex JSONL
does not expose that measurement.

Web search starts a new ephemeral thread with `--search`, retains the same
read-only/no-shell restrictions, and asks Codex for a concise evidence-focused
answer with direct links. It never joins Kennedy's conversation thread.

## 5. API

### 5.1 Health and Providers

`GET /health` reports local service availability, including whether paid audio
transcription is configured. Startup has already verified the configured
ChatGPT login. A missing transcription key does not disable text generation.

`GET /api/v1/providers` returns provider name, `kind: codex`, default and
allowed models, configured `reasoning_effort`, and context limits. The browser
uses the model plus reasoning effort for runtime prompt disclosure and automatic
Kmap mutation attribution. It also returns per-model `input_modalities` and
whether transcription is available. The endpoint never returns authentication
details.

### 5.2 Audio Transcription

`POST /api/v1/audio/transcriptions` accepts multipart fields `provider`,
`model`, and one `file`. The recording must use a supported audio/container
media type and remain within `audio.max_upload_bytes`. The service first checks
the selected model capability. If native audio is supported it returns
`native_audio_supported` without calling transcription; otherwise it forwards
the bytes to OpenAI's `/v1/audio/transcriptions` endpoint with
`gpt-4o-transcribe` and returns the paid transcript, transcription model, input
model, and any reported usage. A configured prompt asks transcription to retain
concise annotations for discernible relevant non-speech sounds, speaker changes,
tone, pauses, music, and background audio. Audio content and API keys are never logged.

### 5.3 Generate

`POST /api/v1/generate` accepts:

```json
{
  "provider": "primary",
  "model": "gpt-5.6-sol",
  "messages": [{"role": "user", "content": "New text for this round."}],
  "previous_response_id": "019f5ca7-020f-7b63-be2f-82785fb68c03",
  "prompt_cache_key": "kennedy-conversation-prompt-v1"
}
```

The first request omits `previous_response_id` and sends the complete Chatend.
Continuation IDs must be Codex UUID thread IDs. `prompt_cache_key` remains in
the stable browser/backend contract but Codex manages caching per thread, so
the bridge does not pass the key to the CLI.

Successful responses contain:

```json
{
  "status": "complete",
  "response_id": "019f5ca7-020f-7b63-be2f-82785fb68c03",
  "message": {"role": "assistant", "content": "Readable model output."},
  "usage": {
    "input_tokens": 1000,
    "output_tokens": 80,
    "cached_tokens": 768,
    "cache_write_tokens": 0,
    "reasoning_tokens": 30
  }
}
```

The bridge does not interpret whether assistant text is a final answer or a
Kennedy text-tool request.

### 5.4 Web Search

`POST /api/v1/web/search` accepts provider/model plus a natural-language
`question` of 1–4000 characters. It returns `answer`, deduplicated `sources`,
provider/model, and optional usage. A valid answer may have no public source
URL for a live-data lookup; ordinary cited research should include links.

### 5.5 Web Fetch

`POST /api/v1/web/fetch` accepts one absolute public HTTP(S) URL. It resolves
and pins public addresses, rejects credentials, localhost/private/reserved IPs,
nonstandard ports, excessive redirects, unsupported media, oversized bodies,
and unsafe redirect destinations. It returns the final URL, optional title,
content type, bounded readable text, retrieval time, and truncation flag.

## 6. Errors, Logging, and HTTP

Invalid inputs return `400 invalid_request`. Unknown providers/models return
`400 provider_not_configured`. ChatGPT login failures use
`401 provider_auth_failed`; subscription/rate limits use
`429 provider_rate_limited`; Codex process and protocol failures use sanitized
5xx errors; deadlines use `504 provider_timeout`. Errors may include a local
request UUID but never credentials or full prompts.

The service binds to loopback by default, accepts JSON plus bounded multipart
audio, exposes explicit GET and POST routes, permits only configured frontend origins, and applies a
request-body limit before deserialization.

## 7. Verification

Tests cover request and thread-ID validation, Codex JSONL event normalization,
last-message selection, usage mapping, source extraction/deduplication, config
defaults, model modality metadata, paid-transcription configuration, safe audio
filenames, public-URL safety, and readable-content bounds. A smoke test should
confirm fresh generation, thread resume, and web search with the machine's
actual ChatGPT login.
