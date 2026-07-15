# Intelligence Backend Specification

## 1. Scope

The intelligence backend is a Rust HTTP bridge between the local browser, a
host launcher for the Podman-sandboxed Codex CLI, OpenAI's paid transcription
API, and public web pages. Model generation and web
research use the user's ChatGPT-authenticated Codex subscription. Audio
transcription is the deliberately separate billed path and reads
the conventional vault name `openai-api-key`; `kennedy-server` resolves and
passes that credential directly to the trusted transcription connector.
Ordinary generation never receives that key and does not call the Responses
HTTP API.

The service stores no Kennedy or Kmap state. It accepts normalized text
messages, resumes opaque Codex thread IDs, returns normalized text and usage,
and safely fetches bounded public pages. It is an independent backend hosted by
`kennedy-server` and neither imports nor calls the Kweb or conversation-history
backends.

## 2. Responsibilities

- Apply compiled defaults for the Codex sandbox launcher, working directory,
  allowed models, reasoning effort, timeouts, model-context limits, audio, and
  web safety bounds.
- Require the launcher's successful `login status` response to
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
or internet tools. Only `/api/v1/web/search` invokes a web-enabled provider.

## 3. Compiled Defaults and Deployment Options

The backend has no runtime configuration file. Stable defaults live in
`IntelligenceBackend/src/defaults.rs`: provider `primary`, launcher
`codex-safe`, temporary working directory, model `gpt-5.6-sol`, ordinary
generation at `xhigh`, a 600-second generation deadline, known model context
limits, bounded public-page fetches, and paid `gpt-4o-transcribe` behavior.
Changing these policies is a code change reviewed and tested with the rest of
the adapter.

The three WebSearch modes are also compiled policy:

| Mode | Provider and model | Reasoning | Search context | Deadline | Source cap |
| --- | --- | --- | --- | --- | --- |
| `quality` | Codex `gpt-5.6-sol` | `xhigh` | `high` | 600 seconds | 12 |
| `balanced` | Codex `gpt-5.6-terra` | `low` | `low` | 90 seconds | 8 |
| `fast` | Gemini `gemini-3.1-flash-lite` | `low` | Google Search grounding | 45 seconds | 6 |

`balanced` is the request default and Kennedy's recommended ordinary choice.
`quality` performs thorough research for hard questions. `fast` trades research
quality for latency, caps output at 2048 tokens, uses Priority service, and
sets Gemini thinking to `low` rather than `minimal` so it can synthesize the
retrieved evidence while page retrieval remains the likely dominant cost.

The host supplies the intelligence listener, allowed frontend origins, and
optional secret values when starting the library. `kennedy-server` exposes
listener addresses, source and data paths, and the encrypted vault path as
deployment CLI options. It resolves `gemini-api-key` for fast search and
`openai-api-key` for transcription without exposing either to the browser. The
request-body limit and connector safety bounds remain compiled invariants.

`codex-safe` is a host executable on Kennedy's `PATH` that forwards all
arguments and piped stdin/stdout to Codex inside a persistent Podman sandbox.
The host does not need a directly installed `codex` binary. For noninteractive
Kennedy calls the launcher must use `podman run -i`, not require a TTY, and
preserve stdout for Codex JSONL; it may add `-t` only for an interactive
terminal invocation. It must mount its Codex state directory so login and
thread resumes survive runs. The temporary working directory must contain no
project instructions or user data.

The native-audio model list is empty for the `gpt-5.6-sol` Codex transport. A
model belongs in that list only when its active Kennedy transport can actually
forward native audio. The transcription API rejects rather than transcribes
requests for a listed model, ensuring that paid transcription is a fallback
and not an accidental downgrade.

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

`quality` and `balanced` web search start a new ephemeral Codex thread with
`--search`, retain the same read-only/no-shell restrictions, and ask Codex for
an answer with direct links. The mode selects the fixed model, reasoning
effort, Codex web-search context size, deadline, source cap, and focused or
thorough research prompt.

`fast` sends a stateless request to Gemini's Interactions API with model
`gemini-3.1-flash-lite`, the built-in `google_search` tool, `low` thinking,
Priority service, `store: false`, and a 2048-output-token cap. The bridge reads
answer text from `model_output` steps, citations from `url_citation`
annotations, fallback sources from `google_search_result` steps, and token
usage from the interaction usage object. It records the effective service-tier
response header for operational logs. No search mode joins Kennedy's
conversation continuation thread.

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

`POST /api/v1/web/search` accepts provider/model, a natural-language `question`
of 1–4000 characters, and `mode: "quality" | "balanced" | "fast"`. An omitted
mode defaults to `balanced`, while Kennedy's frontend tool contract requires
her to choose explicitly. The requested active provider/model is validated;
the selected mode then fixes the actual search provider and model. The response
reports that actual provider/model together with `answer`, deduplicated
`sources`, selected mode, and optional normalized usage. A valid answer may
have no public source URL for a live-data lookup; ordinary cited research
should include links.

### 5.5 Web Fetch

`POST /api/v1/web/fetch` accepts one absolute public HTTP(S) URL. It resolves
and pins public addresses, rejects credentials, localhost/private/reserved IPs,
nonstandard ports, excessive redirects, unsupported media, oversized bodies,
and unsafe redirect destinations. It returns the final URL, optional title,
content type, bounded readable text, retrieval time, and truncation flag.

## 6. Errors, Logging, and HTTP

Invalid inputs return `400 invalid_request`. Unknown providers/models and a
missing Gemini fast-search secret return `provider_not_configured`. Provider
authentication failures use `401 provider_auth_failed`; subscription, quota,
and rate limits use `429 provider_rate_limited`; provider transport and
protocol failures use sanitized 5xx errors; deadlines use
`504 provider_timeout`. Errors may include a local request UUID but never
credentials or full prompts.

The service binds to the address supplied by `kennedy-server` (loopback by
default), accepts JSON plus bounded multipart audio, exposes explicit GET and
POST routes, permits only the supplied frontend origins, and applies a
request-body limit before deserialization.

## 7. Verification

Tests cover request and thread-ID validation, Codex JSONL event normalization,
last-message selection, usage mapping, source extraction/deduplication,
compiled defaults, all three search profiles, Gemini request and interaction
normalization, model modality metadata, paid-transcription behavior, safe audio
filenames, public-URL safety, and readable-content bounds. Smoke tests should
confirm fresh generation, thread resume, Codex search with the machine's actual
ChatGPT login, and fast search with a configured Gemini key.
