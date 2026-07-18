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

The service stores no Kennedy or Kmap state. It accepts one canonical plaintext
Chatend (or continuation suffix), resumes opaque Codex thread IDs, returns
normalized text and usage, and safely fetches bounded public pages. It is an
independent backend hosted by `kennedy-server` and neither imports nor calls
the Kweb or conversation-history backends.

## 2. Responsibilities

- Apply compiled defaults for the Codex sandbox launcher, working directory,
  allowed models, reasoning effort, timeouts, audio, and web safety bounds.
- Discover configured models' advertised effective context windows from Codex
  at startup and fail closed rather than substitute local context estimates.
- Require the launcher's successful `login status` response to
  identify ChatGPT login inside the sandbox.
- Remove API-key environment variables from spawned Codex processes so
  generation cannot silently switch to direct API billing.
- Validate canonical plaintext generation requests.
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
generation at `xhigh`, a 600-second generation deadline, bounded public-page
fetches, and paid `gpt-4o-transcribe` behavior.
Changing these policies is a code change reviewed and tested with the rest of
the adapter.

The three WebSearch modes are also compiled policy:

| Mode | Provider and model | Reasoning | Search context | Deadline | Source cap |
| --- | --- | --- | --- | --- | --- |
| `quality` | Codex `gpt-5.6-sol` | `xhigh` | `high` | 1,800 seconds | 12 |
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
project instructions or user data. It must also bind the backend-created
`${TMPDIR:-/tmp}/kennedy-codex-catalogs` directory into the container at the
same absolute path, read-only. A persistent container must be recreated when
adding this mount. The backend creates the host directory before its first
launcher call.

At startup the backend and AudioIngress concurrently request one shared catalog
from `kennedy-codex-runtime`. The runtime always checks `codex-safe --version`.
It runs `codex-safe debug models` and sanitizes/probes the result only on a
versioned cache miss; concurrent callers share the same initialization. It
reads each model's `context_window` and `effective_context_window_percent`, and
exposes their product as the usable context and input window. The selected
model must be present with valid advertised values or startup fails. There is
no hardcoded fallback window. The sanitized catalog blanks every model's
`base_instructions`, removes `model_messages`, disables model-selected skill
instructions, and removes `tool_mode`, `multi_agent_version`, and
`apply_patch_tool_type`. Kennedy never falls back to the stock
instruction-bearing catalog.

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
- empty Codex runtime, developer, and model-base instructions, with personality, project
  documents, skills, permission, app, collaboration, and environment
  instruction blocks suppressed;
- optional multi-agent, app, shell, unified-exec, code-mode, goal, hook, plugin,
  browser/computer, image-generation, elicitation, and related tool scaffolding
  disabled, including the separately configured experimental
  `request_user_input` tool, along with web search on ordinary Kennedy turns;
- the mandatory verified sanitized live model catalog, removing provider prompt
  templates and model-selected agent tools without altering advertised context
  limits;
- the canonical plaintext Chatend passed unchanged through stdin rather than
  command-line arguments or a JSON prompt envelope;
- `model_auto_compact_token_limit` set to the largest signed 64-bit value,
  beyond every reachable advertised context window, so Codex does not
  automatically compact Kennedy's Chatend;
- a bounded total deadline and child termination on timeout.

Generation and web-search requests may additionally supply a positive
`timeout_seconds`. The backend clamps it to the configured provider/search
profile timeout rather than allowing a caller to lengthen work. Free time uses
this field to bound each operation to its persisted deadline plus the
two-minute graceful-shutdown window.

History and audio ingress use the normal provider generation timeout; their
short retry delay does not shorten a Codex turn. Oversized Chatends are rejected
locally with `input_too_large` before starting Codex, and known launcher
warnings are excluded from provider failure details.

These settings minimize every exposed Codex layer the deployment can control.
The canonical Chatend is the exact application-controlled plaintext sent to
Codex. `codex-safe debug prompt-input` must report exactly one model-visible
message containing an application sentinel; any additional or altered prompt
item aborts startup. A successful result is cached for the exact Codex/catalog,
model, reasoning, and prompt-configuration version rather than repeated on
every launch. Codex or its upstream provider may still attach forced structured
metadata downstream. Codex still registers its
unconditional `update_plan` and environment-backed `view_image` schemas even
when every exposed switch for them is false; transport testing confirms that no
supported setting removes those final core schemas. Kennedy adds no invisible
instruction concerning them.

The first call starts a persisted Codex thread. Later calls use
`codex exec resume <thread-id>` and contain only newly appended normalized
messages. `ResetContext` is implemented by the frontend: it omits the old
thread ID and sends the rebuilt full Chatend to a new thread.
The backend keeps an in-memory set of thread IDs created after its startup
prompt-boundary verification. A continuation ID outside that set receives
`stale_codex_thread`; the frontend then resends the complete Chatend without the
old ID. Thus deploying or restarting this boundary cannot resume a thread that
contained older hidden instructions.

Codex JSONL may contain intermediate agent messages. Only the last completed
`agent_message` is Kennedy's response. `thread.started` supplies
`response_id`; `turn.completed.usage` supplies input, cached-input, output, and
reasoning tokens. Codex's values are cumulative for the provider thread; the
response marks them as cumulative so the frontend can difference continuation
rounds. When the JSON event includes an optional `last_token_usage` object or
equivalent last-input/output fields, the bridge also exposes those individual
model-pass counts as `last_input_tokens` and `last_output_tokens`; the frontend
uses only those fields for context occupancy. Cache-write tokens are reported
as zero because Codex JSONL does not expose that measurement.

`quality` and `balanced` web search start a new ephemeral Codex thread with
`--search`, retain the same read-only/no-shell restrictions, and retain the
required hosted search capability. Their complete research instruction is the
ordinary supplied prompt, with no additional Codex developer or base prompt.
The mode selects the fixed model, reasoning effort, Codex web-search context
size, deadline, source cap, and focused or thorough research prompt.

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
allowed models, configured `reasoning_effort`, and the effective context limits
discovered from Codex's advertised model catalog. The browser
uses the model plus reasoning effort for runtime prompt disclosure and automatic
Kmap mutation attribution. It also returns per-model `input_modalities` and
effective context/input limits, plus whether transcription is available. The
endpoint never returns authentication details.

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
  "chatend": "David\n\nNew text for this round.",
  "previous_response_id": "019f5ca7-020f-7b63-be2f-82785fb68c03",
  "prompt_cache_key": "kennedy-conversation-prompt-v4"
}
```

The first request omits `previous_response_id` and sends the complete Chatend.
A continuation sends only the newly appended suffix, formatted by the same
canonical frontend formatter as the Full inspector. The backend passes this
string to Codex without wrapping or reformatting it. Continuation IDs must be
Codex UUID thread IDs. `prompt_cache_key` remains in
the stable browser/backend contract but Codex manages caching per thread, so
the bridge does not pass the key to the CLI.

The frontend ends the string with
`context window usage: {used-or-unknown} / {advertised-effective-limit}`.
Usage comes from the latest completed provider response; fresh threads use
`unknown`. The intelligence backend treats this terse line like every other
part of the canonical plaintext and does not rewrite it.

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
    "reasoning_tokens": 30,
    "cumulative": true,
    "last_input_tokens": 520,
    "last_output_tokens": 40
  }
}
```

The bridge does not interpret whether assistant text is a final answer or a
Kennedy text-tool request.

Generation, web-search, and web-fetch requests may include a UUID
`operation_id`. While such a request is active,
`POST /api/v1/operations/{operation_id}/cancel` cancels it. Cancelling a Codex
generation or Codex-backed search drops and kills the child process; cancelling
a remote search or page fetch drops its network future. A cancelled request
returns `409 operation_cancelled`. The cancellation endpoint is idempotent from
the caller's perspective and reports whether the operation was still active.

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

### 5.6 Document Extraction

`POST /api/v1/documents/extract` accepts one multipart file up to 20 MiB and
returns normalized local text, format, character count, and truncation status.
It supports searchable PDF, DOCX, XLSX/XLS/XLSB/ODS, CSV/TSV, and plain-text
formats. Output is capped at 1,000,000 characters. A PDF with no extractable
text returns an explicit error explaining that image-only input requires OCR;
the endpoint does not send document content to a remote service.

## 6. Errors, Logging, and HTTP

Invalid inputs return `400 invalid_request`. Unknown providers/models and a
missing Gemini fast-search secret return `provider_not_configured`. Provider
authentication failures use `401 provider_auth_failed`; subscription, quota,
and rate limits use `429 provider_rate_limited`; provider transport and
protocol failures use sanitized 5xx errors; deadlines use
`504 provider_timeout`. Errors may include a local request UUID but never
credentials or full prompts.

The service binds to the address supplied by `kennedy-server` (loopback by
default), accepts JSON plus bounded multipart audio/documents, exposes explicit
GET and POST routes, permits only the supplied frontend origins, and applies a
request-body limit before deserialization.

## 7. Verification

Tests cover request and thread-ID validation, Codex JSONL event normalization,
last-message selection, usage mapping, source extraction/deduplication,
compiled defaults, all three search profiles, Gemini request and interaction
normalization, model modality metadata, paid-transcription behavior, safe audio
filenames, document format and DOCX extraction, public-URL safety, and
readable-content bounds. Smoke tests should
confirm fresh generation, thread resume, Codex search with the machine's actual
ChatGPT login, and fast search with a configured Gemini key.
