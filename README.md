# Kennedy MVP

Kennedy is a local-first personal assistant with inspectable, provenance-backed
long-term memory. The MVP has four logically independent backend APIs and a
browser-native frontend:

- `kennedy-kweb` owns SQLite, memory/history invariants, and serves the UI.
- `kennedy-intelligence` is a local Codex bridge with thread continuation,
  token/cache telemetry, web research, and safe public page extraction.
- `kennedy-conversation-history` checkpoints active conversations and durably
  stores complete conversation and history-ingress recovery archives, with
  multiple live conversations and a serialized history-ingress queue.
- `kennedy-telegram-relay` uses `teloxide` to queue authorized Telegram text,
  voice, document, and reset events while the browser remains the visible Chatend owner.
- `Frontend/public` owns live conversations, context, tool execution, durable
  recovery orchestration, conversation-history browsing, and automatic
  background history ingress.

All four backends are separate library crates with separate listeners, state,
and databases. One `kennedy-server` binary hosts them without allowing the
backends to call or access one another.

## First run

Install a current stable Rust toolchain. Kennedy expects a `codex-safe` host
launcher on `PATH`; that launcher runs Codex inside the persistent Podman
sandbox and forwards its arguments plus piped stdin/stdout. The host itself
does not need a `codex` binary. Sign the sandboxed Codex into the ChatGPT
account whose subscription limits Kennedy should use:

```sh
codex-safe login
codex-safe login status
```

For service calls, `codex-safe` must use `podman run -i` so the Chatend reaches
Codex over stdin. It must not require a TTY; add `-t` only when its own stdin is
a terminal. Kennedy logs one duration for the complete LLM call and fails
prompt forwarding after 30 seconds rather than hanging silently. Tool calls and
complete user turns have their own concise duration logs and matching Chatend
latency entries.

The launcher must also make the backend-created
`${TMPDIR:-/tmp}/kennedy-codex-catalogs` directory visible inside the Codex
container at the same absolute path, read-only. For example, set
`catalog_dir="${TMPDIR:-/tmp}/kennedy-codex-catalogs"` and add
`--mount type=bind,src="$catalog_dir",dst="$catalog_dir",ro` to the container's
`podman run` or `podman create` arguments. If the launcher uses a persistent
container, recreate that container with the bind mount. Kennedy creates the
source directory before its first launcher call, probes the generated catalog
through `codex-safe` before using it, and warns while falling back to the stock
catalog when the mount is unavailable.

The UI's Full Chatend inspector and generation path share one plaintext
formatter: what the Full inspector shows is every application-controlled byte
of the prompt supplied to Codex for Kennedy, not a formatted JSON archive.
Codex or its upstream provider can still add forced system/tool scaffolding
outside the application's observable boundary. Versioned JSON archives exist
only for recovery and provenance storage. History ingress parses an archive
and sends its human-readable message text under `Archived Chatend`; it does not
send the archive envelope, media blobs, counters, or diagnostics.

The launcher must also forward `codex-safe debug models`. Kennedy discovers the
configured model's advertised effective context window at startup and refuses
to invent a fallback. It derives a slim catalog from that live result, removes
only Codex's agent-tool selectors, verifies that all advertised effective
limits are unchanged, and uses the catalog only when a launcher probe succeeds.
Every turn also uses minimal inline Codex instructions and suppresses exposed
optional instruction/tool/plugin scaffolding, including Codex's separately
configured `request_user_input` tool. Stock Codex still registers its core
`update_plan` and environment-backed `view_image` schemas; there is no supported
0.144.1 setting to remove them, so the inline instruction forbids their use.
All Codex turns set the auto-compaction threshold beyond any reachable window
so Kmap context is not silently compacted.
Each model-facing Chatend ends with the terse line
`context window usage: {used-or-unknown} / {advertised-effective-limit}`.
History ingress
records up to five concise failure diagnostics, then marks the conversation's
memory update failed and advances the queue instead of retrying indefinitely.

Kennedy has no runtime configuration file. Stable provider, model, search,
audio, and safety defaults are compiled into the codebase. Deployment-specific
listener addresses, database paths, frontend paths, and the encrypted vault
path remain ordinary `kennedy-server` CLI options; run with `--help` to inspect
or override them.

No API key is required for ordinary Kennedy generation. Startup rejects
API-key-only Codex authentication so a
misconfigured machine cannot silently bill ordinary OpenAI API usage.

Voice notes use the paid `gpt-4o-transcribe` API because the configured
`gpt-5.6-sol` transport has no native audio input. Store the OpenAI API key in
Kennedy's generic passphrase-encrypted credential vault:

```sh
cargo run -p kennedy-server -- secrets set openai-api-key
```

The low-latency `fast` WebSearch tier uses Gemini 3.1 Flash-Lite with Google
Search grounding. Store its API key under the compiled secret name:

```sh
cargo run -p kennedy-server -- secrets set gemini-api-key
```

The first `secrets set` command creates `kennedy-secrets.age`, asks for a vault
passphrase twice, and then asks for the secret value twice without echoing
either input. To enable the optional Telegram relay, create a bot with
BotFather and store its token under Kennedy's conventional secret name:

```sh
cargo run -p kennedy-server -- secrets set telegram-bot-token
```

The first private message from `@taek42` binds that stable numeric Telegram
user ID; later authorization no longer depends on the username. The encrypted
vault is mode `0600`, excluded by `.gitignore`, contains arbitrary named
secrets, and has no reveal command or HTTP API. Available maintenance commands
are `secrets list`, `secrets remove NAME`, and `secrets change-passphrase`.

Start Kennedy with one command:

```sh
cargo run -p kennedy-server
```

When the encrypted vault exists, startup prompts once for its passphrase and
keeps the unlocked values only inside `kennedy-server`. Copy
`kennedy-secrets.age` alongside the three SQLite databases to migrate the same
credentials to another machine; the same vault passphrase unlocks them there.

Open `http://127.0.0.1:4321`. The Kweb and conversation databases are created
as `kennedy.sqlite3`, `kennedy-conversations.sqlite3`, and
`kennedy-telegram.sqlite3` on first run. The four APIs bind to loopback ports
4321 through 4324. Without a Telegram token, port 4324 reports the relay as
disabled and the rest of Kennedy remains usable.

The compiled defaults use `gpt-5.6-sol` with `xhigh` reasoning effort and
execute each turn through `codex-safe`, which invokes non-interactive
`codex exec` inside Podman. If the deployment needs another compatible model,
change the provider model constants in `IntelligenceBackend/src/defaults.rs`;
the usable context window is always read from Codex's advertised metadata.

## Editing Kennedy

Kennedy's live system prompts are deliberately plain-text files in
[`Frontend/SystemPrompts`](Frontend/SystemPrompts/README.md):

- `KennedyIdentity.txt` — Kennedy's identity and Kmap-based learning model.
- `ConversationManual.txt` — exact live-mode, read-only Kmap, and tool
  mechanics.
- `HistoryIngress.txt` — exact ingress-mode and Kmap mutation mechanics.

Kennedy's strategy for using her harness is intentionally learned and stored in
her own Kmap graph rather than embedded in the mode manuals. Every session
starts with both the user's root and Kennedy's root loaded.

Kennedy's local tools use a text protocol documented in the session manuals,
so tool requests and results are visible in the chatend. Live conversations can
read Kmap memory and use WebSearch/WebFetch but cannot mutate the Kmap; the
serialized, offline history-ingress worker owns memory mutation. Kennedy
chooses `quality`, `balanced`, or `fast` for each WebSearch call; the concrete
provider, model, reasoning, context, deadline, and retrieval bounds for those
modes stay in the intelligence backend. The UI also reports provider token
usage, context-window headroom, and prompt-cache reads and writes in the
Chatend header, and shows history ingress as it runs. The Chatend inspector can
display the complete context, just the system prompts, or an expandable tree
of loaded Kmap memory.

The browser fetches these files at session startup. Edit them and reload the
page; no compilation is required.

The `TG Bot` view shows one normal Kennedy conversation per Telegram user. The
browser must be open to run Kennedy, but Telegram messages remain durably
queued while it is closed. `/reset` closes the current Telegram session and
queues its full Chatend for the same history-ingress flow as an ended UI
conversation. The browser composer also has a microphone button; both sources
preserve the original audio with the paid transcription.

The browser composer and Telegram also accept PDF, DOCX, spreadsheet, CSV, and
text documents up to 20 MiB. Kennedy receives locally extracted, bounded text;
searchable PDFs work directly, while scanned/image-only PDFs report that OCR is
required.

## Verification

```sh
cargo test --workspace
node --experimental-default-type=module --test Frontend/tests/*.test.mjs
```

The Rust suite covers graph limits, bootstrap/history integrity, conversation
state transitions, Telegram authorization/queue behavior, normalized request validation, cached continuation request
shape, and provider usage normalization. The frontend suite covers short IDs,
resets, load limits, checkpoint-before-generation ordering, pending-query
recovery, multi-call text-tool execution, usage aggregation, clean provenance,
and safe rendering. Intelligence tests also cover Codex event normalization,
thread-ID validation, and search-source extraction.

## MVP boundaries

The MVP intentionally has one local Kmap user, bootstrap-only Telegram access
control, no streaming, and no manual memory editing or deletion. Active conversations and unfinished
history ingress survive an abrupt UI close; transient provider-chain and tool
telemetry are rebuilt rather than restored.
