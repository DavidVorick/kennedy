# Kennedy MVP

Kennedy is a local-first personal assistant with inspectable, provenance-backed
long-term memory. The MVP has three logically independent backend APIs and a
browser-native frontend:

- `kennedy-kweb` owns SQLite, memory/history invariants, and serves the UI.
- `kennedy-intelligence` is a local Codex bridge with thread continuation,
  token/cache telemetry, web research, and safe public page extraction.
- `kennedy-conversation-history` checkpoints active conversations and durably
  stores complete conversation and history-ingress Chatend archives, with
  multiple live conversations and a serialized history-ingress queue.
- `Frontend/public` owns live conversations, context, tool execution, durable
  recovery orchestration, conversation-history browsing, and automatic
  background history ingress.

All three backends are separate library crates with separate listeners, state,
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
a terminal. Kennedy logs separate launcher-started, prompt-forwarded, and
Codex-completed stages, and fails prompt forwarding after 30 seconds rather
than hanging silently.

Create the local intelligence configuration:

```sh
cp IntelligenceBackend/config.example.yaml IntelligenceBackend/config.yaml
```

No API key is required. Startup rejects API-key-only Codex authentication so a
misconfigured machine cannot silently bill ordinary OpenAI API usage.

Start Kennedy with one command:

```sh
cargo run -p kennedy-server
```

Open `http://127.0.0.1:4321`. The Kweb and conversation databases are created
as `kennedy.sqlite3` and `kennedy-conversations.sqlite3` on first run. The three
APIs bind to loopback ports 4321, 4322, and 4323.

The example configuration uses `gpt-5.6-sol` with `xhigh` reasoning effort and
executes each turn through `codex-safe`, which invokes non-interactive
`codex exec` inside Podman.
Change `default_model` and the `models` allowlist together if your account uses
another compatible model, and configure its context limits when they are not
known to the bridge.

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
serialized, offline history-ingress worker owns memory mutation. Search policy
and retrieval limits stay in the intelligence backend. The UI also reports
provider token usage, context-window headroom, and prompt-cache reads and
writes in the Chatend header, and shows history ingress as it runs. The
Chatend inspector can display the complete context, just the system prompts,
or an expandable tree of loaded Kmap memory.

The browser fetches these files at session startup. Edit them and reload the
page; no compilation is required.

## Verification

```sh
cargo test --workspace
node --experimental-default-type=module --test Frontend/tests/*.test.mjs
```

The Rust suite covers graph limits, bootstrap/history integrity, conversation
state transitions, normalized request validation, cached continuation request
shape, and provider usage normalization. The frontend suite covers short IDs,
resets, load limits, checkpoint-before-generation ordering, pending-query
recovery, multi-call text-tool execution, usage aggregation, clean provenance,
and safe rendering. Intelligence tests also cover Codex event normalization,
thread-ID validation, and search-source extraction.

## MVP boundaries

The MVP intentionally has one local user, no authentication, no streaming, and
no manual memory editing or deletion. Active conversations and unfinished
history ingress survive an abrupt UI close; transient provider-chain and tool
telemetry are rebuilt rather than restored.
