# Kennedy MVP

Kennedy is a local-first personal assistant with inspectable, provenance-backed
long-term memory. The MVP has three logically independent backend APIs and a
browser-native frontend:

- `kennedy-kweb` owns SQLite, memory/history invariants, and serves the UI.
- `kennedy-intelligence` is a local OpenAI Responses bridge with response-chain
  continuation, prompt-cache telemetry, hosted web research, and safe public
  page extraction.
- `kennedy-conversation-history` checkpoints active conversations and durably
  stores complete conversation and history-ingress Chatend archives and
  durably gates new conversations on successful history ingress.
- `Frontend/public` owns live conversations, context, tool execution, durable
  recovery orchestration, conversation-history browsing, and automatic
  background history ingress.

All three backends are separate library crates with separate listeners, state,
and databases. One `kennedy-server` binary hosts them without allowing the
backends to call or access one another.

## First run

Install a current stable Rust toolchain, then create the local intelligence
configuration:

```sh
cp IntelligenceBackend/config.example.yaml IntelligenceBackend/config.yaml
```

Open `IntelligenceBackend/config.yaml` and replace
`replace-with-your-openai-api-key` with your API key. Keep the populated file
private and do not commit or share it. Create a key from the
[OpenAI API keys page](https://platform.openai.com/api-keys), then copy the new
secret into the `api_key` field.

Start Kennedy with one command:

```sh
cargo run -p kennedy-server
```

Open `http://127.0.0.1:4321`. The Kweb and conversation databases are created
as `kennedy.sqlite3` and `kennedy-conversations.sqlite3` on first run. The three
APIs bind to loopback ports 4321, 4322, and 4323.

The example configuration uses `gpt-5.6-sol` with `xhigh` reasoning effort.
Change `default_model` and the `models` allowlist together if your account uses
another compatible model, and configure its context limits when they are not
known to the bridge.

## Editing Kennedy

Kennedy's live system prompts are deliberately plain-text files in
[`Frontend/SystemPrompts`](Frontend/SystemPrompts/README.md):

- `KmapAgentManual.txt` — identity, job, shared memory rules, and
  safety boundaries.
- `ConversationAgentManual.txt` — user-facing problem solving and memory
  navigation.
- `HistoryIngressAgentManual.txt` — conservative conversation-to-memory
  extraction.

Kennedy's local tools use a text protocol documented in the session manuals,
so tool requests and results are visible in the chatend. During conversation,
Kennedy can delegate a natural-language WebSearch question or inspect one
source with WebFetch; search policy and retrieval limits stay in the
intelligence backend. The UI also reports
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
and safe rendering.

## MVP boundaries

The MVP intentionally has one local user, no authentication, no streaming, and
no manual memory editing or deletion. Active conversations and unfinished
history ingress survive an abrupt UI close; transient provider-chain and tool
telemetry are rebuilt rather than restored.
