# Kennedy MVP

Kennedy is a local-first personal assistant with inspectable, provenance-backed
long-term memory. The MVP consists of two Rust services and a browser-native
frontend:

- `kennedy-kweb` owns SQLite, memory/history invariants, and serves the UI.
- `kennedy-intelligence` is a local OpenAI Responses bridge with response-chain
  continuation and prompt-cache telemetry.
- `Frontend/public` owns live conversations, context, tool execution, and
  automatic history ingress when a conversation ends.

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

Start the intelligence bridge in one terminal:

```sh
cargo run -p kennedy-intelligence -- --config IntelligenceBackend/config.yaml
```

Start the Kweb and frontend in another terminal:

```sh
cargo run -p kennedy-kweb
```

Open `http://127.0.0.1:4321`. The SQLite database is created as
`kennedy.sqlite3` on first run. Both services bind to loopback only.

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
so tool requests and results are visible in the chatend. The UI also reports
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

The Rust suite covers graph limits, bootstrap/history integrity, normalized
request validation, cached continuation request shape, and provider usage
normalization. The frontend suite covers short IDs, resets, load limits,
multi-call text-tool execution, usage aggregation, clean provenance, and safe
rendering.

## MVP boundaries

The MVP intentionally has one local user, no authentication, no streaming, no
durable in-progress chats, and no manual memory editing or deletion. Closing
the page before ending a conversation can lose that active conversation.
