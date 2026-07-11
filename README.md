# Kennedy MVP

Kennedy is a local-first personal assistant with inspectable, provenance-backed
long-term memory. The MVP consists of two Rust services and a browser-native
frontend:

- `kennedy-kweb` owns SQLite, memory/history invariants, and serves the UI.
- `kennedy-intelligence` is a stateless OpenAI-compatible generation bridge.
- `Frontend/public` owns live conversations, context, tool execution, and
  automatic history ingress when a conversation ends.

## First run

Install a current stable Rust toolchain, then create the local intelligence
configuration:

```sh
cp IntelligenceBackend/config.example.yaml IntelligenceBackend/config.yaml
export OPENAI_API_KEY="your-key"
```

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

The example configuration uses `gpt-5.5`. Change `default_model` and the
`models` allowlist together if your account uses another tool-capable model.

## Editing Kennedy

Kennedy's live system prompts are deliberately plain-text files in
[`Frontend/SystemPrompts`](Frontend/SystemPrompts/README.md):

- `SystemPromptKmapAgentManual.txt` — identity, job, shared memory rules, and
  safety boundaries.
- `ConversationAgentManual.txt` — user-facing problem solving and memory
  navigation.
- `HistoryIngressAgentManual.txt` — conservative conversation-to-memory
  extraction.

The browser fetches these files at session startup. Edit them and reload the
page; no compilation is required.

## Verification

```sh
cargo test --workspace
node --experimental-default-type=module --test Frontend/tests/*.test.mjs
```

The Rust suite covers graph limits, bootstrap/history integrity, normalized
request validation, tool-call correlation, and provider response
normalization. The frontend suite covers short IDs, resets, load limits and
budgets, durable-ID translation, clean provenance, and safe rendering.

## MVP boundaries

The MVP intentionally has one local user, no authentication, no streaming, no
durable in-progress chats, and no manual memory editing or deletion. Closing
the page before ending a conversation can lose that active conversation.
