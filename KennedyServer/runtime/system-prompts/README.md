# Kennedy system prompts

These plain-text files are Kennedy's live backend behavioral configuration.
KennedyServer loads them into the orchestration worker. Edit a file and restart
Kennedy to test a change; no Rust rebuild is needed.

- `KennedyIdentity.txt` defines who Kennedy is and establishes that her learned
  strategy lives in the kmap rather than in static harness instructions. Its
  wording is intentionally preserved.
- `ConversationSession.txt`, `SelfTimeSession.txt`, `HistoryIngressSession.txt`,
  and `AudioIngressSession.txt` are mutually exclusive descriptions of the
  current session.
- `TelegramSession.txt` is added only for private or group Telegram sessions.
  `TelegramGroupSession.txt` is added only for Telegram groups. Retained group
  messages are supplied separately as ordinary prose, never serialized into
  the system prompt as JSON.
- `KmapBasics.txt` introduces the kmap to a model with no prior knowledge of
  Kennedy's harness. It explains the graph, identifiers, connections,
  automatically loaded roots, and Kmap-first tool discovery without embedding
  individual tool contracts.
- `ReadTools.txt` defines only the critical Kmap and context-navigation tools
  needed to discover and manage further context.
- `WriteTools.txt` defines the kmap mutation tools included in ingress and self
  time.
- `CodexHarness.txt` establishes the native `call_ktool` boundary.

The backend composes the identity first, then the selected session type,
channel-specific Telegram layers when applicable, Kmap basics, critical
navigation tools, writable tools when allowed, the Codex harness note, and
dynamic runtime information. Runtime information identifies the model,
thinking mode, and current date and time using an unambiguous twelve-hour
clock with `am` or `pm` and an explicit UTC label.

Most tool inventories, model matrices, and operating manuals belong in the
kmap and are loaded only when relevant. Prefer natural-language context over
JSON or other structured renderings whenever an exact machine protocol is not
required. Keep the remaining prompt contracts synchronized with validation and
execution in `KennedyServer/src/orchestration/session.rs`; hard limits remain
enforced in code.
