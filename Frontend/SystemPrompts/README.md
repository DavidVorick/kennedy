# Kennedy system prompts

These plain-text files are Kennedy's live behavioral configuration. The Kweb
service serves them to Kennedy's backend orchestration worker and the browser's
read-only inspector. Edit a file and restart Kennedy to test a change; no Rust
or JavaScript rebuild is needed.

- `KennedyIdentity.txt` defines who Kennedy is and establishes that her learned
  strategy lives in the kmap rather than in static harness instructions.
- `CodexHarness.txt` corrects Codex wrapper capability claims, and is included
  only when the backend-selected inference provider has kind `codex`.
- `ConversationSession.txt`, `SelfTimeSession.txt`, `HistoryIngressSession.txt`,
  and `AudioIngressSession.txt` are mutually exclusive, minimal descriptions of
  the current session and its context-loading budget.
- `KmapBasics.txt` defines identifier lifetime, the session's automatic root
  set, the text tool-call protocol, and the fact that more tools and
  documentation may be available through the kmap.
- `ReadTools.txt` defines all shared read-only tools, including kmap reads and
  web research.
- `WriteTools.txt` defines the kmap mutation tools included in ingress and self
  time.

The backend composes the identity first, then the selected session type, kmap
basics, read-only tools, optional write tools, the Codex harness note only for
Codex inference, and a dynamic runtime sentence identifying the configured
model and thinking mode. Each fact or tool contract has one prompt source;
strategy and learned judgment belong in Kennedy's kmap. Keep
technical contracts synchronized with validation and execution in
`KennedyServer/src/orchestration/session.rs`; hard limits remain enforced in
code.
