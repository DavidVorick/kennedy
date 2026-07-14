# Kennedy system prompts

These plain-text files are Kennedy's live behavioral configuration. The Kweb
service serves them to the browser, and the browser fetches them at session
startup. Edit a file and reload the page to test a change; no Rust or JavaScript
rebuild is needed.

- `KennedyIdentity.txt` defines who Kennedy is and establishes that her learned
  strategy lives in the Kmap rather than in static harness instructions.
- `ConversationManual.txt` describes the live-conversation mode, its read-only
  Kmap mechanics, and the exact tools available there.
- `HistoryIngress.txt` describes the non-interactive ingress mode, its Kmap
  mechanics, and the exact mutation tools available there.

The identity is composed first, followed by the mode manual, then a dynamic
runtime sentence identifying the configured model and thinking mode. Mode
manuals are deliberately limited to mode behavior, technical Kmap facts, the transparent
tool-request protocol, and exact tool contracts. Strategy and learned judgment
belong in Kennedy's Kmap. Keep technical contracts synchronized with validation
and execution in `Frontend/public/js/tools.js`; hard limits remain enforced in
code.
