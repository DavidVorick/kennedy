# Kennedy system prompts

These plain-text files are Kennedy's live behavioral configuration. The Kweb
service serves them to the browser, and the browser fetches them at session
startup. Edit a file and reload the page to test a change; no Rust or JavaScript
rebuild is needed.

- `KmapAgentManual.txt` defines Kennedy's identity, primary job,
  safety boundaries, and shared memory model.
- `ConversationAgentManual.txt` defines how she helps the user and navigates
  memory during a visible conversation.
- `HistoryIngressAgentManual.txt` defines how an ended conversation is
  conservatively turned into durable memory.

The shared manual is composed first, followed by the session-specific manual.
Keep tool names and JSON argument names synchronized with
`Frontend/public/js/tools.js`. The prompts should describe judgment and policy;
hard limits and validation are enforced in code.
