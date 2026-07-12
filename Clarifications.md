# Project Clarifications

These user-provided decisions supplement the repository specifications and
technical design. They may be consolidated or removed once represented by the
canonical documents; this file is not an append-only log.

## Chatend and Inspector

- The chatend is the complete, human-readable logical context Kennedy forms for
  the LLM. The main UI's right-hand inspector visualizes this mental context,
  rather than provider payloads or transport diagnostics.
- Show readable system and conversation text, memory context, Kennedy's
  ordinary-text JSON tool requests, and readable tool results. Hide provider
  response IDs, credentials, and non-context bookkeeping.
- Organize system instructions as prose sections and loaded Kmap nodes and tool
  results as clear YAML-like text rather than serialized JSON.
- Provide Full, System Prompts, and expandable Memory Tree views. Distinguish
  directly loaded nodes, full nodes included through active connections, and
  summary-only fanout references.
- Keep exact context occupancy and remaining-window telemetry visible in the
  Chatend header. Also report provider cache reads, cache writes, and total
  token usage where available.

## Tools and Provider Execution

- Use the OpenAI Responses API. Kennedy's Kweb tools are local operations
  requested through the ordinary, human-visible text protocol defined in the
  system-prompt manuals; do not use provider-native function or custom-tool
  APIs.
- A Kennedy response may request multiple tools. Execute them sequentially in
  written order and return readable results to the model.
- Give live-conversation Kennedy `WebSearch(question)` for delegated hosted
  research and `WebFetch(url)` for inspecting one public page. Search language,
  geography, freshness, domains, result counts, and research depth are not tool
  arguments; Kennedy states relevant constraints naturally and the intelligence
  layer manages retrieval policy and budgets.
- Continue append-only conversation and history-ingress rounds using provider
  response IDs and stable prompt-cache keys. `ResetContext` starts a fresh
  provider chain with Kennedy's rebuilt logical context.
- Use prompt caching where economically sensible. Do not automatically compact
  or reset context; resets remain under user or Kennedy control.
- Return actionable, sanitized provider errors, including request IDs when
  useful, without exposing credentials or other sensitive provider data.

## Frontend Behavior

- Persist the active conversation through a conversation-history backend.
  Checkpoint each user query before any LLM request, restore unfinished work on
  startup, and durably require history ingress to finish before a new
  conversation may begin.
- Kennedy has three logically separate backends: Kweb, intelligence, and
  conversation history. They are independent services with separate APIs,
  listeners, state, and databases and must not call or access one another. They
  happen to be compiled into and hosted by one Rust binary for operational
  convenience; the frontend and architecture otherwise treat them as if they
  were unrelated processes.
- Show live history-ingress tool requests and results in the conversation UI
  so the user can follow memory updates.
- Serve the local frontend without reusable browser caching and version its
  entry assets so HTML and JavaScript revisions cannot be mixed.
- Surface startup exceptions as visible failures instead of leaving the UI
  frozen at “Starting…”.
