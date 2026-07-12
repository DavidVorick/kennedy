# Project Clarifications

These user-provided clarifications supplement the repository specifications and
technical design.

## Chatend Inspector

- The chatend is the human-readable context Kennedy forms and sends to the LLM.
- The main UI's right-hand inspector is a visualization of Kennedy's current
  mental context, not an API-payload viewer or debugging tool.
- The inspector shows readable system context, conversation text, memory
  context, and the ordinary-text JSON envelopes Kennedy uses to request tools.
  It hides provider response IDs, credentials, and non-context bookkeeping.
- Text actually supplied to the model should share those readable qualities.
  System instructions should be organized as prose sections, while loaded Kmap
  nodes and local tool results should use a clear YAML-like text presentation
  instead of JSON serialization.

## OpenAI API and Local Tools

- The OpenAI adapter should use the Responses API rather than Chat
  Completions.
- Kennedy's Kweb tools are implemented and executed locally. Tool definitions
  and the request envelope live in the readable agent manuals; provider-native
  function and custom-tool APIs are not used.
- Kennedy remains responsible for constructing and displaying its complete
  logical context. Append-only rounds use provider response continuation and a
  stable prompt-cache key; ResetContext starts a fresh provider chain.

## Provider Errors

- Provider failures should produce useful, actionable messages instead of
  hiding the cause behind a generic gateway error.
- Error reporting must still protect credentials and other sensitive provider
  data; sanitized provider details and request IDs should be used where useful.

## Transparent tools, caching, and usage

- Kennedy requests local tools through an ordinary, human-visible text
  protocol described completely in the system-prompt manuals. Do not send
  provider-native function or custom-tool definitions.
- A single Kennedy response may request multiple tools. Execute them
  sequentially in the order written and return readable results to the model.
- Continue append-only conversation and history-ingress rounds with provider
  response IDs and stable prompt-cache keys. `ResetContext` starts a fresh
  provider chain because it removes prior Kmap context.
- Enable prompt caching where it is economically sensible and report cache
  reads, cache writes, total tokens, and estimated context-window use in the UI.
- Do not implement automatic resets or compaction. Context resets remain under
  user/Kennedy control.
- Show live history-ingress tool requests and results in the conversation UI so
  the user can follow memory updates.
