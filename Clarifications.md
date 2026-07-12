# Project Clarifications

These user-provided clarifications supplement the repository specifications and
technical design.

## Chatend Inspector

- The chatend is the human-readable context Kennedy forms and sends to the LLM.
- The main UI's right-hand inspector is a visualization of Kennedy's current
  mental context, not an API-payload viewer or debugging tool.
- The inspector should show only readable system context, conversation text,
  and memory context. It must hide JSON envelopes, function-call arguments,
  provider items, call IDs, and other transport bookkeeping.
- Text actually supplied to the model should share those readable qualities.
  System instructions should be organized as prose sections, while loaded Kmap
  nodes and local tool results should use a clear YAML-like text presentation
  instead of JSON serialization.

## OpenAI API and Local Tools

- The OpenAI adapter should use the Responses API rather than Chat
  Completions.
- Kennedy's Kweb tools are implemented and executed locally. Function-tool
  definitions sent to the model only let it return structured requests for
  those local operations; OpenAI does not execute them.
- Kennedy remains responsible for constructing and resending its context. The
  integration should not depend on provider-side conversation persistence.

## Provider Errors

- Provider failures should produce useful, actionable messages instead of
  hiding the cause behind a generic gateway error.
- Error reporting must still protect credentials and other sensitive provider
  data; sanitized provider details and request IDs should be used where useful.
