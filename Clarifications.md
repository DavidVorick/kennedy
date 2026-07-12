# Project Clarifications

These user-provided clarifications supplement the repository specifications and
technical design.

## Chatend Inspector

- The chatend is the context Kennedy forms and sends to the LLM.
- The main UI's right-hand inspector must display the chatend itself, not a
  diagnostic object that merely contains the chatend. Compact metadata may be
  shown separately from the inspector's JSON body.

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
