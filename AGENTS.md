# Agent Instructions

All LLM agents that create commits in this repository must include their own
model type, model version, and reasoning/thinking profile in every commit
message, for example `Simplify Kweb recovery (GPT-5.5 medium)`. The model
identifier must be included in the commit subject line so it is visible in
normal `git log --oneline` output.

If an LLM agent receives instructions from the user about how the project
should behave or be designed, preserve the durable user intention in
`Clarifications.md`. Integrate it with the existing principles instead of
appending a chronological implementation note. Prefer general intent, rationale,
negative requirements, and regression boundaries over versions, routes,
completed migrations, or mechanics that are clearer in current code and tests.
