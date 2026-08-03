# Agent Instructions

`Clarifications.md` is binding repository guidance, not optional background.
Before reviewing, planning, or changing this project, every LLM agent must read
`Clarifications.md` in full and follow every applicable instruction throughout
the task. Agents must not make a design or implementation decision that
contradicts it. If a current explicit user instruction revises an existing
clarification, follow the user's revision and update `Clarifications.md` so it
remains authoritative; otherwise, stop and surface any apparent conflict rather
than silently ignoring either instruction. Higher-priority system and developer
instructions continue to take precedence.

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

When working with kcode libraries, the LLM agent must keep a very light touch.
The only top level files that are allowed are Cargo.toml and Documentation.md.
There should be no license file, no changelist file, no readme, and no
dependency audit file.

When splitting kcode libraries into smaller components, an LLM agent must keep
clean and minimal API boundaries. When splitting code, behavior must be
preserved as much as possible, except where clear bugs are identified. Bugs
must be confirmed with the user before they are fixed, as must all other
non-cosmetic changes.

Do not use exact-pin versions for any libraries unless the calling library is
itself passing API key material to the library that it is calling. All other
libraries must accept any compatibility-preserving update automatically.
