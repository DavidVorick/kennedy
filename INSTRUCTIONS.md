# Repository Instructions

All LLM agents that create commits in this repository must include their own
model type, model version, and reasoning/thinking profile in every commit
message, for example `Add kweb API specification (GPT-5.5 medium)` The model
identifier must be included in the commit subject line so it is visible in
normal `git log --oneline` output.

If an LLM agent receives instructions from the user about how the project
should be designed, the LLM should put a concise summary of the instructions in
a file called Clarifications.md, such that future LLMs can see the extra
instructions that were provided by the user to help shape the repository.
