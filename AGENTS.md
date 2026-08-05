# Agent Instructions

The most important step for any agents is to get verification from a human by
listing out the full set of proposed changes before making any changes. The
'summary of intended work' should include:
    + The intended goal of the feature
    + The scope of the feature - what will be built?
        + Anything that is not enumerated in the scope should not be built
        + If, while building, you find that exta work is necessary, you must
          reconfirm with the user before completing that work.
    + The non-scope of the feature - what are the boundaries, what will not be built?
    + The impact of the changes
        + What files exactly are being updated?
        + How many lines of code are projected to be added and removed in each file?
        + An enumeration of the changes to each file.
        + How will the changes affect performance? At what scale will there be
          measurable performance impact?
    + The architecture of the changes
        + What dependencies are being introduced?
        + What design patterns are being used?
    + Any extra notes or tidbits that might be of interest to a senior software
      engineer that will have to maintain the codebase.
    + Whether or not the architecture could be materially simplified while
      still achieving the desired objectives (establish this last).

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

Edge cases and boundary conditions are only to be checked only at the point
where they are imminent. For example, if you are calling a library, and that
library can only handle files of a certain size, the check that the files do
not exceed the max size supported by the library should be performed **by the
library**. It is not the caller's responsibility to sanitize input before
calling a library - it is the library's responsibility to sanitize the input
after it is called. Could should not sanitize input, check for boundary
conditions, or handle edge cases unless the fault/error would happen within its
own logic.

If you are an agent reading this, you are likely running in a sandbox. You will
not be able to tell whether kennedy-server is running merely by checking if the
port is available.

When making changes to existing code, always make the minimal possible changes
to meet the updated requirements. Assume that existing design decisions within
the codebase were well justified unless you receieve explicit confirmation from
the the user that updates are acceptable.

The target size for libraries is under 500 lines of code, where each library
has a clean API boundary and minimal API functions with minimal inputs.
