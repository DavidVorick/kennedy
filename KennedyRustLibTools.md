# Kennedy Rust Library Tools

Kennedy can create, inspect, edit, validate, and publish small Rust libraries through five local tools. These tools are documented in the Kmap rather than in Kennedy's static prompt. They are always available in every Kennedy execution mode, including browser conversations, private and group Telegram conversations, self-time sessions, history ingress, and audio ingress.

The server uses the published `kcode-rust-libs` crate and manages the filesystem, Podman validation environment, and crates.io credentials. Kennedy supplies only the arguments documented here. Kennedy must never attempt to supply a filesystem root, absolute path, shell command, Podman image, registry token, or other infrastructure setting.

All calls use Kennedy's ordinary `KENNEDY_TOOL_CALLS` envelope. Multiple Rust library calls may appear in one envelope and execute sequentially. A typical single call looks like:

```text
KENNEDY_TOOL_CALLS
{"calls":[{"name":"OpenRustLib","arguments":{"name":"example-lib"}}]}
```

## Session ownership

Creating or opening a library gives the current Kennedy session exclusive ownership of that library's open handle. The same session may own several different libraries. Another Kennedy session cannot open or modify an owned library until the owning session ends and the server drops its handle.

Create or open a library before writing, checking, or publishing it. An idle handle left behind by an abandoned session expires after 24 hours. If the server reports that a library is not open—such as after a server restart or that lease expires—call `OpenRustLib` and continue from the complete files it returns. If it reports that another active Kennedy session owns the library, do not repeatedly retry; continue in the owning session or wait for that session to end.

There is no model-facing close operation. Handles are released automatically when the Kennedy session ends, is reset into history ingress, is purged, or otherwise terminates.

## `CreateRustLib`

Create a new Rust 2024 library and open it in the current Kennedy session.

```json
{"name":"example-lib"}
```

The name must begin with an ASCII letter or digit. Its remaining characters may be ASCII letters, digits, `-`, or `_`. Creation fails if that library already exists.

The result contains the library name, canonical version, complete `Documentation.md` text, and every file sorted by path. The readable result represents each complete file body as a JSON string so newlines, quotes, empty files, and the presence or absence of a final newline are exact rather than visually ambiguous.

A newly created library contains at least:

- `Cargo.toml`, with a Rust 2024 root package at version `0.1.0`;
- `Documentation.md`;
- `Version.txt`, initially `0.1.0` followed by one LF; and
- `src/lib.rs`.

Creation already opens the returned library. Do not call `OpenRustLib` immediately afterward unless the tool result was ambiguous or lost.

## `OpenRustLib`

Open an existing managed library and return its complete current in-memory snapshot.

```json
{"name":"example-lib"}
```

The result has the same complete form as `CreateRustLib`: name, canonical version, documentation, and every UTF-8 file. Read the existing files before editing. Do not guess at omitted source, tests, manifests, documentation, or version metadata.

Calling `OpenRustLib` again from the same session returns the already-open handle's current snapshot. It does not reload changes made outside these tools. The library API intentionally has no reload operation.

The tool library is itself a managed library named `kcode-rust-libs`. Kennedy can inspect and maintain its source, tests, specification, and documentation with:

```json
{"name":"kcode-rust-libs"}
```

## `WriteRustLib`

Create or completely overwrite one or more files in an already-open library.

```json
{
  "name":"example-lib",
  "files":[
    {
      "path":"src/lib.rs",
      "contents":"/// Return the answer.\npub fn answer() -> u8 {\n    42\n}\n"
    },
    {
      "path":"tests/answer.rs",
      "contents":"#[test]\nfn answer_is_42() {\n    assert_eq!(example_lib::answer(), 42);\n}\n"
    }
  ]
}
```

Every `contents` value is the complete desired text of that file, not a patch, diff, search-and-replace fragment, or insertion. An empty string creates or overwrites an empty file. Files omitted from the batch remain unchanged. Duplicate paths in one batch are rejected.

Paths use `/`, are UTF-8, and are relative to the library root. Absolute paths, backslashes, empty components, `.`, `..`, traversal, trailing `/`, NUL, and drive-like `:` paths are rejected. The library also rejects symlinks, non-UTF-8 files, and unsupported filesystem entries.

There is no deletion API. Plan a library so obsolete code can be replaced with harmless text or left unused. Do not simulate deletion through shell commands or filesystem tricks.

Every managed library must always contain root-level `Documentation.md` and `Version.txt`. A write batch is rejected if its projected result would violate either requirement. `Version.txt` must contain a stable canonical `major.minor.patch` such as `0.2.0`, optionally followed by exactly one LF. Prefixes, leading zeroes, prerelease suffixes, extra blank lines, and missing components are invalid.

The result confirms the written paths and the canonical version after the write. It does not repeat every file; Kennedy already has the exact submitted text. Call `OpenRustLib` to view the current complete in-memory snapshot again when needed.

## `CheckRustLib`

Run the complete standardized quality pipeline for an already-open library.

```json
{"name":"example-lib"}
```

The server runs the work in disposable Podman environments outside the managed library. Kennedy does not choose commands, flags, images, or target directories. The stages run fail-fast in this order:

1. dependency fetch;
2. formatting;
3. build;
4. Clippy;
5. tests; and
6. documentation tests.

The result contains `passed` and every stage that ran, including its stage name, success flag, optional exit code, stdout, and stderr. A code-quality failure is a successful tool invocation with `passed: false`; read the first failed stage's output, replace the relevant complete files, and run `CheckRustLib` again. An infrastructure failure is instead a failed tool invocation and is not evidence that the source code is wrong.

Do not treat a partial stage list as success. `passed` becomes true only when all six stages ran and passed.

## `PublishRustLib`

Validate and publish the root Cargo package of an already-open library to crates.io.

```json
{"name":"example-lib"}
```

Publication is an external, durable action. Before publishing:

1. update the library source, tests, and public documentation;
2. choose the intended new stable version;
3. write that version to `Version.txt`;
4. set the root `Cargo.toml` `[package].version` to the exact same literal string;
5. run `CheckRustLib` and inspect a `passed: true` result; and
6. call `PublishRustLib` only when the crate name, version, API, documentation, and behavior are ready to become public.

The version in `Version.txt` must equal the literal `[package].version` in the root `Cargo.toml`. Workspace-inherited versions are rejected. `PublishRustLib` runs the full check again even if a previous explicit check passed, then publishes using the operator-provisioned `cratesio-key` value that the server retrieved from its encrypted vault and supplied to the library at initialization. The managed libraries root contains no credential file. Kennedy never sees or supplies that token.

A successful result confirms the published crate name and version. A validation failure stops publication. Server startup rejects a missing or invalid operator credential; Podman/crates.io failures are infrastructure errors. Do not work around them by placing credentials in library files.

Publication may succeed even if a later browser transport or Chatend checkpoint fails before its success result is retained. Do not blindly repeat an ambiguous `PublishRustLib` call. First verify whether that exact crate version is already present on crates.io, using web research when necessary. The same recovery issue is harmless for complete-file writes because repeating the identical write is idempotent; after an ambiguous create, call `OpenRustLib` rather than creating the same name again.

## Recommended workflow

For a new crate:

1. `CreateRustLib`.
2. Study the generated manifest and files.
3. `WriteRustLib` with complete source, tests, `Documentation.md`, and any manifest changes.
4. `CheckRustLib`.
5. Fix complete files and repeat checks until `passed: true`.
6. Update both version files consistently when preparing a release.
7. Run a final check.
8. `PublishRustLib` only when publication is intended.

For an existing crate:

1. `OpenRustLib` and read every relevant file.
2. Make bounded complete-file writes.
3. Check after meaningful changes rather than accumulating a large unvalidated rewrite.
4. Preserve existing behavior and tests unless the requested design intentionally changes them.
5. Publish only after version alignment and a clean final result.

The tools provide no arbitrary commands, patches, deletion, listing of library names, reload, or model-controlled close. If a task requires one of those absent capabilities, report the limitation rather than inventing a call.
