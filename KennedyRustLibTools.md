# Kennedy Rust Library Tools

Kennedy can create, inspect, replace, validate, and publish small Rust libraries through six local tools. They are available in every Kennedy execution mode. The server uses exact-pinned `kcode-rust-libs-v2` 1.1.0 and owns the configured filesystem root, crates.io credential, authorization, and transport. Kennedy supplies only the arguments documented here.

Invoke each operation through the native `call_ktool` function. For example:

```json
{"name":"kcode-rust-libs-v2/open","arguments":{"name":"example-lib"}}
```

## Snapshots and concurrency

`kcode-rust-libs-v2/create` and `kcode-rust-libs-v2/open` retain a complete source snapshot for the current Kennedy session. Open before writing, checking, or publishing. Several sessions may open the same library; no session holds a persistent filesystem lock.

Writes are optimistic. If another snapshot commits first, `kcode-rust-libs-v2/write` fails with `stale_snapshot`. Call `kcode-rust-libs-v2/open` to reload the current complete source, reconcile the intended change, and retry. Calling `open` again always replaces the session's retained snapshot with the current repository generation.

Retained snapshots are discarded when the Kennedy session ends and abandoned snapshots expire after 24 hours. If an operation reports that the library is not open, call `kcode-rust-libs-v2/open` and continue from the returned source.

## `kcode-rust-libs-v2/create`

Create a new Rust 2024 library at version `0.1.0` and retain its initial snapshot.

```json
{"name":"example-lib"}
```

The name must begin with an ASCII letter or digit and then contain only ASCII letters, digits, `-`, or `_`. Creation fails if the library already exists.

The result contains the library name and every useful UTF-8 source file in canonical path order. A new library contains `Cargo.toml`, `Documentation.md`, and `src/lib.rs`. File bodies are exact JSON strings. `Documentation.md` appears once inside `files`; `Cargo.lock` and private repository metadata are excluded.

## `kcode-rust-libs-v2/open`

Open or reload an existing library and return its complete useful source.

```json
{"name":"example-lib"}
```

Read every returned file before editing. Do not infer omitted source. Opening a valid library stored by the legacy `kcode-rust-libs` backend automatically migrates its flat repository into the current generation format while preserving the original flat files as recovery material.

## `kcode-rust-libs-v2/docs`

Read only the canonical package version and complete root `Documentation.md`.

```json
{"name":"example-lib"}
```

The result contains `name`, `version`, and `documentation`. This operation does not retain a source snapshot, so call `open` separately before writing, checking, or publishing. A first docs read may perform the same safe legacy migration as `open`.

## `kcode-rust-libs-v2/write`

Commit a complete replacement for the snapshot retained by this session.

```json
{
  "name":"example-lib",
  "files":[
    {
      "path":"Cargo.toml",
      "contents":"[package]\nname = \"example-lib\"\nversion = \"0.2.0\"\nedition = \"2024\"\n"
    },
    {
      "path":"Documentation.md",
      "contents":"# API\n"
    },
    {
      "path":"src/lib.rs",
      "contents":"pub fn answer() -> u8 { 42 }\n"
    }
  ]
}
```

`files` is the entire desired source. Omitted files are deleted. Every content value is a complete UTF-8 file body, not a patch. Root `Cargo.toml` and `Documentation.md` are required. `[package].name` must equal the managed-library name and `[package].version` must be a double-quoted canonical stable `major.minor.patch`. `Cargo.lock` is ephemeral and must not be supplied.

Paths are slash-separated and relative to the library root. Empty components, `.`, `..`, absolute paths, backslashes, colons, NUL, symlinks, special files, duplicates, and non-UTF-8 source are rejected.

A successful result reports the canonical paths and file count. A failed write does not advance the repository. Reopen after `stale_snapshot`; correct the complete file set directly after an ordinary validation error.

## `kcode-rust-libs-v2/check`

Validate the exact retained in-memory source.

```json
{"name":"example-lib"}
```

The fixed fail-fast pipeline performs dependency fetch, formatting, locked/offline build, Clippy with warnings denied, tests, and documentation tests in disposable Podman work. Success returns `passed: true`. Failure is a failed tool invocation containing one bounded category and relevant diagnostic excerpt; successful stage logs are not returned.

## `kcode-rust-libs-v2/publish`

Recheck and publish the exact retained in-memory source to crates.io.

```json
{"name":"example-lib"}
```

Publication is durable. Finish source, tests, documentation, and the intended version; run `check`; inspect `passed: true`; then publish. The server-provisioned token never appears in Kennedy's arguments, source, command line, or returned diagnostics.

Publication may succeed even if a later transport or checkpoint loses the result. Do not blindly repeat an ambiguous publish. Verify the exact version on crates.io first.

## Recommended workflow

For a new library:

1. `kcode-rust-libs-v2/create`.
2. Replace the complete source with `kcode-rust-libs-v2/write`.
3. Run `kcode-rust-libs-v2/check` and repair bounded failures.
4. Publish only when explicitly intended.

For an existing library:

1. `kcode-rust-libs-v2/docs` when only API documentation is needed, or `kcode-rust-libs-v2/open` for source work.
2. Submit the complete desired source with `kcode-rust-libs-v2/write`.
3. On `stale_snapshot`, reopen, reconcile, and retry.
4. Check, then publish only when ready.

The tools expose no arbitrary commands, host paths, credentials, Podman settings, repository listing, rename, patch, or model-controlled close operation.
