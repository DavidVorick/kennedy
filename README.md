# Kennedy

Kennedy is a local-first personal assistant with inspectable, transactional
long-term memory. One native Rust server owns orchestration; the browser and
Telegram are clients.

This repository is maintained by LLM agents. Read
[`Clarifications.md`](Clarifications.md) for durable user intention, then inspect
the current code, tests, manifests, and dependency source for exact mechanics.
Do not infer current behavior from old commits, runtime recovery material, or
inactive managed-library generations that the workspace does not resolve.

## Source Orientation

- `KennedyServer/src/` contains the server, HTTP adapters, orchestration, Kmap
  integration, and background workers.
- `KennedyServer/runtime/system-prompts/` contains live prompt layers loaded at
  startup.
- `scripts/` contains the offline backup and constrained Codex runtime helpers.
- `data/` is ignored runtime state. It also contains Kennedy-managed source and
  immutable publications. Treat it as user data, not disposable build output.
- `target/` and `.tools/` are generated build/tool state.

`Cargo.toml` and `KennedyServer/Cargo.toml` are the dependency and workspace
authority. Some managed libraries may be linked into the workspace from
`data/kcode/`; inspect the resolved path and `Cargo.lock` rather than copying
version claims into prose.

## Verify

```sh
cargo build --workspace
cargo test --workspace
```

Run the server with:

```sh
cargo run -p kennedy-server
```

Use `cargo run -p kennedy-server -- --help` for current options and maintenance
commands.

## Persistence Safety

Kennedy-owned runtime state defaults beneath `data/`. Do not manually edit,
delete, migrate, or replace it merely to make a build or test pass.

For an offline backup, stop Kennedy and run:

```sh
scripts/backup
```

The script archives the complete opaque `data/` tree outside that tree. The
credential vault is encrypted, but the backup as a whole is not.
