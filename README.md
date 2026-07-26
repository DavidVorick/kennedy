# Kennedy

Kennedy is a local-first personal assistant with inspectable, transactional
long-term memory. The browser and Telegram are clients of one native Rust
orchestrator; the browser does not run Kennedy.

## Current architecture

- `kcode-kweb-db` 1.0 owns canonical binary Kweb nodes, immutable objects,
  histories, signed transactions, the append-only transaction log, and WAL.
- `kcode-session-log` 0.2.1 owns one append-only, checksummed transcript per
  in-progress session and one durable file per pending object. Only
  `kcode-session-history` depends on it.
- `kcode-session-history` 0.1.0 is a published standalone library. It owns
  opaque active `Session` handles, Chatend
  reconstruction, lifecycle and commands in a separate control journal,
  pending objects, and durable completion receipts. It has no Axum dependency.
- `KennedyServer` owns context and orchestration policy, provider calls,
  Ktools, Kweb graph policy, credential handling, the global Kweb writer lane,
  Session History's HTTP adapter, and static frontend serving.
- `Frontend/public` is a browser-native observer and command client.
- `kcode-audio-ingress` is a standalone library that owns durable audio intake,
  automatic transcription state, and completed transcripts. `KennedyServer`
  owns its Axum adapter and the separate audio memory-ingress queue. The
  Telegram crate owns its durable intake stream.
- `kcode-dev-tools` owns the session-scoped Rust-library, Web-library, and
  Rust-binary Ktool adapters. Its `kcode-rust-bins` backend publishes immutable
  local executables and exchanges call payloads through Kennedy's Kweb object
  adapter.

Every provider-visible item is a Chatend box. Boxes can be hydrated,
dehydrated, summarized, or stale. Kennedy controls their representation. There
is no ResetContext and no fixed LoadNode count.

Ordinary sessions admit user messages, tool exchanges, and Kennedy responses
only when the resulting context remains at or below 70% of the model's
effective window. Rejections are retained as system capacity messages; if that
message leaves context above 75%, the source session is force-ended and sent
to history ingress. Ingress initially fits at or below 75% but may then use
100%.

Read-only sessions run concurrently. One global V1 writer lane serializes
history ingress, self time, audio ingress, and other Kweb writes. A completed
write session creates one Kweb transaction and one immutable session archive
object.

See [TechnicalDesign.md](TechnicalDesign.md),
[UserSpecification.md](UserSpecification.md),
[web-libs-education.txt](web-libs-education.txt), and
[chatend-overhaul/chatend-discussion-review.txt](chatend-overhaul/chatend-discussion-review.txt).

## Requirements

- current stable Rust;
- Podman;
- a `codex-safe` launcher on `PATH`;
- provider and service credentials in Kennedy's encrypted vault.

The host does not need a separate `codex` binary if `codex-safe` provides it
inside the persistent sandbox. Sign that sandbox into the ChatGPT account
Kennedy should use:

```sh
codex-safe login
codex-safe login status
```

Build the reusable development runtime on the Podman host, then install the
tracked launcher at the deployment path:

```sh
scripts/build-codex-safe-runtime
install -m 0755 scripts/codex-safe /home/user/podman/codex-safe
```

The build derives `local/codex-dev` from `local/codex` and installs stable
Rust, rustfmt, Clippy, rust-src, rust-analyzer, native build dependencies, and
Chromium. It verifies the Rust tools and a headless Chromium launch under the
same outer Podman restrictions as `codex-safe`. Re-run it when the development
toolchain should be refreshed. Set `CODEX_SAFE_BASE_IMAGE`,
`CODEX_SAFE_RUNTIME_IMAGE`, or `CODEX_SAFE_RUST_TOOLCHAIN` to override its
defaults.

For an interactive terminal launch, `codex-safe` mounts the Git repository root
when the current directory is in a repository; otherwise it mounts the current
directory itself. Noninteractive launches without `CODEX_SAFE_WORKSPACE` use
the failsafe workspace at
`/home/user/podman/codex-state-empty-workspace` by default and wipe it before
each mount.

Service calls use piped stdin/stdout, so the launcher must use `podman run -i`
and must not require a TTY. Add `-t` only when the launcher's own stdin is a
terminal.

Kennedy creates a sanitized model catalog under:

```text
${TMPDIR:-/tmp}/kcode-codex-catalogs
```

The launcher must mount that directory at the same absolute path inside the
Codex container, read-only. `scripts/codex-safe` documents and implements the
expected mount.

The launcher uses `local/codex-dev` by default and keeps Cargo's download cache
under `/home/user/podman/codex-state-cargo-cache` on the host. Override that
location with `CODEX_SAFE_CARGO_CACHE_DIR`. The cache is mounted at
`/tmp/codex-cargo`, which remains writable to Codex's `workspace-write`
sandbox, and does not expose the host user's personal Cargo credentials.

## Build and verify

```sh
cargo build --workspace
cargo test --workspace
node --test Frontend/tests/*.test.mjs
```

Node is used only for frontend development tests. It is not a production
dependency.

## Credential vault

The default vault is `data/kennedy-secrets.age`. It is passphrase-encrypted and
has no HTTP, frontend, model, Codex, or reveal surface.

```sh
cargo run -p kennedy-server -- secrets list
cargo run -p kennedy-server -- secrets set openai-api-key
cargo run -p kennedy-server -- secrets set gemini-api-key
cargo run -p kennedy-server -- secrets set telegram-bot-token
cargo run -p kennedy-server -- secrets set cratesio-key
```

The Kweb signing key and authorized-writer list are stored under
`kweb-writer-signing-key` and `kweb-writers-by-priority`. They must be generated
and installed locally by a human-controlled process; never paste private key
material into a model conversation or commit it to this repository.

At startup, the human unlocks the vault. The server passes the writer key
directly to `kcode-kweb-db`; Kennedy never sees it.

## Run

```sh
cargo run -p kennedy-server
```

Defaults:

- main UI and Kennedy APIs: `127.0.0.1:4321`;
- Telegram transport: `127.0.0.1:4324`;
- Kweb root: `data/kweb`;
- in-progress journals: `data/sessions/in-progress`;
- completed Session History ID list: `data/session-history.txt`;
- AudioIngress persistence root: `data/audio-ingress-media` (including its
  derived `state.sqlite3` and `originals/`);
- Kennedy's audio memory-ingress queue and the Telegram and user-directory
  SQLite files under `data/`;
- managed Rust libraries: `data/kcode/kcode-rust-libs`;
- managed Web libraries and immutable publications:
  `data/kcode/kcode-web-libs` (published versions are created lazily beneath
  `.published/module/<name>/v<version>/`);
- managed Rust-binary source: `data/kcode/kcode-rust-bins`;
- immutable published Rust executables:
  `data/kcode/kcode-rust-bin-artifacts`;
- frontend assets: `Frontend/public`;
- system-prompt boxes: `Frontend/SystemPrompts`.

The main listener is bound before persistent state is opened. Maintenance
commands use that address as an offline lock.

## Storage model

### Kweb

`data/kweb` is a `kcode-kweb-db` 1.0 root. IDs are canonical eight-character
URL-safe Base64 strings with disjoint node/object domains. Current node files,
objects, per-node history, transaction packages, state, log, and WAL use
canonical checksummed binary encodings.

Node data contains complete fixed and recent connection arrays. Application
policy, user roots, and Telegram identity mappings live outside the library.

Kweb enforces 32 GiB for one object and 32 GiB aggregate object payload in one
transaction.

### Session log

An active session has one `<session-id>.session-log` file containing exactly a
three-field header and an ordered array of `{role, text}` events. The header
fields are the string format version, session ID, and creation time. Array
position is the event's stable identity, so event IDs are not serialized.
Each frame is checksummed and synchronized before an append returns. Opening a
log discards an incomplete trailing frame but rejects checksum-valid structural
corruption.

Each pending object is written first as
`<session-id>-<event-position>.pending-object`, including filename, media type,
declared byte length, checksum, and bytes. Its `pending-object` event is
appended only after the object file and containing directory are durable.
Opening the session removes object files not referenced by the ordered event
array.

Chatend boxes, representations, token policy, context projection, Kweb plans,
tool interpretation, lifecycle, and commands are outside `kcode-session-log`.
KennedyServer reconstructs the current Chatend from the transcript.

The browser uploads original objects as multipart data and receives a shared
temporary ID such as `pending:47`. At commit, Kennedy reads the staged bytes
and passes them to Kweb. V1 intentionally accepts this second disk write.

### Session History

`data/session-history.txt` is an append-only JSON-lines completion index. Each
new receipt records the Kweb transaction ID when available, permanent archive
object ID, pending-node mappings, and pending-object mappings. Older
one-ID-per-line records remain readable. The browser loads the immutable
session-log archive from Kweb on demand. There is no purge action.

## Frontend

Open the main origin in a browser. The UI can:

- start, continue, retry, end, and stop sessions;
- capture/upload original attachments and voice notes;
- inspect boxes, their canonical revisions, representations, stale state, and
  continuation markers;
- inspect Kweb nodes using canonical IDs;
- view self-time, Telegram, audio, and completed Session History.

Only Kennedy can manage boxes. The frontend uses DOM text rendering and does
not inject backend content as HTML.

## Backup

Stop Kennedy, then archive the complete opaque `data/` tree:

```sh
scripts/backup
```

The optional argument selects an output directory; the default is the ignored
top-level `backups/` directory:

```sh
scripts/backup /path/to/off-machine-backups
```

The script refuses to start while a `kennedy-server` process exists or the
default Kennedy listener is accepting connections. It creates one timestamped
`.tar.gz` containing the complete opaque contents of `data/` plus
`BACKUP-METADATA.txt`, which records the source commit and whether the source
tree was dirty. Progress is reported as the compressed archive grows, and the
completed archive is moved into place only after `tar` succeeds.

The output directory must be outside `data/` so the archive cannot include
itself. The backup is not encrypted as a whole: Kweb, sessions, databases, and
audio are plaintext even though the included credential vault remains
encrypted. For recovery, extract the archive at the repository root, check out
the recorded commit, and inspect or migrate the restored formats from that
source version.

Kennedy-owned runtime defaults live under `data/`. Explicit path overrides
outside that tree are operator-managed and are not captured by this script.
Codex login state and the source vnote recording directory are external inputs,
not Kennedy-owned runtime state.

## Legacy cutover

The old conversation SQLite runtime was retired on 2026-07-23. Six unfinished
sessions were exported one file each, the complete legacy database was moved
under `data/archive/`, and 67 legacy conversation-ingress rows were archived
before removal from the live mixed queue. No runtime code loads those files.
The completed cutover tools are no longer part of the runtime binary.

## Deliberate V1 limits

- The exact signed Kweb transaction package cannot yet be prepared and
  persisted before local `finalize`; a narrow post-commit/pre-journal crash
  window is accepted.
- Zero-copy object handoff and streaming objects are deferred.
- The writer lane is global rather than fine-grained.
- Gossip protocol integration is deferred.
- Legacy sessions are offline archives, not compatibility records.
