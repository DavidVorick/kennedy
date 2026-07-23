# Kennedy

Kennedy is a local-first personal assistant with inspectable, transactional
long-term memory. The browser and Telegram are clients of one native Rust
orchestrator; the browser does not run Kennedy.

## Current architecture

- `kcode-kweb-db` 1.0 owns canonical binary Kweb nodes, immutable objects,
  histories, signed transactions, the append-only transaction log, and WAL.
- `Chatend` (`kennedy-chatend`) owns one append-only, checksummed journal per
  in-progress session. Source activity and history ingress share that journal.
- `ConversationHistory` retains its historical crate/package name but
  implements the Session History domain: lifecycle and command sidebands in
  the journal, plus one local list of completed Kweb archive object IDs.
- `KennedyServer` owns provider orchestration, Ktools, Kweb graph policy,
  credential handling, the global Kweb writer lane, HTTP adapters, and static
  frontend serving.
- `Frontend/public` is a browser-native observer and command client.
- `AudioIngress`, `MemoryIngress`, and the Telegram crate own their specialized
  durable intake streams.

Every provider-visible item is a Chatend box. Boxes can be hydrated,
dehydrated, summarized, or stale. Kennedy controls their representation. There
is no ResetContext and no fixed LoadNode count.

Ordinary sessions use at most 70% of the model's effective context. Their
history-ingress continuation may use 100%; a 72% emergency boundary force-ends
an overfull source session and sends the same journal to ingress.

Read-only sessions run concurrently. One global V1 writer lane serializes
history ingress, self time, audio ingress, and other Kweb writes. A completed
write session creates one Kweb transaction and one immutable session archive
object.

See [TechnicalDesign.md](TechnicalDesign.md),
[UserSpecification.md](UserSpecification.md), and
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
- audio, Telegram, user-directory, and mixed audio-ingress SQLite files under
  `data/`;
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

### Chatend

An active session is one `.chatend` file. Frames have a kind, canonical
little-endian length, SHA-256 checksum, and payload. Evolving event/control
state is JSON; staged objects are raw bytes with a compact metadata prefix.
Updates are appended immediately without explicit flush or `fsync`. A
transition that needs several events is encoded as one frame under one
checksum. On replay, the first incomplete or checksum-invalid frame and
everything after it are discarded. Checksum-valid structural or schema errors
remain fatal.

The browser uploads original objects as multipart data and receives a shared
temporary ID such as `pending:47`. At commit, Kennedy reads the staged bytes
and passes them to Kweb. V1 intentionally accepts this second disk write.

### Session History

Completed local history is only `data/session-history.txt`, one permanent Kweb
archive object ID per line. The browser loads archive detail from Kweb on
demand. There is no completed-session metadata database and no purge action.

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

Stop Kennedy or let the command acquire the offline listener lock:

```sh
cargo run -p kennedy-server -- backup
```

Backup format 10 includes:

- the complete Kweb root and objects;
- all active Chatend journals;
- `session-history.txt`;
- current runtime SQLite databases;
- audio media;
- the encrypted vault, when present.

Use `--lightweight-kweb` only when immutable Kweb objects are preserved
elsewhere.

## Legacy cutover

The old conversation SQLite runtime was retired on 2026-07-23. Six unfinished
sessions were exported one file each, the complete legacy database was moved
under `data/archive/`, and 67 legacy conversation-ingress rows were archived
before removal from the live mixed queue. No runtime code loads those files.

The one-time command remains available for reproducibility:

```sh
cargo run -p kennedy-server -- archive-legacy-sessions
```

It refuses to overwrite an existing archive and requires the server to be
stopped.

## Deliberate V1 limits

- The exact signed Kweb transaction package cannot yet be prepared and
  persisted before local `finalize`; a narrow post-commit/pre-journal crash
  window is accepted.
- Zero-copy object handoff and streaming objects are deferred.
- The writer lane is global rather than fine-grained.
- Gossip protocol integration is deferred.
- Legacy sessions are offline archives, not compatibility records.
