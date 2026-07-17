# Kennedy MVP

Kennedy is a local-first personal assistant with inspectable, provenance-backed
long-term memory. The MVP has five logically independent backend APIs and a
browser-native frontend:

- `kennedy-kweb` owns SQLite, memory/history invariants, and serves the UI.
- `kennedy-intelligence` is a local Codex bridge with thread continuation,
  token/cache telemetry, web research, and safe public page extraction.
- `kennedy-conversation-history` checkpoints active conversations and durably
  stores complete conversation and history-ingress recovery archives, with
  multiple live conversations and a serialized history-ingress queue.
- `kennedy-telegram-relay` uses `teloxide` plus a separate user-directory SQLite
  database to enforce TOFU whitelists/permanent group decisions and queue
  authorized private or group work while the browser remains the visible
  Chatend owner.
- `kennedy-audio-ingress` durably owns content-addressed vnotes, restartable
  Gemini/Sol transcript preparation, and timestamped Kennedy-ingress pieces.
- `Frontend/public` owns live conversations, context, tool execution, durable
  recovery orchestration, conversation-history browsing, and automatic
  background history ingress.

All five backends are separate library crates with separate listeners, state,
and databases. One `kennedy-server` binary hosts them without allowing the
backends to call or access one another.

## First run

Install a current stable Rust toolchain. Kennedy expects a `codex-safe` host
launcher on `PATH`; that launcher runs Codex inside the persistent Podman
sandbox and forwards its arguments plus piped stdin/stdout. The host itself
does not need a `codex` binary. Sign the sandboxed Codex into the ChatGPT
account whose subscription limits Kennedy should use:

```sh
codex-safe login
codex-safe login status
```

For service calls, `codex-safe` must use `podman run -i` so the Chatend reaches
Codex over stdin. It must not require a TTY; add `-t` only when its own stdin is
a terminal. Kennedy logs one duration for the complete LLM call and fails
prompt forwarding after 30 seconds rather than hanging silently. Tool calls and
complete user turns have their own concise duration logs and matching Chatend
latency entries.

The launcher must also make the backend-created
`${TMPDIR:-/tmp}/kennedy-codex-catalogs` directory visible inside the Codex
container at the same absolute path, read-only. For example, set
`catalog_dir="${TMPDIR:-/tmp}/kennedy-codex-catalogs"` and add
`--mount type=bind,src="$catalog_dir",dst="$catalog_dir",ro` to the container's
`podman run` or `podman create` arguments. If the launcher uses a persistent
container, recreate that container with the bind mount. Kennedy creates the
source directory before its first launcher call, probes the generated catalog
through `codex-safe` before using it, and warns while falling back to the stock
catalog when the mount is unavailable.

The UI's Full Chatend inspector and generation path share one plaintext
formatter: what the Full inspector shows is every application-controlled byte
of the prompt supplied to Codex for Kennedy, not a formatted JSON archive.
Codex or its upstream provider can still add forced system/tool scaffolding
outside the application's observable boundary. Versioned JSON archives exist
only for recovery and provenance storage. History ingress parses an archive
and sends its human-readable message text under `Archived Chatend`; it does not
send the archive envelope, media blobs, counters, or diagnostics.

The launcher must also forward `codex-safe debug models`. Kennedy discovers the
configured model's advertised effective context window at startup and refuses
to invent a fallback. It derives a slim catalog from that live result, removes
only Codex's agent-tool selectors, verifies that all advertised effective
limits are unchanged, and uses the catalog only when a launcher probe succeeds.
Every turn also uses minimal inline Codex instructions and suppresses exposed
optional instruction/tool/plugin scaffolding, including Codex's separately
configured `request_user_input` tool. Stock Codex still registers its core
`update_plan` and environment-backed `view_image` schemas; there is no supported
0.144.1 setting to remove them, so the inline instruction forbids their use.
All Codex turns set the auto-compaction threshold beyond any reachable window
so Kmap context is not silently compacted.
Each model-facing Chatend ends with the terse line
`context window usage: {used-or-unknown} / {advertised-effective-limit}`.
History ingress
records up to five concise failure diagnostics, then marks the conversation's
memory update failed and advances the queue instead of retrying indefinitely.

Kennedy has no runtime configuration file. Stable provider, model, search,
audio, and safety defaults are compiled into the codebase. Deployment-specific
listener addresses, database paths, frontend paths, and the encrypted vault
path remain ordinary `kennedy-server` CLI options; run with `--help` to inspect
or override them.

No API key is required for ordinary Kennedy generation. Startup rejects
API-key-only Codex authentication so a
misconfigured machine cannot silently bill ordinary OpenAI API usage.

Voice notes use the paid `gpt-4o-transcribe` API because the configured
`gpt-5.6-sol` transport has no native audio input. Store the OpenAI API key in
Kennedy's generic passphrase-encrypted credential vault:

```sh
cargo run -p kennedy-server -- secrets set openai-api-key
```

The low-latency `fast` WebSearch tier uses Gemini 3.1 Flash-Lite with Google
Search grounding. Durable vnote ingress uses Gemini 3.1 Pro Preview for its
ordered four-minute transcription chunks. Store the shared Gemini API key
under the compiled secret name:

```sh
cargo run -p kennedy-server -- secrets set gemini-api-key
```

The first `secrets set` command creates `kennedy-secrets.age`, asks for a vault
passphrase twice, and then asks for the secret value twice without echoing
either input. To enable the optional Telegram relay, create a bot with
BotFather and store its token under Kennedy's conventional secret name:

```sh
cargo run -p kennedy-server -- secrets set telegram-bot-token
```

The first observed Telegram account presenting the initially whitelisted
`@taek42` handle pins its stable numeric user ID under TOFU; later authorization
no longer depends on the username. David can whitelist another trusted handle
with `/adduser @theirHandle`, which reserves that user's blank Kmap root before
its first matching observation. The encrypted
vault is mode `0600`, excluded by `.gitignore`, contains arbitrary named
secrets, and has no reveal command or HTTP API. Available maintenance commands
are `secrets list`, `secrets remove NAME`, and `secrets change-passphrase`.
Stop the running server before using these commands; they acquire the same Kweb
address used to exclude server startup during an offline backup.

Start Kennedy with one command:

```sh
cargo run -p kennedy-server
```

When the encrypted vault exists, startup prompts once for its passphrase and
keeps the unlocked values only inside `kennedy-server`. Copy
`kennedy-secrets.age` alongside the five SQLite databases and audio-ingress
media directory to migrate the same credentials to another machine; the same
vault passphrase unlocks them there.

Open `http://127.0.0.1:4321`. The Kweb and conversation databases are created
as `kennedy.sqlite3`, `kennedy-conversations.sqlite3`,
`kennedy-telegram.sqlite3`, `kennedy-users.sqlite3`, and
`kennedy-audio.sqlite3` on first run. Original
vnotes and restartable working chunks live under `kennedy-audio-ingress/`. The
five APIs bind to loopback ports 4321 through 4325. Without a Telegram token,
port 4324 reports the relay as disabled and the rest of Kennedy remains usable.

## Durable vnote ingress

[`scripts/vnote-start`](scripts/vnote-start) and
[`scripts/vnote-stop`](scripts/vnote-stop) are non-interactive scripts intended
for i3 hotkeys. Start records directly into `/home/user/media/vnotes`, using the
recording-start Unix timestamp in the filename. Stop ends `arecord`, examines
the five newest vnotes, asks Kennedy which SHA-256 hashes she already has, and
uploads only finalized, stable files that are missing. Both upload scripts take
one two-second directory snapshot and verify the finalized RIFF/WAVE length,
so a recording that is still changing or has not had its header closed is
ignored. The local WAV files are never deleted.

For example, use absolute paths in i3:

```text
bindsym $mod+Shift+v exec --no-startup-id /path/to/kennedy/scripts/vnote-start
bindsym $mod+Shift+b exec --no-startup-id /path/to/kennedy/scripts/vnote-stop
```

The two paths are plain defaults at the top of the scripts if this fixed
loopback setup changes later; `KENNEDY_VNOTE_DIR` and `KENNEDY_AUDIO_API`
override them. The stop hotkey waits only for file stability and each upload
HTTP response; it never waits for transcription, reconciliation, or Kmap
ingress.

For importing a backlog or a reorganized backup, copy
[`scripts/vnote-ingress`](scripts/vnote-ingress) into a directory containing
vnotes and run it directly:

```sh
./vnote-ingress
```

It scans every `*-vnote.wav` in its own directory from newest recording to
oldest, checks each content hash against Kennedy, and uploads the first five
finalized, stable recordings that Kennedy does not already know. It continues
past files that are still changing or have unfinished WAV headers. It uses the
recording-start epoch embedded by `vnote-start`; for other matching filenames
it falls back to the file's modification time. Set `KENNEDY_AUDIO_API` to
override the default loopback API URL. SHA-256 results are cached privately in
`.vnote-ingress-sha256-cache-v1` beside the script. A cached digest is reused
only while the file's device, inode, size, modification time, and change time
still match, so later backlog scans do not reread unchanged large recordings.
Deleting the cache is safe; the next scan simply rebuilds it.

After acceptance, the server hashes and stores the original WAV, creates equal
four-minute-or-shorter windows with fifteen seconds of neighboring overlap,
and transcribes up to four windows concurrently with
`gemini-3.1-pro-preview`. Successful chunk results are stored immediately and
retries send only unfinished chunks. `gpt-5.6-sol` with `xhigh` reasoning
receives the stored transcripts in chronological order, reconciles speakers,
removes repeated overlap, preserves annotations and translations, and produces
the canonical final transcript. If needed, Sol inserts sensible boundaries so
each Kennedy ingress piece remains at or below an estimated 50,000 tokens.
Every piece repeats the recording timestamp and shares the recording SHA-256
identity. Processing stages, transcripts, retries, and Kennedy ingress
checkpoints are SQLite-backed and resume after server or browser restarts. Kmap
mutation runs when Kennedy's browser worker is open and remains serialized with
ordinary conversation-history ingress.

Recordings are idempotent by content hash. Check one with
`GET /api/v1/audio-ingress/by-sha256/{sha256}` or inspect the entire durable
processing history in the UI's **Audio Ingress** tab. Selecting a recording
shows its metadata, Gemini chunk transcripts, reconciled final transcript,
Kennedy-sized pieces, retry records, and the saved Kennedy Chatend for each
piece.

## Backups

Stop the running Kennedy server, then create an offline backup with:

```sh
cargo run -p kennedy-server -- backup
```

The command first binds the configured Kweb address and serves a small
maintenance page there. It fails before creating backup files if Kennedy or
another backup already owns that address. Normal Kennedy startup also acquires
the Kweb address before opening the vault or any database, preventing a second
instance from modifying persistent state during a backup.

The result is a private
`backups/kennedy-backup-YYYY-MM-DDTHH-MM-SSZ.tar.gz` archive containing
verified standalone snapshots of all five SQLite databases, the complete
audio-ingress media directory, the encrypted credential vault when present, a
machine-readable checksum manifest, and a self-contained recovery README. The
README begins with the creating source commit and includes the exact SQLite DDL
and current persisted JSON/vault/audio formats. Gzip does not encrypt the
databases or recordings; move the archive to appropriately protected
off-machine storage.

Use `--backup-dir PATH` to select another destination. The existing global
`--kweb-bind`, `--kweb-database`, `--conversation-history-database`,
`--telegram-database`, `--user-database`, `--audio-ingress-database`,
`--audio-ingress-media`, and `--vault-path` flags select the lock address and
source files when their deployment values differ from the defaults.

For a quick estimate of the current Kmap's model-context footprint, run:

```sh
cargo run -p kennedy-server -- kmap-size
```

This read-only command can run while Kennedy is serving. It reports estimated
tokens for complete node text and for long descriptions alone, using one token
per four Unicode characters, and also prints the underlying word and character
counts. Node history, provenance, connections, and every non-node table are
excluded.

The compiled defaults use `gpt-5.6-sol` with `xhigh` reasoning effort and
execute each turn through `codex-safe`, which invokes non-interactive
`codex exec` inside Podman. If the deployment needs another compatible model,
change the provider model constants in `IntelligenceBackend/src/defaults.rs`;
the usable context window is always read from Codex's advertised metadata.

## Editing Kennedy

Kennedy's live system prompts are deliberately plain-text files in
[`Frontend/SystemPrompts`](Frontend/SystemPrompts/README.md):

- `KennedyIdentity.txt` — Kennedy's identity and Kmap-based learning model.
- `ConversationSession.txt`, `HistoryIngressSession.txt`, and
  `AudioIngressSession.txt` — minimal, mutually exclusive session descriptions.
- `KmapBasics.txt` — shared identifier, root, tool-protocol, and
  Kmap-discoverable-capability facts.
- `ReadTools.txt` — shared Kmap and web read-only tools.
- `WriteTools.txt` — ingress-only Kmap mutation tools.

Kennedy's strategy for using her harness is intentionally learned and stored in
her own Kmap graph rather than embedded in static prompts. The frontend composes
identity, session type, Kmap basics, read-only tools, optional write tools, and
the current runtime in that order. Web and private Telegram sessions start with
the user and Kennedy roots loaded; persistent group-user sessions additionally
load the group root. Full nodes identify their Kennedy, user, or group owner;
legacy null owners are shown as unowned. Three arbitrary numbered fixed
connections replace the former priority/task terminology.

Kennedy's local tools use a text protocol documented once in `KmapBasics.txt`,
so tool requests and results are visible in the chatend. Every session can read
Kmap memory and use WebSearch/WebFetch. Live conversations cannot mutate the
Kmap; the serialized, offline history-ingress worker owns memory mutation. Kennedy
chooses `quality`, `balanced`, or `fast` for each WebSearch call; the concrete
provider, model, reasoning, context, deadline, and retrieval bounds for those
modes stay in the intelligence backend. The UI also reports provider token
usage, context-window headroom, and prompt-cache reads and writes in the
Chatend header, and shows history ingress as it runs. The Chatend inspector can
display the complete context, just the system prompts, or an expandable tree
of loaded Kmap memory.

The browser fetches these files at session startup. Edit them and reload the
page; no compilation is required.

The `TG Bot` view shows private conversations and persistent Telegram-group
sessions scoped to one user in one group. The
browser must be open to run Kennedy, but Telegram messages remain durably
queued while it is closed. `/reset` closes the current Telegram session and
queues its full Chatend for the same history-ingress flow as an ended UI
conversation. In a group, `/reset` closes only the invoking user's session for
that group. Each allowed group has its own blank
Kmap root. Kennedy loads the invoker's root, the group root, and her own root,
lists every other participant's root as a loadable reference, and receives the
latest 50 messages initially and unseen messages on later invocations. Voice
notes sent as replies and supported documents sent by caption mention or reply
use the same transcription/extraction paths as DMs. More than 100 uninvoked group messages queues the oldest 80
for background ingress with the group and Kennedy roots loaded. Groups require Kennedy to be an
administrator and are permanently blacklisted on an unknown/conflicting member
or incomplete membership ledger. The browser composer also has a microphone button; both sources
preserve the original audio with the paid transcription.

The browser composer and Telegram also accept PDF, DOCX, spreadsheet, CSV, and
text documents up to 20 MiB. Kennedy receives locally extracted, bounded text;
searchable PDFs work directly, while scanned/image-only PDFs report that OCR is
required.

## Verification

```sh
cargo test --workspace
node --experimental-default-type=module --test Frontend/tests/*.test.mjs
```

The Rust suite covers graph limits, bootstrap/history integrity, conversation
state transitions, Telegram authorization/queue behavior, normalized request validation, cached continuation request
shape, and provider usage normalization. The frontend suite covers short IDs,
resets, load limits, checkpoint-before-generation ordering, pending-query
recovery, multi-call text-tool execution, usage aggregation, clean provenance,
and safe rendering. Intelligence tests also cover Codex event normalization,
thread-ID validation, and search-source extraction.

## MVP boundaries

The MVP intentionally has a small code-seeded Telegram whitelist with one
`/adduser` administrator, trusted shared-Kmap access without per-root access
controls, no streaming, and no manual memory editing or deletion. Active conversations and unfinished
history ingress survive an abrupt UI close; transient provider-chain and tool
telemetry are rebuilt rather than restored.
