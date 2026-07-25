# kcode-tg-kennedy-bot

`kcode-tg-kennedy-bot` is a transplantable Rust library for Telegram transport. It owns Telegram polling, bounded message/media persistence, fail-closed group security, availability-first per-principal dispatch, and a loopback work-queue API. It deliberately owns no Kennedy users, whitelist entries, application capabilities, secrets vault, Kmap roots, prompts, model execution, or downstream object store.

The directory is a standalone Rust 2024 Cargo package with no workspace-inherited metadata or dependencies. `Documentation.md` is the detailed integration guide, `Specification.md` is the behavioral contract, and the literal package version in `Cargo.toml` is canonical.

## Host contract

The host passes an optional bot token to `serve` and implements `IdentitySink`:

- `observe_identity` receives a numeric Telegram ID, current handle when present, and display name.
- `whitelist` returns the numeric Telegram user IDs authorized at that moment.
- `request_add_user` authorizes and performs the private `/adduser @handle` workflow outside the relay.
- `observe_group` receives the relay's stable opaque group ID so the host can associate application-owned state independently.

```rust,no_run
use std::{path::PathBuf, sync::Arc};

use kcode_tg_kennedy_bot::{BotToken, Config, IdentitySink};

async fn run(
    identity_sink: Arc<dyn IdentitySink>,
    token: String,
) -> anyhow::Result<()> {
    kcode_tg_kennedy_bot::serve(Config {
        bind: "127.0.0.1:4324".into(),
        database: PathBuf::from("telegram.sqlite3"),
        allowed_origins: vec!["http://127.0.0.1:4321".into()],
        bot_token: Some(BotToken::new(token)?),
        identity_sink,
        max_voice_bytes: 20 * 1024 * 1024,
    })
    .await
}
```

`max_voice_bytes` is retained as a source-compatible field name, but it limits every inbound and outbound media payload. It must be nonzero.

Passing `None` as `bot_token` keeps the loopback API available while disabling Telegram polling. A configured token is rejected when empty, validated with Telegram before readiness, redacted from `Debug`, never serialized, and zeroized on drop.

Call `migrate_storage(path)` to apply idempotent SQLite migrations without starting the listener or poller.

## Security boundary

The HTTP API has no bearer-token or per-user authentication layer. It accepts only a literal IPv4 or IPv6 loopback bind such as `127.0.0.1:4324` or `[::1]:4324`. Startup rejects wildcard, LAN, public, hostname, and malformed values, including `0.0.0.0`, `[::]`, and `localhost`.

This prevents accidental direct exposure when the host also serves public websites. It does not authorize untrusted local processes and does not protect a public reverse proxy or an SSRF-capable service that can reach arbitrary loopback URLs. Do not proxy the relay API to the internet. Add an explicit authenticated boundary before changing the loopback-only rule.

Browser requests must use exactly one configured `Origin`; requests carrying Fetch Metadata headers without `Origin` are rejected. Native host clients may omit both. CORS is defense in depth, not authentication. Responses are marked `no-store` and `nosniff`, and request bodies are bounded by the configured media limit plus multipart overhead.

## Availability-first polling

The next Telegram polling offset is durable in SQLite. Updates are sorted by update ID, offered to bounded in-memory dispatch, and then durably skipped past even when an individual update is malformed, saturated, or fails processing. This is intentionally at-most-once-leaning: losing one informal chatbot update is preferable to freezing every later user.

Private work is keyed by numeric user ID. Ordinary group work is keyed by Telegram chat ID and sender ID, while group-control work has its own key. Each key is FIFO, but independent keys may run concurrently. Current bounds are 32 waiting updates per principal, 256 active principal keys, and 16 concurrently executing processors. Queue saturation, processor errors, and processor panics remain local to the affected stream.

Finite Telegram operations retry only plausibly transient provider, network, timeout, and download failures, with at most five total attempts. `RetryAfter` is respected; permanent errors stop immediately. Sends can be duplicated when Telegram accepts a request but the network loses its response. The relay deliberately has no durable delivery outbox or exactly-once claim.

Host `IdentitySink` callbacks, Telegram requests, media downloads, and retry sleeps execute without holding the shared SQLite mutex. A slow host callback or provider operation therefore does not serialize unrelated local database work.

## Group security and sessions

The bot must be a group administrator. The relay permanently records every human identity observed in administrator lists, membership updates, joins, leaves, and message envelopes, including departed and kicked members.

A group is allowed only while all of these conditions hold:

1. Telegram confirms that the bot is an administrator or owner.
2. Telegram's member count matches the observed active-human ledger plus the bot.
3. Every human ever recorded for the group is in the host's current whitelist.

Otherwise the group is quarantined. Quarantine returns before invocation parsing, content extraction, media download, feedback, archival, or exposure to Kennedy. Eligibility is recomputed on later updates, so quarantine is reversible after the roster is complete and every historical identity is authorized.

Telegram cannot enumerate all ordinary members of an existing group. Reliable strict onboarding therefore starts with a new group: add the bot, promote it to administrator, and then add human members so their joins are observed.

Each allowed group user has a session pointer separate from private sessions and from every other group user. If the host discovers that a downstream conversation is permanently missing, it can clear only the exact current pointer with:

`POST /api/v1/group-sessions/{conversation_id}/detach-if-current`

The JSON body supplies `groupId` and `telegramUserId`. The compare-and-swap operation conflicts if the pointer is absent, already detached, or rebound, and preserves messages, events, cursors, resets, membership, and other users.

## Bidirectional media

Authorized private and allowed-group messages accept voice notes, generic documents, photos, videos, animations, audio tracks, video notes, and stickers. A generic document remains `document` regardless of MIME type. The relay enforces the configured media limit before and during download and retains bounded Bot API bytes, exact caption or sticker emoji where applicable, essential MIME/file/duration metadata, transport owner, and event or group association.

For photos, Telegram supplies provider-generated renditions. The relay retains the rendition with greatest pixel area, then declared size, then stable provider order. It does not claim to retain the sender's exact original upload.

The host elects whether to retrieve a file, copy it into an object store, extract it, expose it to Kennedy, or ignore it:

- `GET /api/v1/events/{event_id}/media`
- `GET /api/v1/group-messages/{chat_id}/{message_id}/media`

The host can send a bounded generic document back through an active event with:

`POST /api/v1/events/{event_id}/file`

This multipart endpoint requires `conversationId` and a nonempty `file`. It accepts optional `fileName`, `caption`, and `complete`. Names are bounded and path-free, captions respect Telegram's UTF-16 limit, and group deliveries are archived. `complete=false` leaves the event active for additional files or a later text reply. `complete=true` completes only after Telegram accepts the file and the event binding still matches.

Native delivery uses:

`POST /api/v1/events/{event_id}/media`

It additionally requires `kind` set to `photo`, `video`, `animation`, `audio`, `video_note`, or `sticker`. Only `fileName`, `caption`, and `complete` are optional; captions are forbidden for video notes and stickers. Each kind calls its matching native Telegram method. The route never falls back to `sendDocument`; hosts that want document semantics must deliberately use `/file`.

Telegram acceptance followed by a local archive or compare-and-swap conflict is an ambiguous side-effect boundary. Callers must reconcile rather than blindly retry. The bounded transient send retry policy can also duplicate a message after an ambiguous network failure.

`GET /health` reports the supported inbound and outbound kind lists plus `maxMediaBytes`, including when Telegram is disabled.

## Text fidelity

User-facing values are trimmed only to test whether they contain a non-whitespace character. Original text is retained and sent unchanged. Long replies are split at Telegram's UTF-16 limit so concatenating every chunk exactly reconstructs the source, including leading, trailing, and inter-chunk whitespace.

## Persistence and API

The SQLite database contains Telegram events and media, private and group session pointers, the durable polling cursor, opaque group IDs and chat aliases, the permanent member ledger, allowed group archives, reset work, and background-ingress batches. It contains no whitelist, observed-identity directory, application root, capability table, API credential, or bot token.

The loopback API covers event listing, media retrieval, binding, audio-oriented transcription records, replies, outbound documents and native media, abort/reset completion, group ingress, group-session updates and reconciliation, and group media preparation. See [Specification.md](./Specification.md) for exact route, persistence, validation, and failure semantics.
