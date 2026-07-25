# kcode-tg-kennedy-bot

`kcode-tg-kennedy-bot` is a host-integrated Telegram transport library. It long-polls Telegram, stores private and group transport state in SQLite, enforces fail-closed group security, and serves a loopback HTTP queue for a Kennedy frontend. It deliberately does not own Kennedy users, whitelist entries, application capabilities, secrets, prompts, model execution, Kmap roots, or downstream file/object storage.

The crate is a standalone Rust 2024 package. Its public Rust crate name is `kcode_tg_kennedy_bot`.

## Host integration

The host supplies an optional Telegram bot token and an implementation of `IdentitySink`:

- `observe_identity` receives each Telegram identity observed by the relay.
- `whitelist` returns the authorized numeric Telegram user IDs at that moment.
- `request_add_user` authorizes and performs the private `/adduser @handle` workflow outside the relay.
- `observe_group` receives each stable opaque group ID so the host can attach application-owned state independently.

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

`max_voice_bytes` is retained as a source-compatible field name, but it is the byte limit for every inbound and outbound media payload. It must be nonzero.

Passing `None` as `bot_token` keeps the local API available and reports Telegram as disabled. A configured token is rejected when empty, validated with Telegram before readiness, redacted from `Debug`, never serialized, and zeroized on drop.

Call `migrate_storage(path)` to apply the idempotent SQLite migrations without starting HTTP serving or Telegram polling.

## Local API security

The API has no bearer-token or per-user authentication layer and must remain host-local. `Config.bind` must be a literal IPv4 or IPv6 loopback socket address such as `127.0.0.1:4324` or `[::1]:4324`. Startup rejects wildcard, LAN, public, hostname, and malformed values, including `0.0.0.0`, `[::]`, and `localhost`.

This protects a public-IP host from accidentally listening for private event data and effectful mutations on an internet-facing interface. It does not authorize untrusted local processes and does not make a public reverse proxy or an SSRF-capable public service safe. Do not proxy the relay API to the open internet. Add an explicit authenticated transport boundary before changing the loopback-only rule.

Browser requests with an `Origin` header must match one configured `allowed_origins` value exactly. Requests carrying Fetch Metadata headers without `Origin` are rejected. Native host clients may omit both. CORS is defense in depth, not authentication.

Responses use `Cache-Control: no-store`, `Pragma: no-cache`, and `X-Content-Type-Options: nosniff`. Request bodies are bounded to the configured media limit plus multipart overhead, and request or response bodies are not intentionally logged.

## Identity and group-security contract

The host is authoritative for identity observations, the whitelist, handle pinning, `/adduser` authorization, and application-owned roots. The relay uses numeric Telegram user IDs for authorization decisions and never stores the bot token or application authorization tables in its database.

Groups have random stable opaque IDs that survive Telegram basic-group to supergroup migrations. The relay permanently retains every human identity it has observed in a group's membership ledger, including departed and kicked members.

A group is allowed only while all of these conditions hold:

1. Telegram confirms that the bot is currently an administrator or owner.
2. Telegram's member count matches the observed active-human ledger plus the bot.
3. Every human ever recorded for the group is in the host's current whitelist.

Any failed condition quarantines the group. While quarantined, the relay handles only sender and membership metadata needed to improve the ledger; it does not inspect invocation text, download media, archive message content, or expose content to Kennedy. Eligibility is recomputed on later updates, so quarantine is reversible after the roster becomes complete and every historical identity is authorized.

Telegram cannot enumerate all ordinary members of an existing group. Reliable strict onboarding therefore starts with a new group: add the bot, promote it to administrator, and then add human members so their joins are observed.

## Availability-first polling and dispatch

The next Telegram polling offset is durable in SQLite. Returned updates are sorted by update ID, and updates older than the durable offset are skipped.

Each new update is offered to a bounded in-memory principal queue before the cursor advances. A malformed update, saturated queue, processor error, or processor panic may lose that informal chatbot update, but it cannot deliberately freeze every later Telegram update. Failure to persist the cursor pauses polling rather than knowingly advancing beyond durable state.

Private work is keyed by numeric user ID. Ordinary group work is keyed by `(Telegram chat ID, numeric sender ID)`, while group-control updates use a group-control key. Each key is FIFO; independent keys may execute concurrently. Current limits are 32 waiting updates per principal, 256 active principal keys, and 16 concurrently executing processors.

This is an at-most-once-leaning, availability-first transport, not a guaranteed-delivery queue. Existing update and source-message uniqueness still suppresses accepted duplicates and stale revisions.

Finite Telegram operations use one bounded retry policy: no more than five total attempts, `RetryAfter` honored exactly, bounded backoff for plausibly transient network/I/O failures, and immediate failure for permanent API or local errors. Long polling remains a continuous loop rather than consuming five lifetime attempts. An ambiguous send retry may duplicate a Telegram message; no durable outbox or exactly-once guarantee is provided.

`IdentitySink` callbacks, Telegram calls, download work, and retry sleeps occur outside the shared SQLite mutex. Database ownership is limited to short reads, writes, and compare-and-swap transitions.

## Durable transport behavior

Authorized private text, voice notes, arbitrary bounded documents, native photos, videos, animations, audio tracks, video notes, stickers, and `/reset` updates are queued in per-user order. Binding an event records the durable start of its response deadline. Conversation rebinding uses compare-and-swap semantics. Reply, reset, timeout, transcription, media retrieval, file delivery, and abort transitions require explicit host reconciliation.

Allowed group messages invoke Kennedy when they mention the bot, reply to a bot message, or contain a scoped `/reset`. Group conversation pointers are keyed by stable group ID and numeric Telegram user ID, separate from private and other group sessions. The database also retains allowed group context, media, background-ingress batches, reset ranges, and transport cursors.

If the host discovers that a downstream group conversation is permanently missing, it may compare-and-swap detach only the matching pointer:

`POST /api/v1/group-sessions/{conversation_id}/detach-if-current`

with:

```json
{
  "groupId": "opaque-relay-group-id",
  "telegramUserId": 42
}
```

The path must be a UUID. The relay clears only the exact `(groupId, telegramUserId, conversationId)` match. Missing, detached, or rebound state returns `409 state_conflict`; messages, events, cursors, resets, membership, and other users are preserved.

## Inbound and outbound media

Authorized Telegram voice notes, documents, photos, videos, animations, audio tracks, video notes, and stickers share one bounded download path. A document has no format allowlist and remains `document` even when its MIME type starts with `image/`, `video/`, or `audio/`. The relay enforces `max_voice_bytes` before download when Telegram declares a size and again while streaming. It stores the retained Bot API bytes, essential file/MIME/duration metadata, exact caption or sticker emoji where applicable, owner/session association, and transport kind.

Telegram photo messages contain provider-generated renditions. The relay retains one: greatest pixel area, then greatest declared size, then earliest provider order. This is not guaranteed to be the sender's original upload.

Animation is classified before its backward-compatible document representation. Missing animation or audio MIME types are inferred only from a recognized safe filename extension; otherwise the relay reports `application/octet-stream`. Photo, video, video-note, and sticker fallbacks follow their documented Bot API formats.

Private event metadata is available from `GET /api/v1/events`, with bytes from:

`GET /api/v1/events/{event_id}/media`

Allowed group metadata appears in event/context payloads, with bytes from:

`GET /api/v1/group-messages/{chat_id}/{message_id}/media`

The host elects whether to retrieve a file, retain it in an object store, extract it, expose it to Kennedy, or ignore it. The relay does not infer file meaning or create Kweb objects.

The host can send a generic document in an active event's Telegram chat through:

`POST /api/v1/events/{event_id}/file`

The request is `multipart/form-data` with:

- `conversationId`: required UUID matching the active event binding;
- `file`: required nonempty binary part;
- `fileName`: optional override, otherwise the file part needs a file name;
- `caption`: optional, preserved verbatim when nonempty;
- `complete`: optional `true`, `false`, `1`, or `0`; defaults to false.

Duplicate or unknown fields are rejected. File names must be path-free, control-character-free, nonempty, and at most 255 characters. Content types are similarly bounded. Captions are limited to 1024 UTF-16 code units. Files are limited by `max_voice_bytes`.

Group deliveries reply to the invoking message when possible and are archived with the original bytes and metadata. `complete=true` completes the event only after Telegram accepts the file and only if the conversation binding still matches. Telegram delivery followed by a local compare-and-swap conflict is an ambiguous side-effect boundary; callers must reconcile rather than blindly retry. `complete=false` leaves the event active for more files or a later text reply.

Native media uses:

`POST /api/v1/events/{event_id}/media`

It uses the same binding, size, filename, group-reply, archive, `complete`, and ambiguity semantics, with one additional required field:

- `kind`: exactly `photo`, `video`, `animation`, `audio`, `video_note`, or `sticker`.

Only `conversationId`, `kind`, `file`, `fileName`, `caption`, and `complete` are accepted. Captions are allowed for photo, video, animation, and audio, and rejected for video notes and stickers. The kind selects `sendPhoto`, `sendVideo`, `sendAnimation`, `sendAudio`, `sendVideoNote`, or `sendSticker`. Native delivery never silently falls back to `sendDocument`; use `/file` deliberately for document semantics.

The transport stores caller-declared multipart content type in an outbound group archive but does not claim teloxide transmits that metadata unchanged. A Telegram success followed by archive or completion failure is an ambiguous external side effect.

Group context, session updates, passive background-ingress batches, and edit refreshes share one projection containing `kind`, `mimeType`, `fileName`, `durationSeconds`, `preparedText`, `preparationModel`, `documentFormat`, `preparationTruncated`, and `hasMedia`. The group preparation route accepts every retained media kind; interpretation remains host-supplied.

The private transcription route accepts the audio-oriented `voice`, `audio`, and `video_note` kinds. It stores host-provided text and model metadata; the relay performs no transcription itself.

`GET /health` additively reports `capabilities.inboundMediaKinds`, `capabilities.outboundMediaKinds`, and the configured `capabilities.maxMediaBytes`. Capabilities remain present while `telegram` is `disabled`.

## Text fidelity

Nonempty user-facing text is validated by trimming only to test whether it contains a non-whitespace character. The original value is stored and sent unchanged.

Long replies are split by Telegram's UTF-16 limit. Concatenating the chunks exactly reconstructs the source string; leading, trailing, and inter-chunk whitespace is neither trimmed nor synthesized.

## HTTP API

The transport API is rooted at `/api/v1`. Its current routes are:

- `GET /health`
- `GET /api/v1/events`
- `GET /api/v1/events/{event_id}/media`
- `POST /api/v1/events/{event_id}/file`
- `POST /api/v1/events/{event_id}/media`
- `POST /api/v1/events/{event_id}/bind`
- `POST /api/v1/events/{event_id}/transcription`
- `POST /api/v1/events/{event_id}/reply`
- `POST /api/v1/events/{event_id}/abort`
- `POST /api/v1/events/{event_id}/reset-completed`
- `GET /api/v1/group-ingress`
- `POST /api/v1/group-ingress/{batch_id}/complete`
- `GET /api/v1/group-sessions/updates`
- `POST /api/v1/group-sessions/{conversation_id}/detach-if-current`
- `POST /api/v1/group-sessions/{conversation_id}/context-ack`
- `POST /api/v1/group-sessions/{conversation_id}/silent-reset-completed`
- `GET /api/v1/group-messages/{chat_id}/{message_id}/media`
- `POST /api/v1/group-messages/{chat_id}/{message_id}/preparation`

Detailed persistence, validation, side-effect, and failure semantics are in `Specification.md`.

## Managed-library maintenance

The literal `[package].version` in `Cargo.toml` is the canonical managed-library version. `Version.txt` is neither required nor used. Update the manifest version for a release, and keep every file in this directory as ordinary UTF-8 text; generated build output and binary artifacts must remain outside the managed-library directory.

The standard managed check runs dependency fetch, formatting, build, Clippy with warnings denied, unit and integration tests, and documentation tests. SQLite migrations under `migrations/` are compiled into the library and must remain present.
