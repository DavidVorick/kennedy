# kcode-tg-kennedy-bot Specification

## Library boundary

`kcode-tg-kennedy-bot` is a Rust library invoked by the Kennedy server binary. It long-polls Telegram with `teloxide`, persists transport work in SQLite, enforces fail-closed group transport security, and exposes a local HTTP work-queue API. It does not unlock Kennedy's credential vault, own or open Kennedy's user database, assign Kmap roots, construct prompts, invoke a model, inspect Chatend state, or grant application capabilities.

The host passes an optional `BotToken` directly in `Config`. With no token, the HTTP API remains available and reports Telegram disabled. A configured token is validated with `getMe` during startup, is never serialized or exposed by an API, has a redacted `Debug` representation, and is zeroized on drop.

The host supplies an `IdentitySink` implementation. The relay reports an `IdentityObservation` containing a numeric Telegram ID, current handle when known, and display name. It requests point-in-time `WhitelistSnapshot` values containing authorized numeric IDs. The private `/adduser` command is transported by the relay, but authorization, handle normalization and pinning, mutation, and application capability checks occur through the host's `request_add_user` implementation. Stable opaque group IDs are reported through `observe_group` so the host may attach application-owned state independently.

The relay package has standalone Cargo metadata and dependencies. Copying the `kcode-tg-kennedy-bot` directory to another repository is sufficient to build the library; the consumer supplies its own `IdentitySink` and startup configuration.

The directory is also a conforming managed Rust library. It contains root-level `Documentation.md`; the canonical package version is the literal root `[package].version` in `Cargo.toml`, and `Version.txt` is not used. All maintained files are ordinary UTF-8 text, and generated Cargo output must remain outside the directory.

## Configuration and startup

`Config` contains:

- `bind`: a literal IPv4 or IPv6 socket address for the local API;
- `database`: the SQLite database path;
- `allowed_origins`: exact browser origins allowed to call the API;
- `bot_token`: an optional `BotToken`;
- `identity_sink`: the host identity and authorization integration;
- `max_voice_bytes`: the nonzero byte limit used for every inbound and outbound media payload. The field name is retained for API compatibility even though it is the general Telegram media limit.

A configured bot token is validated before the listener is announced as ready. If Telegram validation fails, startup fails rather than exposing a nominally ready Telegram service.

`migrate_storage(path)` applies the idempotent SQLite migrations without starting HTTP serving or Telegram polling.

## Local API security boundary

The HTTP API has no bearer-token or user-authentication layer. Its authority boundary is therefore a strict local listener:

- `bind` must parse directly as a `SocketAddr`;
- the IP must be an IPv4 or IPv6 loopback address;
- wildcard, LAN, public, hostname, and malformed values are rejected;
- examples accepted are `127.0.0.1:4324` and `[::1]:4324`;
- examples rejected include `0.0.0.0:4324`, `[::]:4324`, `192.168.1.4:4324`, public addresses, and `localhost:4324`.

This prevents accidental direct exposure of private event data and effectful mutators on a machine with a public IP. CORS is not treated as authentication.

Browser requests receive an additional origin check. A request with an `Origin` header must contain exactly one origin and it must exactly match a configured `allowed_origins` value. A request carrying Fetch Metadata headers but no `Origin` is rejected. Host-native clients may omit both browser origin and Fetch Metadata headers. CORS advertises only configured origins and the `GET`, `POST`, and `OPTIONS` methods.

All responses passing through the API middleware receive `Cache-Control: no-store`, `Pragma: no-cache`, and `X-Content-Type-Options: nosniff`. Media responses also set `Cache-Control: no-store` directly.

Loopback isolation does not authorize untrusted local processes and does not protect against a public service that deliberately proxies the relay or has a server-side request-forgery path to arbitrary loopback URLs. A deployment must not reverse-proxy these routes onto a public listener. If non-loopback access is ever required, an explicit authenticated transport boundary must be added before changing the bind rule.

Request bodies are bounded to the configured media limit plus one MiB of multipart overhead. The relay does not log request or response bodies.

## Identity and private messages

Kennedy owns the whitelist, observed-identity directory, TOFU handle pinning, `/adduser` authority, and Kmap root assignments. The relay database contains none of those tables.

For each private message, the relay reports the sender's numeric ID, handle, and display name before requesting a whitelist snapshot. Unauthorized content is not archived. Authorized text, voice notes, arbitrary bounded documents, native photos, videos, animations, audio tracks, video notes, stickers, and `/reset` updates are accepted. Retained media bytes remain in the transport archive.

A document has no format allowlist. Its Telegram-provided file name, MIME type when present, caption, and original bounded bytes are retained. The document caption is exposed as the event's `text` value.

The private `/adduser @handle` UX calls the host. A forbidden outcome produces an administrator-only response; a successful outcome reports whether a numeric ID is already known. The relay itself never inserts, updates, or queries a user-management table.

Private active conversation pointers are keyed by numeric Telegram user ID. Binding an event records `processing_started_at`, the durable origin of its response deadline. Compare-and-swap rebinding is allowed only when the caller supplies the event's exact current conversation ID. Replies, resets, timeout transitions, transcription writes, media retrieval, and file delivery are designed for explicit host reconciliation.

## Polling cursor and failure policy

The relay persists the next Telegram polling offset in the singleton `telegram_polling_state` row. Each response from `getUpdates` is sorted by update ID. Updates older than the durable offset are skipped.

For every new update, the relay attempts bounded in-memory dispatch and then advances the durable offset. Dispatch rejection, malformed content, queue saturation, processor failure, or a processor panic is local to that update. The relay may deliberately lose such an informal chatbot update rather than block every later Telegram update. Reopening the database resumes from the persisted next offset.

The long-poll request itself retries after transport failure because no returned update batch has yet been accepted. Failure to persist the cursor causes polling to pause and retry rather than knowingly move the provider offset beyond the durable cursor.

Every finite Telegram provider operation retries only plausibly transient failures and makes at most five total attempts, including the first. `RetryAfter` is honored. Timeout, connection, network, and transient I/O errors use bounded backoff. Permanent Telegram API errors, migration responses, invalid JSON, non-transient I/O, SQLite failures, host failures, authorization failures, and invalid input are not retried. Media stream failure restarts a bounded download from the beginning.

Sends are inherently ambiguous when Telegram may have accepted a request before the client observed a network failure. Retrying may duplicate the message. This is accepted for the informal transport and is not represented as exactly-once delivery. No durable send outbox exists.

This is an at-most-once-leaning, availability-first transport contract, not guaranteed delivery. The database's unique update and source-message identities still suppress accepted duplicates and stale message revisions.

## Bounded principal dispatch

Accepted updates enter bounded principal queues:

- private messages and edits are keyed by numeric Telegram user ID;
- ordinary group messages and edits are keyed by Telegram chat ID plus numeric sender ID;
- membership, migration, anonymous-group, and other group-control updates use a group-control key;
- otherwise unkeyed updates share one bounded fallback key.

Each key is processed in local FIFO order. Independent keys may run concurrently. A slow private user or one slow `(group, user)` stream therefore does not intentionally block unrelated principals.

No `IdentitySink` callback, Telegram request, media download, retry sleep, or other host callback executes while the shared SQLite mutex is held. The relay stages provider/host work around short local reads, writes, and compare-and-swap transitions.

The current bounds are 32 waiting updates per principal, 256 active principal keys, and 16 concurrently executing processors. Saturated keys or a saturated active-key table drop the new update locally. Worker errors and panics are logged and the worker continues with later queued work. Idle keys are removed without leaving an enqueue race or retaining an idle task.

Ordering is guaranteed only within a dispatch key. Group-control work and a particular group user's work have different keys and may overlap. SQLite transitions and the independent group eligibility check remain the durable security authority.

## Stable group identity and historical membership

Every observed Telegram group receives a random stable opaque `group_id`. Current and former Telegram chat IDs map to it through an alias table, so a basic-group-to-supergroup migration retains the member ledger, security state, cursors, messages, and group-user session pointers. No application root ID is stored in the relay.

The bot must be a group administrator. Polling explicitly requests `message`, `edited_message`, `my_chat_member`, and `chat_member` updates. Administrator loss fails closed.

For each group, the relay maintains a permanent human membership ledger. It updates the ledger from Telegram's administrator list, membership updates, join and leave service metadata, and sender envelopes. A member who leaves or is kicked remains in the ledger with that terminal membership status. The bot itself and Telegram's anonymous group identity are never inserted as human users. Every real human observation is sent to the host even when the person has departed.

Before reading group message content, the relay performs this eligibility test:

1. Telegram confirms that the bot is currently an owner or administrator.
2. Telegram's current member count exactly equals the number of ledger entries currently marked member, administrator, or creator, plus the bot.
3. Every numeric human ID ever recorded in that group's ledger appears in the host's latest whitelist snapshot, including left and kicked members.

Passing all checks sets the group to `allowed`. Any failed check sets it to `quarantined` with roster-completeness metadata and a reason. Quarantine is reversible: eligibility is recomputed on later updates and messages, and a group becomes allowed once the roster is complete and the host has whitelisted every historical identity. Rediscovering or renaming a chat alone does not bypass quarantine.

Telegram has no bot method that enumerates all ordinary members in an existing group. Its available membership methods provide administrator lists, a count, and lookup of an already known user. The reliable strict onboarding sequence is therefore to create a new group with the bot, promote it to administrator, and then add members so each join is observed. A pre-existing group remains quarantined until every current human has at least been observed and the count reconciles. Membership that predates the bot and departed before it joined cannot be reconstructed.

## Quarantined message handling

Telegram necessarily delivers and deserializes an update envelope before application code can identify its chat and sender. Within the relay, a quarantined group message is handled only far enough to record sender, membership, and service metadata and recompute eligibility. If the group is still ineligible, the function returns before:

- testing mentions, commands, replies, or reset instructions;
- reading or deriving message or caption text;
- selecting photo renditions or downloading any retained media;
- producing validation feedback;
- storing message content, media, events, or background-ingress rows;
- exposing content to Kennedy's browser or model pipeline.

This prevents unauthorized group content from entering the application's durable content archive or prompt pipeline.

## Allowed group transport

An allowed group message invokes Kennedy when it mentions the bot handle, replies to a bot message, or is a scoped `/reset`. Voice notes and video notes invoke by reply. Caption-bearing documents, photos, videos, animations, and audio may invoke by caption mention or reply; stickers may invoke by reply. Each event carries relay identity fields, its stable `groupId`, and recent transport context. The relay never decorates an event with user or group root IDs. The host obtains those separately and joins them by numeric user ID and stable group ID.

Group conversation pointers are keyed by `(group_id, telegram_user_id)` and never alter a private-DM or other-group pointer. Each pointer tracks passive-context and invocation cursors. Every allowed message is archived once; bot replies and outbound media are archived with their source conversation. Media retrieval and preparation endpoints allow host-supplied processing to be reused across sessions.

After the 51st group message since a user's last invocation, the relay atomically detaches that pair's active pointer and records a silent-reset range. More than 100 non-invocation messages beyond the covered cursor queues the oldest 80 as a durable background-ingress batch, leaving 20 unbatched. Recent context, session updates, background-ingress creation, and edit refreshes use one shared message projection containing existing identity/text fields plus `kind`, `mimeType`, `fileName`, `durationSeconds`, `preparedText`, `preparationModel`, `documentFormat`, `preparationTruncated`, and `hasMedia`. Queue payloads carry `groupId`, participants' Telegram IDs, handles and display names, and group title; they contain no Kmap roots.

### Orphaned group-session detachment

A host that discovers a permanently missing downstream conversation may call:

`POST /api/v1/group-sessions/{conversation_id}/detach-if-current`

The path value must be a UUID. The JSON body is:

```json
{
  "groupId": "opaque-relay-group-id",
  "telegramUserId": 42
}
```

The relay clears `current_conversation_id` only when the stored `(group_id, telegram_user_id, current_conversation_id)` triple exactly matches the request. A missing, already detached, or rebound pointer returns `409 state_conflict`. A successful detach does not delete messages, cursors, reset ranges, events, ingress batches, membership history, group identity, or any other user's pointer.

The host must use this compare-and-swap endpoint rather than blindly clearing a group or user session. The relay does not need to understand why the downstream conversation is missing.

## Bidirectional media

### Inbound Telegram media

Authorized private and allowed-group Telegram media uses these transport kinds:

- existing: `voice`, `document`;
- native: `photo`, `video`, `animation`, `audio`, `video_note`, `sticker`.

A generic Telegram document remains `document` regardless of an image, video, or audio MIME type. Native animation is classified before Telegram's compatibility document field.

Every media kind is accepted only when its declared and streamed size does not exceed `max_voice_bytes`. The relay rejects a declared oversize before `getFile`, restarts transiently failed bounded downloads, and enforces the limit again while streaming. One oversized or malformed update remains local to its principal stream.

Private event and group-message JSON continue to use only the essential fields `kind`, `text`, `mimeType`, `fileName`, `durationSeconds`, and `hasMedia` where applicable. No generic provider-metadata object or width, height, performer, title, emoji, animation-flag, or video-flag field is added.

Text semantics are:

- photo, video, animation, and audio retain the exact caption when present;
- sticker retains Telegram's associated emoji in `text` when present;
- video note uses no synthetic text;
- voice and document behavior remains compatible;
- group rows use an empty string when native media has no provider text.

For a photo array, the relay chooses greatest checked pixel area, then greatest declared file size, then earliest stable provider order. It retains only that Bot API rendition and does not call it the sender's original upload.

MIME behavior uses Telegram's value when present. Fallbacks are `image/jpeg` for photo, `video/mp4` for video and video note, `image/webp`/`application/x-tgsticker`/`video/webm` for static/animated/video stickers, and a recognized safe filename extension for animation or audio. An uncertain animation or audio type is `application/octet-stream`. Telegram file paths are never exposed as file names; absent provider names receive stable safe names based on message identity.

For private events, `GET /api/v1/events` exposes essential metadata and:

`GET /api/v1/events/{event_id}/media`

returns the exact retained Bot API bytes with the recorded or honest fallback MIME type.

For archived group messages:

`GET /api/v1/group-messages/{chat_id}/{message_id}/media`

returns the exact retained bytes. Both routes retain the existing `no-store`, `no-cache`, and `nosniff` protections.

The host may elect whether to retrieve, retain elsewhere, extract, or expose a file to Kennedy. The relay stores transport bytes and association metadata; it does not itself create Kweb objects or infer file meaning.

The private transcription record route accepts only the audio-oriented `voice`, `audio`, and `video_note` kinds. Group preparation accepts every retained media kind. Both store host-supplied processing; the relay invokes no transcription, extraction, or vision provider.

### Outbound generic documents

The host may send a document in the active event's Telegram chat with:

`POST /api/v1/events/{event_id}/file`

The request is `multipart/form-data` with exactly these fields:

- `conversationId` — required UUID matching the event's current binding;
- `file` — required, nonempty binary part;
- `fileName` — optional override; otherwise the binary part must have a file name;
- `caption` — optional and preserved verbatim when nonempty;
- `complete` — optional `true`, `false`, `1`, or `0`; false by default.

Unknown or duplicate multipart fields are rejected. The effective file name must be nonempty, path-free, control-character-free, and at most 255 characters. The binary part's content type, when present, must be nonempty, control-character-free, and at most 255 characters; otherwise `application/octet-stream` is reported. Captions are limited to 1024 UTF-16 code units. The file bytes must not exceed `max_voice_bytes`, and empty files are rejected.

The event must exist, must not already be complete, and must still be bound to the supplied conversation. Group files reply to the invoking Telegram message when possible and are archived as Kennedy-authored document messages with their bytes, name, content type, caption, source conversation, and stable group association.

If `complete=true`, the relay marks the event complete after Telegram accepts the file, but only if its conversation binding still matches. If delivery succeeds and the later local compare-and-swap fails, the API reports a conflict even though Telegram may already contain the document. Callers must reconcile this side-effect boundary and must not assume that retrying an ambiguous request is duplicate-free.

For `complete=false`, the event remains active so the host may send additional files or a later text reply. The endpoint is intentionally a simple bounded send path rather than a guaranteed-delivery outbox.

### Outbound native media

The host may send native media with:

`POST /api/v1/events/{event_id}/media`

The request is `multipart/form-data` with exactly:

- `conversationId` — required UUID matching the event's current binding;
- `kind` — required `photo`, `video`, `animation`, `audio`, `video_note`, or `sticker`;
- `file` — required nonempty binary part;
- `fileName` — optional validated override, then the part name, then a safe synthesized name;
- `caption` — optional only for photo, video, animation, and audio;
- `complete` — optional `true`, `false`, `1`, or `0`; false by default.

Unknown, duplicate, rejected caption, malformed, empty, and oversized fields use the existing `invalid_request` response style. No width, height, duration, performer, title, or emoji multipart field is accepted.

Kinds map exactly to `sendPhoto`, `sendVideo`, `sendAnimation`, `sendAudio`, `sendVideoNote`, and `sendSticker`. The binary input uses an in-memory Telegram file with the effective safe filename. Caller-declared multipart MIME metadata is retained in a local outbound group archive, but the relay does not claim teloxide transmits it unchanged.

The route never falls back to `sendDocument`. A caller wanting document semantics must use `/file`.

Event existence, incomplete state, conversation binding, group reply behavior, `allow_sending_without_reply`, group archive association, `complete`, and post-send compare-and-swap semantics match `/file`. Group archives retain the native kind, outbound bytes, effective filename, recorded MIME, exact caption or empty text, returned message identity/date and duration when present, source conversation, stable group ID, and invoking-message reply association. Private sends add no separate application-domain archive.

Any Telegram success followed by archive or completion failure is an ambiguous external side effect. The API surfaces the failure even though Telegram may contain the media.

## Capability discovery

`GET /health` retains `service`, `status`, and `telegram`, and additively returns:

```json
{
  "capabilities": {
    "inboundMediaKinds": [
      "voice", "document", "photo", "video",
      "animation", "audio", "video_note", "sticker"
    ],
    "outboundMediaKinds": [
      "document", "photo", "video",
      "animation", "audio", "video_note", "sticker"
    ],
    "maxMediaBytes": 20971520
  }
}
```

The byte limit is the configured `max_voice_bytes`. Supported capability lists remain present when Telegram is disabled.

## Text fidelity

User-facing nonempty text is validated with trimming only to determine whether it contains a non-whitespace character. The original text is retained and sent verbatim for replies, context warnings, abort notices, reset confirmations, transcriptions, and group media preparation where applicable.

Telegram text is split only when necessary. Chunk boundaries are computed in UTF-16 code units to respect Telegram's limit, and concatenating the emitted chunks exactly reconstructs the original string. Leading, trailing, and inter-chunk whitespace is not trimmed or synthesized.

## Storage

The relay SQLite database owns:

- private transport sessions;
- Telegram events and original event media;
- the durable next polling offset;
- stable opaque groups and Telegram chat-ID aliases;
- the permanent historical group-member ledger;
- group-user sessions and reset ranges;
- allowed group message and media archives;
- background-ingress batches and transport cursors.

It does not own whitelist entries, observed-identity directory records, `can_add_users` or any other application capability, user roots, group roots, system roots, API keys, or bot tokens.

Migrations are idempotent. A legacy permanent-blacklist schema is converted to fail-closed `quarantined` rows with `roster_complete = false`. The 0.2.0 `telegram_events` table is rebuilt only to expand its kind constraint; every row, ID, update/revision ID, state, binding, timestamp, transcription, session kind, group context/ID, byte blob, and metadata field is preserved, all indexes are recreated, and foreign keys are checked. The private legacy `voice_bytes` column continues to store general event media. `telegram_group_messages` is not rebuilt because it has no kind constraint.

## HTTP API summary

The local API exposes:

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

These routes expose only Telegram transport state and transitions. User provisioning beyond the transported `/adduser` callback, application-root management, model execution, and application capabilities remain host responsibilities.
