# Changelog

## 0.3.0

- Add bounded native Telegram photo, video, animation, audio, video-note, and sticker ingress with essential transport metadata, deterministic largest-photo rendition selection, existing-route byte retrieval, and unchanged generic-document classification.
- Add `POST /api/v1/events/{event_id}/media` for explicit native Telegram delivery. The six native kinds map to their corresponding Bot API methods, never silently fall back to `sendDocument`, preserve group reply/archive behavior, and share the existing event binding and completion fence.
- Add health capability discovery for inbound/outbound media kinds and the configured general media byte limit without changing the source-compatible `Config.max_voice_bytes` field.
- Migrate the `telegram_events.kind` constraint idempotently from the actual 0.2.0 schema while preserving all event rows, state, media, bindings, revision data, group context, and indexes.
- Move every `IdentitySink` callback outside the shared SQLite mutex and add deterministic concurrency regression tests for blocked whitelist and group-observation callbacks.
- Add centralized five-attempt transient Telegram retries for finite provider reads, sends, file lookup, and restarted downloads. Permanent errors stop immediately; ambiguous send retries may duplicate and no durable outbox is claimed.
- Use one media-complete group-message JSON projection for session context, passive background ingress, and edit refreshes so retained media is no longer collapsed into text-only snapshots.

## 0.2.0

- Persist Telegram's next polling offset in SQLite, sort updates by update ID, and advance the durable cursor after bounded in-memory enqueue. A failed, malformed, or saturated principal stream may lose an informal chatbot update, but it can no longer freeze every later Telegram update.
- Dispatch accepted updates through bounded per-principal workers with independent cross-principal concurrency, local ordering, queue saturation isolation, and worker cleanup. Private users and group-user conversations no longer perform slow processing in the global polling loop.
- Accept arbitrary bounded Telegram documents alongside text and voice notes, preserving original bytes, captions, file names, MIME metadata, ownership context, and event or group-session association for host-side storage and processing.
- Add bounded multipart outbound document delivery for an active event, including optional captions, safe path-free file names, group reply/archive integration, and optional atomic event completion after Telegram accepts the file.
- Require the relay HTTP API to bind to a literal IPv4 or IPv6 loopback address, rejecting wildcard, LAN, public, hostname, and malformed bind values so an internet-facing host cannot accidentally expose private transport data or mutators directly.
- Add a compare-and-swap group-session detach endpoint so a host can clear a permanently orphaned conversation pointer without clearing a newer binding or affecting another group user.
- Preserve exact response text and whitespace across UTF-16-aware Telegram message chunks.
- Classify Telegram request, download, and processing failures without logging raw provider errors that could contain token-bearing request URLs.

## 0.1.5

- Replace the generated-source build overlay with directly maintained ordinary Rust source.
- Move edit and revision reconciliation into `src/edit_revisions.rs` and represent its schema changes through `migrations/005_edit_revisions.sql`.
- Remove the build script, patched library target, generated include fragments, and empty source-snapshot test while preserving the public API and transport behavior.

## 0.1.4

- Preserve each event's original Telegram update ID as its immutable queue and deduplication identity while recording the latest accepted edit update separately.
- Ignore duplicate and out-of-order message revisions, preserve per-user queue order, and prevent replay of an original update from recreating work for the same Telegram message.
- Invalidate processing background-ingress batches whose captured message is edited; stale completion is rejected with a state conflict, while pending snapshots continue to refresh in place.
- Exact-pin the audited direct dependency set used by this credential-bearing transport boundary.

## 0.1.3

- Cancel and detach an in-flight group response when an earlier message in its captured context is edited, preventing a response computed from stale context from being delivered. Such events complete with `context_edited`.
- Suppress validation and unsupported-content feedback for edited group updates; edits now reconcile transport state silently rather than producing unrelated bot messages.

## 0.1.2

- Allow group responses, timeout notices, and reset confirmations to be delivered without their reply target when Telegram reports that the source message no longer exists.
- Reconcile authorized private and allowed-group message edits without creating duplicate invocations. Pending work is refreshed to the edited revision; in-flight work is completed as `source_edited` and its matching session binding is detached so a stale response cannot later be submitted.
- Replace edited group archive content and media metadata completely, invalidate derived media preparation when its source changes, and refresh affected pending background-ingress snapshots.
- Continue to treat ordinary Telegram deletions as unobservable after ingress because the ordinary Bot API exposes no corresponding deletion update.
