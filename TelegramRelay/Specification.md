# Telegram Relay Specification

## Library boundary

The Telegram relay is a Rust library invoked by the Kennedy server binary. It long-polls Telegram with `teloxide`, persists transport work, enforces group transport security, and exposes a loopback HTTP queue for the browser frontend. It does not unlock Kennedy's credential vault, own or open Kennedy's user database, assign Kmap roots, construct prompts, invoke a model, inspect Chatend state, or grant application capabilities.

The host passes an optional `BotToken` directly in `Config`. With no token, the HTTP API remains available and reports Telegram disabled. A configured token is validated with `getMe` during startup, is never serialized or exposed by an API, has a redacted `Debug` representation, and is zeroized on drop.

The host supplies an `IdentitySink` implementation. The relay reports an `IdentityObservation` containing a numeric Telegram ID, current handle when known, and display name. It requests point-in-time `WhitelistSnapshot` values containing authorized numeric IDs. The private `/adduser` command is transported by the relay, but authorization, handle normalization/pinning, mutation, and application capability checks occur through the host's `request_add_user` implementation. Stable opaque group IDs are reported through `observe_group` so the host may attach application-owned state independently.

The relay package has standalone Cargo metadata and dependencies. Copying the `TelegramRelay` directory to another repository is sufficient to build the library; the consumer supplies its own `IdentitySink` and startup configuration.

## Identity and private messages

Kennedy owns the whitelist, observed-identity directory, TOFU handle pinning, `/adduser` authority, and Kmap root assignments in `kennedy-users.sqlite3`. The relay database contains none of those tables.

For each private message, the relay reports the sender's numeric ID, handle, and display name before requesting a whitelist snapshot. Unauthorized content is not archived. Authorized text, voice note, supported document, and `/reset` updates are processed in per-user order. Supported documents include PDF, DOCX, spreadsheet, CSV, and text formats within the configured byte limit. Original voice-note and document bytes remain in the transport archive.

The private `/adduser @handle` UX calls the host. A forbidden outcome produces an administrator-only response; a successful outcome reports whether a numeric ID is already known. The relay itself never inserts, updates, or queries a user-management table.

Private active conversation pointers are keyed by numeric Telegram user ID. Binding an event records `processing_started_at`, the durable origin of its 30-minute response deadline. Compare-and-swap rebinding is allowed only when the caller supplies the event's exact current conversation ID. Replies, resets, timeouts, and media behavior remain durable and idempotent.

## Stable group identity and historical membership

Every observed Telegram group receives a random stable opaque `group_id`. Current and former Telegram chat IDs map to it through an alias table, so a basic-group-to-supergroup migration retains the member ledger, security state, cursors, messages, and group-user session pointers. No application root ID is stored in the relay.

The bot must be a group administrator. Polling explicitly requests `message`, `edited_message`, `my_chat_member`, and `chat_member` updates. Administrator loss immediately fails closed.

For each group, the relay maintains a permanent human membership ledger. It updates the ledger from Telegram's administrator list, membership updates, join/leave service metadata, and sender envelopes. A member who leaves or is kicked remains in the ledger with that terminal membership status. The bot itself and Telegram's anonymous group identity are never inserted as human users. Every real human observation is sent to the host even when the person has departed.

Before reading group message content, the relay performs this eligibility test:

1. Telegram confirms that the bot is currently an owner or administrator.
2. Telegram's current member count exactly equals the number of ledger entries currently marked member/administrator/creator plus the bot.
3. Every numeric human ID ever recorded in that group's ledger appears in the host's latest whitelist snapshot, including left and kicked members.

Passing all checks sets the group to `allowed`. Any failed check sets it to `quarantined` with roster-completeness metadata and a reason. Quarantine is reversible: eligibility is recomputed on later updates/messages, and a group becomes allowed once the roster is complete and the host has whitelisted every historical identity. Rediscovering or renaming a chat alone does not bypass quarantine.

Telegram has no bot method that enumerates all ordinary members in an existing group. Its available membership methods provide administrator lists, a count, and lookup of an already known user. Therefore the reliable strict onboarding sequence is to create a new group with the bot, promote it to administrator, and then add members so each join is observed. A pre-existing group remains quarantined until every current human has at least been observed and the count reconciles. Membership that predates the bot and departed before it joined cannot be reconstructed.

## Quarantined message handling

Telegram necessarily delivers and deserializes an update envelope before application code can identify its chat and sender. Within the relay, a quarantined group message is handled only far enough to record sender/membership/service metadata and recompute eligibility. If the group is still ineligible, the function returns before:

- testing mentions, commands, replies, or reset instructions;
- reading or deriving message/caption text;
- downloading voice notes or documents;
- producing validation feedback;
- storing message content, media, events, or background-ingress rows;
- exposing content to Kennedy's browser/model pipeline.

This is the strongest content-isolation boundary available to a Bot API client and prevents unauthorized group text from entering any application prompt or durable content archive.

## Allowed group transport

An allowed group message invokes Kennedy when it mentions the bot handle, replies to a bot message, or is a scoped `/reset`. Voice notes invoke by reply; documents may invoke by caption mention or reply. Each event carries relay identity fields, its stable `groupId`, and recent transport context. The relay never decorates an event with user or group root IDs. Kennedy's frontend obtains those separately from Kennedy's directory API and joins them by numeric user ID and stable group ID.

Group conversation pointers are keyed by `(group_id, telegram_user_id)` and never alter a private-DM or other-group pointer. Each pointer tracks passive-context and invocation cursors. Every allowed message is archived once; bot replies are archived with their source conversation. Media fetch/preparation endpoints allow a stored voice transcription or document extraction to be reused across sessions.

After the 51st group message since a user's last invocation, the relay atomically detaches that pair's active pointer and records a silent-reset range. More than 100 non-invocation messages beyond the covered cursor queues the oldest 80 as a durable background-ingress batch, leaving 20 unbatched. Queue payloads carry transport fields, `groupId`, participants' Telegram IDs/handles/display names, and group title; they contain no Kmap roots.

## Storage and host API

The relay SQLite database owns:

- private transport sessions;
- Telegram events and original event media;
- stable opaque groups and Telegram chat-ID aliases;
- the permanent historical group-member ledger;
- group-user sessions and reset ranges;
- allowed group message/media archives;
- background-ingress batches and transport cursors.

It does not own whitelist entries, observed-identity directory records, `can_add_users` or any other application capability, user roots, group roots, system roots, API keys, or bot tokens.

`migrate_storage(path)` applies transport migrations idempotently. A legacy permanent-blacklist schema is converted to fail-closed `quarantined` rows with `roster_complete = false`; existing IDs, aliases, members, messages, sessions, resets, cursors, and events are preserved. The loopback API exposes only transport health, event/media/binding/reply/reset/abort transitions, group ingress, and group-session context operations.
