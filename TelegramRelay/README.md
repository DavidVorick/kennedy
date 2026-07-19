# Kennedy Telegram Relay

`kennedy-telegram-relay` is a transplantable Rust library for Telegram transport. It owns Telegram polling, message/media persistence, group membership history, group-security gating, and its loopback work-queue API. It deliberately owns no users, whitelist entries, application capabilities, secrets vault, or Kmap roots.

The directory can be copied to another repository and built as its own Cargo package: its manifest contains no workspace-inherited package metadata or dependencies.

## Host contract

The host passes the bot token when calling `serve` and implements `IdentitySink`:

- `observe_identity` receives the numeric Telegram ID, current handle when present, and display name.
- `whitelist` returns a point-in-time set of authorized numeric Telegram IDs.
- `request_add_user` lets the host authorize and execute the private `/adduser` UX. The relay never changes a user store itself.
- `observe_group` reports the relay's stable opaque group ID. The host can independently associate that ID with application-owned state such as a Kmap root.

```rust,no_run
use std::{path::PathBuf, sync::Arc};
use kennedy_telegram_relay::{BotToken, Config, IdentitySink};

async fn run(directory: Arc<dyn IdentitySink>, token: String) -> anyhow::Result<()> {
    kennedy_telegram_relay::serve(Config {
        bind: "127.0.0.1:4324".into(),
        database: PathBuf::from("telegram.sqlite3"),
        allowed_origins: vec!["http://127.0.0.1:4321".into()],
        bot_token: Some(BotToken::new(token)?),
        identity_sink: directory,
        max_voice_bytes: 20 * 1024 * 1024,
    })
    .await
}
```

Passing `None` as `bot_token` keeps the loopback API available while disabling Telegram polling. `BotToken` rejects empty values, redacts its `Debug` representation, and zeroizes its allocation on drop.

## Group security

The bot must be an administrator. The relay records every human identity it observes in Telegram's administrator list, membership updates, join/leave service messages, and message envelopes. Departed and kicked users remain in the historical ledger.

A group is allowed only when both checks pass:

1. Telegram's current member count equals the observed active-human ledger plus the bot.
2. Every human in the permanent historical ledger is present in the host's current whitelist snapshot.

Otherwise the group is quarantined. Quarantine discards message content before invocation parsing, text/media extraction, download, logging, or archival; only the sender and membership/service metadata needed to improve the ledger are handled. Eligibility is recomputed on later messages, so whitelisting every historical identity makes the quarantine reversible. Losing administrator status fails closed.

Telegram does not provide bots with an API that enumerates all ordinary members of an existing group. The reliable onboarding sequence is therefore to add the bot to a new group, make it an administrator, and then add members. A pre-existing group stays quarantined until its current roster has been completely observed. Membership from before the bot joined cannot be reconstructed by the library.

## Persistence and API boundary

`migrate_storage(path)` applies idempotent transport migrations without starting the service. The database contains Telegram events, private/group session pointers, opaque group IDs and chat aliases, the historical member ledger, archived group messages/media, reset work, and ingress batches. It contains no whitelist, observed-identity directory, application root, or capability table.

The loopback HTTP API is intentionally transport-only: event queues, media, binding/reply/abort transitions, group ingress, and group-session context. User and application-root provisioning endpoints belong to the host.

See [Specification.md](./Specification.md) for the complete behavioral contract.
