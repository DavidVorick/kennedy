# Telegram Relay Specification

The Telegram relay is a Rust service in the Kennedy server binary. It
long-polls Telegram with `teloxide`, accepts private and strictly validated
group messages from whitelisted users, stores transport work durably, and
exposes a loopback HTTP API to the browser frontend. It does not construct
prompts, run Kennedy, access the Kmap, or inspect the Chatend.

Identity state is isolated in `kennedy-users.sqlite3`, separate from the relay
queue and Kweb databases. `@taek42` is the only initial whitelist entry and has
the generic `can_add_users` capability. Its first observed matching Telegram
account pins the stable numeric ID under trust on first use (TOFU), exactly like
any subsequently whitelisted handle. Once pinned, numeric ID is authoritative
even if the username changes. A different ID presenting a pinned handle is a
conflict. The backend contains no David-specific numeric ID or sentinel path.

The privileged pinned identity may send `/adduser @handle` in a private chat.
This immediately reserves a random Kmap root ID and an unresolved whitelist
entry. No registration or onboarding event exists. The frontend idempotently
materializes the reserved structurally blank root through Kweb and marks it
ready. Unauthorized private message content is not stored.

Text, voice-note, supported document, and `/reset` updates are processed in
per-user order. Documents include PDF, DOCX, spreadsheet, CSV, and text files
up to 20 MiB. An update remains pending or processing until the browser supplies
the reply and Telegram accepts it. Original voice-note and document bytes remain
in the relay archive. `/reset` is an event: the browser closes the corresponding
Telegram conversation, requests history ingress, and acknowledges the reset only
after that transition is durable.

Private active pointers remain in `authorized_users`. Group pointers live in
`telegram_group_user_sessions`, keyed by stable group-root ID and Telegram user
ID, and `telegram_events.group_root_node_id` makes that identity durable across
chat-ID migration. A pending group event inherits its pair's current pointer;
binding updates only that pair.

For groups, Kennedy requests `chat_member` updates and must be an administrator
before processing messages. Adding and then promoting Kennedy may be two
actions; the group remains inert in `validating` between them. The relay keeps
a member ledger, resolves previously unpinned whitelisted handles through TOFU,
and compares the observed active ledger plus Kennedy with Telegram's member
count. Any unknown identity, TOFU conflict, incomplete ledger, or loss of
administrator monitoring after activation permanently blacklists that chat ID.
Later `/adduser` calls or member departures cannot reverse the decision.
Every group is also assigned a stable reserved Kmap root when first observed.
Blacklisting retains that assignment. A Telegram basic-group-to-supergroup
migration maps the new chat ID to the same root and carries forward membership,
cursor, readiness, and permanent-blacklist state.

Telegram's Bot API does not enumerate every ordinary member of an existing
group. Consequently the reliable strict flow is to create a new group with
Kennedy, promote her to administrator, and then add the whitelisted members so
each join is observed. Dropping Kennedy into a pre-existing group whose full
membership was not observed produces a count mismatch and permanent blacklist,
which is the fail-closed behavior required by this policy.

An allowed group message queues Kennedy when it mentions her bot handle,
replies to one of her messages, or is a scoped `/reset`. Voice notes therefore
invoke by reply; supported documents may invoke by caption mention or reply.
The event carries the group root, the complete
current member ledger with reserved user-root IDs, and the latest 50 archived
group messages. It is
marked `sessionKind: group`; the relay binds it to a persistent session keyed
by `(group root, Telegram user)` and never changes the invoker's private-DM or
other-group conversation pointer. Group `/reset` clears only that binding.
More than 100 non-invocation messages after the last
covered cursor queues the oldest 80 as one durable background-ingress batch,
leaving 20 messages unbatched. The relay stores Kennedy's group replies in the
same message archive.

The browser may fetch one head-of-line event per private user or group-user
pair, bind it to a Conversation
History ID, store a voice note's paid transcription, fetch original media bytes,
locally extract bounded document text through the intelligence backend, and
submit a final reply. A document that is corrupt, image-only, or otherwise not
readable, or whose extraction otherwise fails, receives a clear Telegram error
and is completed instead of retrying forever and blocking that user's later
events. Group voice and document events retain the same bytes, MIME type,
filename/caption, and duration metadata as private events. Reply bodies contain
Kennedy's conversational output only, plus an
optional separate context-window notice. Long messages are split safely below
Telegram's message limit.

Directory endpoints separately expose unresolved user and group roots for
frontend provisioning and look up a whitelisted entry by handle/numeric ID or a
group by chat ID. Group-ingress endpoints expose pending 80-message batches
decorated with their group root and idempotently mark them complete after the
normal Conversation History/Kmap ingress pipeline finishes.

If the configured bot-token name is absent from Kennedy's unlocked encrypted
credential vault, the relay HTTP service remains available and reports itself
disabled. `kennedy-server` passes the resolved value directly into the relay at
startup; the token is never exposed by the API, frontend, configuration file,
or a vault reveal command.
