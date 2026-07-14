# Telegram Relay Specification

The Telegram relay is a Rust service in the Kennedy server binary. It long-polls Telegram with `teloxide`, accepts private-chat messages from paired users, stores them durably in SQLite, and exposes a loopback HTTP API to the browser frontend. It does not construct prompts, run Kennedy, access the Kmap, or inspect the Chatend.

`@taek42` is the initial bootstrap identity. The first private Telegram message whose current username matches an unpaired configured bootstrap username binds that stable numeric Telegram user ID. All future authorization is by numeric ID; other users receive a refusal and their message content is not stored.

Text, voice-note, and `/reset` updates are processed in per-user order. An update remains pending or processing until the browser supplies the reply and Telegram accepts it. The original voice-note bytes remain in the relay archive. `/reset` is an event: the browser closes the corresponding Telegram conversation, requests history ingress, and acknowledges the reset only after that transition is durable.

The browser may fetch one head-of-line event per user, bind it to a Conversation History ID, store its paid transcription, fetch its original audio bytes, and submit a final reply. Reply bodies contain Kennedy's conversational output only, plus an optional separate context-window notice. Long messages are split safely below Telegram's message limit.

If the configured bot-token name is absent from Kennedy's unlocked encrypted
credential vault, the relay HTTP service remains available and reports itself
disabled. `kennedy-server` passes the resolved value directly into the relay at
startup; the token is never exposed by the API, frontend, configuration file,
or a vault reveal command.
