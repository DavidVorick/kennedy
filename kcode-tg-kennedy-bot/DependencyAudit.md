# Direct dependency audit for 0.3.0

This crate receives a raw Telegram bot token and handles private text plus bounded voice, document, photo, video, animation, audio, video-note, and sticker content. Its direct dependencies are exact-pinned so a compatible-range resolution cannot silently change this credential-bearing process boundary.

The retained validation set is:

- anyhow 1.0.104;
- axum 0.8.9 with multipart support;
- chrono 0.4.45;
- futures 0.3.33;
- rusqlite 0.40.1 with bundled SQLite;
- serde 1.0.229 with derive;
- serde_json 1.0.151;
- teloxide 0.17.0 with default features disabled and rustls, rustls-native-roots, and tracing enabled;
- tokio 1.53.1 with full features;
- tower-http 0.7.0 with cors and trace;
- tracing 0.1.44;
- uuid 1.24.0 with v4;
- zeroize 1.9.0.

Version 0.3.0 adds no dependency or feature. The existing Axum multipart support now also serves explicit native-media delivery, and teloxide 0.17.0 already exposes every required native send method and inbound media type. The router applies an aggregate body limit before extraction, and both outbound endpoints separately reject oversized or empty file parts, duplicate or unknown fields, unsafe file names, invalid content-type metadata, oversized or inapplicable captions, stale event bindings, and malformed conversation IDs.

Axum, tower-http CORS, Fetch Metadata checks, and response security headers are defense-in-depth components, not application authentication. The primary HTTP authority boundary is the crate's strict literal IPv4/IPv6 loopback-only bind rule. A public reverse proxy, server-side request forgery path, or untrusted local process would cross that assumption and requires an explicit authenticated transport boundary rather than a dependency configuration change.

Teloxide and its HTTP stack receive the bot token and transport message and file bytes to Telegram. Rusqlite persists private transport state and original bounded media locally. Tokio and futures execute long polling, downloads, HTTP serving, and bounded per-principal dispatch. Zeroize reduces retention of the configured token's owned string on drop but cannot prove removal of every compiler, allocator, kernel, transport, or upstream-library copy.

Exact pins are change control, not proof of safety. Any dependency version or feature change requires a fresh source, license, and security review plus the complete managed validation workflow. Cargo's lockfile is not a downstream security boundary for a published library because consumers resolve their own transitive graph. Exact direct requirements are therefore the durable package constraint; each release validation must also record and inspect the complete resolved graph selected by Cargo.
