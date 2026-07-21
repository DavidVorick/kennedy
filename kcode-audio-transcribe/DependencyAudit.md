# Dependency Audit

- `kcode-gemini-api`: configured Gemini multimodal client supplied by caller.
- `kcode-codex-runtime`: configured Codex generation runtime supplied by caller.
- `hound`: in-memory WAV parsing and interval decoding.
- `ruopus`: in-memory Ogg Opus encoding.
- `tokio` and `futures`: background execution, bounded concurrency, and retry
  timing.
- `serde` and `serde_json`: public status serialization and structured Gemini
  transcript validation.
- `anyhow`: private error context within the processing pipeline.

The crate has no filesystem, HTTP-server, database, credential-vault, or
application-framework dependency.
