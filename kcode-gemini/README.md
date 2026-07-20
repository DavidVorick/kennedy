# kcode-gemini

An isolated Rust client for Kennedy's Gemini surface:

- Gemini 3.1 Flash-Lite and Gemini 3.1 Pro Preview text inference;
- Gemini 3.1 Pro image/audio multimodal inference;
- Nano Banana Pro image generation and editing with fixed 2K output;
- Gemini Flash-Lite Google Search grounding;
- in-memory, modality-level usage and estimated-spend reporting; and
- live-session UTC hourly, daily, and monthly spending limits.

`Gemini::open(api_key)` creates a fresh session. Clones share that session, but
the library never writes usage, limits, credentials, prompts, media, or output
to disk. Dropping the last clone discards all session accounting and limits.

The API key is retained in redacted, zeroizing memory and sent only in a
sensitive HTTP header. See `Documentation.md` for the integration API and
`Specification.md` for exact behavior and limitations.
