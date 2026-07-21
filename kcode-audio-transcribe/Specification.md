# Specification

## Public boundary

- Input is an owned `Vec<u8>` containing WAV data.
- Provider authentication and configuration belong to caller-created
  `Gemini` and `Codex` objects.
- Output is a `TranscriptionJob` with synchronous snapshot polling through
  `status()`.
- A completed status contains one canonical Markdown transcript.
- No path, reader, filename, timestamp, hash, storage, database, HTTP, Chatend,
  provenance, Kmap, or MemoryIngress API is exposed.

## Pipeline

1. Validate WAV structure and supported sample representation.
2. Plan equalized windows of at most four minutes with fifteen seconds of
   neighboring overlap.
3. Run up to four independent chunk pipelines concurrently. Each decodes its
   interval from the input bytes, resamples to 48 kHz, encodes Ogg Opus in
   memory, asks Gemini 3.1 Pro for structured utterances, and validates JSON.
4. Sort successful chunk results by original index and ask Codex Sol to remove
   overlap duplication, stabilize speakers, preserve timestamps and
   translations, and produce the final Markdown transcript.
5. If necessary, ask Sol for safe transcript boundaries before returning the
   same clean transcript without internal boundary markers.

Provider operations receive bounded retries inside the job. Independent work
is concurrent; validation, planning, reconciliation, and any required splitting
remain serial because of their data dependencies.
