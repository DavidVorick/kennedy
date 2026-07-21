# kcode-audio-transcribe

`kcode-audio-transcribe` turns owned WAV bytes into a reconciled Markdown
transcript. It accepts already-configured `Gemini` and `Codex` clients, starts
the complete pipeline with one call, and exposes a cheap pollable status
snapshot.

```rust,no_run
use kcode_audio_transcribe::{AudioTranscriber, JobState};
use kcode_codex_runtime::Codex;
use kcode_gemini_api::Gemini;

# fn example(gemini: Gemini, codex: Codex, wav_bytes: Vec<u8>) {
let transcriber = AudioTranscriber::new(gemini, codex);
let job = transcriber.transcribe(wav_bytes);
let snapshot = job.status();
if snapshot.state == JobState::Completed {
    println!("{}", snapshot.transcript.as_deref().unwrap_or_default());
}
# }
```

The library never accepts API keys, paths, readers, storage callbacks, or
database handles. Audio, chunks, encoded Opus, progress, and results are held
only in memory. Jobs do not survive process shutdown.

See [Documentation.md](Documentation.md) for the API and
[Specification.md](Specification.md) for behavioral guarantees.
