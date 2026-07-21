# Documentation

Construct `AudioTranscriber` with configured, cloneable `Gemini` and `Codex`
objects. `AudioTranscriber::transcribe(Vec<u8>)` immediately returns a
`TranscriptionJob`; the work runs on the current Tokio runtime.

`TranscriptionJob::status()` performs no provider call. It clones the current
`TranscriptionStatus`, whose ordered `steps` list grows to include one
`TranscribeChunk` entry per planned chunk. `transcript` is `Some` only when the
job is complete. Failures are terminal for that in-memory job and carry a
stable code, a concise message, and whether retrying a fresh job can help.

```text
pub struct TranscriptionStatus {
    pub state: JobState,
    pub steps: Vec<StepStatus>,
    pub transcript: Option<String>,
}

pub struct StepStatus {
    pub step: Step,
    pub state: StepState,
    pub attempts: u32,
    pub retry_after: Option<std::time::Duration>,
    pub error: Option<StepError>,
}

pub struct StepError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}
```

The supported input is a non-empty PCM or IEEE-float WAV byte buffer with one
or two channels. Every independent chunk is decoded, resampled, Opus-encoded,
submitted to Gemini, and validated concurrently, with at most four chunk
pipelines active. Results are reconciled by Codex only after all chunks finish.

The caller owns persistence and may retain the original bytes to start a new
job after shutdown or failure. The library performs no filesystem access.
