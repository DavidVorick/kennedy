//! In-memory, job-oriented audio transcription using caller-owned provider clients.
//!
//! [`AudioTranscriber::transcribe`] is the only operation needed to start the
//! complete pipeline. The returned [`TranscriptionJob`] exposes cheap,
//! synchronous [`TranscriptionJob::status`] snapshots while work proceeds.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::{
    io::Cursor,
    sync::{Arc, PoisonError, RwLock},
    time::Duration,
};

use anyhow::{Context, ensure};
use futures::{StreamExt, stream};
use hound::{SampleFormat, WavReader};
use kcode_codex_runtime::{Codex, ErrorKind as CodexErrorKind, GenerationRequest, ReasoningEffort};
use kcode_gemini_api::{
    CompletionStatus, Error as GeminiError, Gemini, GenerationOptions, MediaInput,
    MultimodalRequest, ServiceTier, StructuredOutput, ThinkingLevel,
};
use ruopus::encode_ogg_opus;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Gemini model used for chunk transcription.
pub const TRANSCRIPTION_MODEL: &str = kcode_gemini_api::GEMINI_31_PRO;
/// Codex model used to reconcile ordered chunk transcripts.
pub const RECONCILIATION_MODEL: &str = "gpt-5.6-sol";
/// Codex reasoning setting used for transcript reconciliation.
pub const RECONCILIATION_REASONING: &str = "xhigh";

const MAX_CHUNK_MILLISECONDS: u64 = 4 * 60 * 1_000;
const CHUNK_OVERLAP_MILLISECONDS: u64 = 15 * 1_000;
const MAX_CONCURRENT_CHUNKS: usize = 4;
const OPUS_SAMPLE_RATE: u32 = 48_000;
const OPUS_MAX_CHANNELS: usize = 2;
const OPUS_BITRATE_PER_CHANNEL_BPS: u32 = 192_000;
const MAX_PROVIDER_ATTEMPTS: u32 = 3;
const MAX_TRANSCRIPT_TOKENS: u64 = 50_000;
const ESTIMATED_CHARACTERS_PER_TOKEN: u64 = 4;
const TRANSCRIPT_BREAK: &str = "<!-- KCODE_TRANSCRIPT_BREAK -->";

/// Overall state of an in-memory transcription job.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// The background task has not begun processing.
    Queued,
    /// At least one pipeline step is active or retrying.
    Running,
    /// Every required step completed and `transcript` is present.
    Completed,
    /// A terminal step error prevented completion.
    Failed,
}

/// One ordered pipeline operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Step {
    /// Validate the supplied WAV byte buffer.
    ValidateAudio,
    /// Calculate equalized overlapping audio windows.
    PlanChunks,
    /// Prepare, submit, and validate one chronological audio chunk.
    TranscribeChunk {
        /// Zero-based chronological chunk index.
        index: usize,
        /// Total number of planned chunks.
        total: usize,
    },
    /// Reconcile all ordered chunk transcripts into canonical Markdown.
    ReconcileTranscript,
    /// Add safe boundaries when the reconciled transcript is unusually large.
    SplitTranscript,
}

/// State of one pipeline step.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StepState {
    /// A dependency has not completed yet.
    Pending,
    /// The step is currently executing.
    Running,
    /// A retryable provider operation is waiting before another attempt.
    Retrying,
    /// The step completed successfully.
    Completed,
    /// The step was unnecessary for this input.
    Skipped,
    /// The step ended with an error.
    Failed,
}

/// Sanitized terminal or retryable step error.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StepError {
    /// Stable machine-readable error category.
    pub code: String,
    /// Concise human-readable diagnostic.
    pub message: String,
    /// Whether starting or continuing a retry can reasonably succeed.
    pub retryable: bool,
}

/// Current status of one ordered pipeline step.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StepStatus {
    /// Operation represented by this entry.
    pub step: Step,
    /// Current lifecycle state.
    pub state: StepState,
    /// Number of times the operation has started.
    pub attempts: u32,
    /// Remaining scheduled retry delay when the snapshot was written.
    pub retry_after: Option<Duration>,
    /// Current failure detail, if any.
    pub error: Option<StepError>,
}

/// Cheap cloneable snapshot returned by [`TranscriptionJob::status`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TranscriptionStatus {
    /// Overall job lifecycle state.
    pub state: JobState,
    /// Ordered pipeline operations, including one entry per planned chunk.
    pub steps: Vec<StepStatus>,
    /// Final canonical Markdown transcript, present only after completion.
    pub transcript: Option<String>,
}

/// Cloneable handle for polling one in-memory transcription.
#[derive(Clone, Debug)]
pub struct TranscriptionJob {
    status: Arc<RwLock<TranscriptionStatus>>,
}

impl TranscriptionJob {
    /// Returns an in-memory snapshot without performing I/O or provider calls.
    pub fn status(&self) -> TranscriptionStatus {
        self.status
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Complete audio transcription pipeline backed by configured provider clients.
#[derive(Clone, Debug)]
pub struct AudioTranscriber {
    gemini: Gemini,
    codex: Codex,
}

impl AudioTranscriber {
    /// Constructs a transcriber without receiving or resolving provider keys.
    pub fn new(gemini: Gemini, codex: Codex) -> Self {
        Self { gemini, codex }
    }

    /// Starts transcription of owned WAV bytes and immediately returns a job.
    ///
    /// The complete pipeline runs on the current Tokio runtime. If no runtime
    /// is active, the returned job is immediately failed with a status error.
    pub fn transcribe(&self, audio: Vec<u8>) -> TranscriptionJob {
        let status = Arc::new(RwLock::new(initial_status()));
        let job = TranscriptionJob {
            status: status.clone(),
        };
        let gemini = self.gemini.clone();
        let codex = self.codex.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(run_job(audio, gemini, codex, status));
            }
            Err(_) => fail_job(
                &status,
                &Step::ValidateAudio,
                Failure::new(
                    "runtime_unavailable",
                    "transcribe() requires an active Tokio runtime",
                    true,
                ),
            ),
        }
        job
    }
}

fn initial_status() -> TranscriptionStatus {
    TranscriptionStatus {
        state: JobState::Queued,
        steps: vec![
            pending(Step::ValidateAudio),
            pending(Step::PlanChunks),
            pending(Step::ReconcileTranscript),
            pending(Step::SplitTranscript),
        ],
        transcript: None,
    }
}

fn pending(step: Step) -> StepStatus {
    StepStatus {
        step,
        state: StepState::Pending,
        attempts: 0,
        retry_after: None,
        error: None,
    }
}

#[derive(Clone, Debug)]
struct Failure {
    code: &'static str,
    message: String,
    retryable: bool,
}

impl Failure {
    fn new(code: &'static str, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code,
            message: concise(&message.into(), 2_000),
            retryable,
        }
    }

    fn step_error(&self) -> StepError {
        StepError {
            code: self.code.into(),
            message: self.message.clone(),
            retryable: self.retryable,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct WavInfo {
    duration_ms: u64,
}

#[derive(Clone, Copy, Debug)]
struct ChunkPlan {
    index: usize,
    total: usize,
    start_ms: u64,
    end_ms: u64,
}

#[derive(Clone, Debug)]
struct ChunkTranscript {
    plan: ChunkPlan,
    transcript: String,
}

async fn run_job(
    audio: Vec<u8>,
    gemini: Gemini,
    codex: Codex,
    status: Arc<RwLock<TranscriptionStatus>>,
) {
    set_job_running(&status);
    set_step_running(&status, &Step::ValidateAudio, 1);
    let validation_audio = audio.clone();
    let info = match tokio::task::spawn_blocking(move || validate_wav(&validation_audio)).await {
        Ok(Ok(info)) => info,
        Ok(Err(error)) => {
            fail_job(&status, &Step::ValidateAudio, error);
            return;
        }
        Err(error) => {
            fail_job(
                &status,
                &Step::ValidateAudio,
                Failure::new(
                    "validation_task_failed",
                    format!("audio validation worker stopped: {error}"),
                    true,
                ),
            );
            return;
        }
    };
    set_step_completed(&status, &Step::ValidateAudio);

    set_step_running(&status, &Step::PlanChunks, 1);
    let boundaries = chunk_boundaries(info.duration_ms);
    if boundaries.is_empty() {
        fail_job(
            &status,
            &Step::PlanChunks,
            Failure::new("invalid_audio", "WAV contains no audio samples", false),
        );
        return;
    }
    let total = boundaries.len();
    let plans = boundaries
        .into_iter()
        .enumerate()
        .map(|(index, (start_ms, end_ms))| ChunkPlan {
            index,
            total,
            start_ms,
            end_ms,
        })
        .collect::<Vec<_>>();
    install_chunk_steps(&status, total);
    set_step_completed(&status, &Step::PlanChunks);

    let shared_audio = Arc::new(audio);
    let mut work = stream::iter(plans.into_iter().map(|plan| {
        let audio = shared_audio.clone();
        let gemini = gemini.clone();
        let status = status.clone();
        async move {
            let result = transcribe_chunk(audio, gemini, status, plan).await;
            (plan.index, result)
        }
    }))
    .buffer_unordered(MAX_CONCURRENT_CHUNKS);
    let mut chunks = vec![None; total];
    let mut first_failure = None;
    while let Some((index, result)) = work.next().await {
        match result {
            Ok(transcript) => chunks[index] = Some(transcript),
            Err(error) if first_failure.is_none() => first_failure = Some(error),
            Err(_) => {}
        }
    }
    if first_failure.is_some() {
        set_job_failed(&status);
        return;
    }
    let ordered = chunks.into_iter().flatten().collect::<Vec<_>>();
    if ordered.len() != total {
        fail_job(
            &status,
            &Step::ReconcileTranscript,
            Failure::new(
                "chunk_result_missing",
                "a completed chunk did not produce a transcript",
                true,
            ),
        );
        return;
    }

    let prompt = reconciliation_prompt(&ordered);
    let transcript = match generate_with_retries(
        &codex,
        &status,
        Step::ReconcileTranscript,
        prompt,
        "reconciliation_failed",
    )
    .await
    {
        Ok(value) => value,
        Err(_) => return,
    };
    let mut pieces = transcript_pieces(&transcript);
    if pieces.is_empty() {
        fail_job(
            &status,
            &Step::ReconcileTranscript,
            Failure::new(
                "empty_transcript",
                "Codex returned an empty reconciled transcript",
                true,
            ),
        );
        return;
    }

    let needs_second_pass = pieces
        .iter()
        .any(|piece| estimate_tokens(piece) > MAX_TRANSCRIPT_TOKENS);
    if needs_second_pass {
        let marked = match generate_with_retries(
            &codex,
            &status,
            Step::SplitTranscript,
            split_prompt(&transcript),
            "split_failed",
        )
        .await
        {
            Ok(value) => value,
            Err(_) => return,
        };
        pieces = transcript_pieces(&marked);
        if pieces.is_empty()
            || pieces
                .iter()
                .any(|piece| estimate_tokens(piece) > MAX_TRANSCRIPT_TOKENS)
        {
            fail_job(
                &status,
                &Step::SplitTranscript,
                Failure::new(
                    "split_invalid",
                    "Codex did not place transcript boundaries below the size limit",
                    true,
                ),
            );
            return;
        }
    } else if pieces.len() > 1 {
        set_step_running(&status, &Step::SplitTranscript, 1);
        set_step_completed(&status, &Step::SplitTranscript);
    } else {
        set_step_skipped(&status, &Step::SplitTranscript);
    }

    let final_transcript = pieces.join("\n\n");
    let mut snapshot = status.write().unwrap_or_else(PoisonError::into_inner);
    snapshot.transcript = Some(final_transcript);
    snapshot.state = JobState::Completed;
}

async fn transcribe_chunk(
    audio: Arc<Vec<u8>>,
    gemini: Gemini,
    status: Arc<RwLock<TranscriptionStatus>>,
    plan: ChunkPlan,
) -> Result<ChunkTranscript, Failure> {
    let step = Step::TranscribeChunk {
        index: plan.index,
        total: plan.total,
    };
    set_step_running(&status, &step, 1);
    let prepared = tokio::task::spawn_blocking(move || {
        wav_interval_to_opus(&audio, plan.start_ms, plan.end_ms)
    })
    .await
    .map_err(|error| {
        Failure::new(
            "audio_task_failed",
            format!("chunk {} audio worker stopped: {error}", plan.index),
            true,
        )
    })?
    .map_err(|error| {
        Failure::new(
            "audio_preparation_failed",
            format!("chunk {} could not be prepared: {error:#}", plan.index),
            false,
        )
    });
    let opus = match prepared {
        Ok(value) => value,
        Err(error) => {
            fail_step(&status, &step, &error);
            return Err(error);
        }
    };

    for attempt in 1..=MAX_PROVIDER_ATTEMPTS {
        set_step_running(&status, &step, attempt);
        let result = request_chunk_transcript(&gemini, &opus, plan).await;
        match result {
            Ok(transcript) => {
                set_step_completed(&status, &step);
                return Ok(ChunkTranscript { plan, transcript });
            }
            Err(error) if error.retryable && attempt < MAX_PROVIDER_ATTEMPTS => {
                let delay = retry_delay(attempt);
                set_step_retrying(&status, &step, attempt, delay, &error);
                tokio::time::sleep(delay).await;
            }
            Err(error) => {
                fail_step(&status, &step, &error);
                return Err(error);
            }
        }
    }
    unreachable!("provider attempt loop always returns")
}

async fn request_chunk_transcript(
    gemini: &Gemini,
    opus: &[u8],
    plan: ChunkPlan,
) -> Result<String, Failure> {
    let media = MediaInput::audio("audio/ogg", opus.to_vec()).map_err(|error| {
        Failure::new(
            "audio_preparation_failed",
            format!("preparing inline Ogg Opus audio: {error}"),
            false,
        )
    })?;
    let mut request = MultimodalRequest::new(transcription_prompt(plan), vec![media]);
    request.options = GenerationOptions {
        max_output_tokens: Some(32_768),
        temperature: None,
        thinking_level: Some(ThinkingLevel::High),
        service_tier: ServiceTier::Standard,
    };
    request.structured_output = Some(StructuredOutput::new(transcription_schema()).map_err(
        |error| {
            Failure::new(
                "schema_invalid",
                format!("validating transcription output schema: {error}"),
                false,
            )
        },
    )?);
    let response = gemini
        .infer_pro_multimodal(request)
        .await
        .map_err(|error| {
            let retryable = gemini_retryable(&error);
            Failure::new(
                "gemini_failed",
                format!("Gemini chunk transcription failed: {error}"),
                retryable,
            )
        })?;
    if response.status != CompletionStatus::Completed {
        return Err(Failure::new(
            "gemini_incomplete",
            "Gemini chunk transcription did not complete",
            true,
        ));
    }
    let text = response.text.ok_or_else(|| {
        Failure::new(
            "gemini_response_invalid",
            "Gemini returned no structured transcript text",
            true,
        )
    })?;
    let transcript: Value = serde_json::from_str(&text).map_err(|error| {
        Failure::new(
            "gemini_response_invalid",
            format!("Gemini returned invalid structured transcript JSON: {error}"),
            true,
        )
    })?;
    if transcript
        .get("utterances")
        .and_then(Value::as_array)
        .is_none()
    {
        return Err(Failure::new(
            "gemini_response_invalid",
            "Gemini transcript omitted the utterances array",
            true,
        ));
    }
    serde_json::to_string_pretty(&transcript).map_err(|error| {
        Failure::new(
            "gemini_response_invalid",
            format!("serializing Gemini transcript: {error}"),
            true,
        )
    })
}

async fn generate_with_retries(
    codex: &Codex,
    status: &Arc<RwLock<TranscriptionStatus>>,
    step: Step,
    prompt: String,
    code: &'static str,
) -> Result<String, Failure> {
    for attempt in 1..=MAX_PROVIDER_ATTEMPTS {
        set_step_running(status, &step, attempt);
        let mut request = GenerationRequest::new(prompt.clone(), RECONCILIATION_MODEL);
        request.reasoning_effort = ReasoningEffort::XHigh;
        request.ephemeral = true;
        request.timeout = Duration::from_secs(30 * 60);
        match codex.generate(request).await {
            Ok(response) if !response.answer.trim().is_empty() => {
                set_step_completed(status, &step);
                return Ok(response.answer);
            }
            Ok(_) if attempt < MAX_PROVIDER_ATTEMPTS => {
                let error = Failure::new(code, "Codex returned empty transcript text", true);
                let delay = retry_delay(attempt);
                set_step_retrying(status, &step, attempt, delay, &error);
                tokio::time::sleep(delay).await;
            }
            Ok(_) => {
                let error = Failure::new(code, "Codex returned empty transcript text", true);
                fail_job(status, &step, error.clone());
                return Err(error);
            }
            Err(provider)
                if codex_retryable(provider.kind()) && attempt < MAX_PROVIDER_ATTEMPTS =>
            {
                let error = Failure::new(
                    code,
                    format!("Codex transcript processing failed: {provider}"),
                    true,
                );
                let delay = retry_delay(attempt);
                set_step_retrying(status, &step, attempt, delay, &error);
                tokio::time::sleep(delay).await;
            }
            Err(provider) => {
                let retryable = codex_retryable(provider.kind());
                let error = Failure::new(
                    code,
                    format!("Codex transcript processing failed: {provider}"),
                    retryable,
                );
                fail_job(status, &step, error.clone());
                return Err(error);
            }
        }
    }
    unreachable!("provider attempt loop always returns")
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_secs(60 * (1_u64 << attempt.saturating_sub(1).min(5)))
}

fn gemini_retryable(error: &GeminiError) -> bool {
    match error {
        GeminiError::Transport(_)
        | GeminiError::Protocol(_)
        | GeminiError::SpendingLimitReached { .. } => true,
        GeminiError::Provider { status, .. } => {
            matches!(*status, 408 | 409 | 425 | 429) || *status >= 500
        }
        GeminiError::InvalidApiKey | GeminiError::InvalidInput(_) | GeminiError::Accounting(_) => {
            false
        }
    }
}

fn codex_retryable(kind: CodexErrorKind) -> bool {
    matches!(
        kind,
        CodexErrorKind::Unavailable
            | CodexErrorKind::RateLimited
            | CodexErrorKind::Capacity
            | CodexErrorKind::Timeout
            | CodexErrorKind::EmptyOutput
            | CodexErrorKind::Protocol
    )
}

fn validate_wav(audio: &[u8]) -> Result<WavInfo, Failure> {
    if audio.is_empty() {
        return Err(Failure::new(
            "invalid_audio",
            "audio byte buffer is empty",
            false,
        ));
    }
    let reader = WavReader::new(Cursor::new(audio)).map_err(|error| {
        Failure::new(
            "invalid_audio",
            format!("invalid WAV recording: {error}"),
            false,
        )
    })?;
    let spec = reader.spec();
    if spec.sample_rate == 0 || !(1..=OPUS_MAX_CHANNELS as u16).contains(&spec.channels) {
        return Err(Failure::new(
            "invalid_audio",
            "WAV must have a positive sample rate and one or two channels",
            false,
        ));
    }
    let supported = matches!(
        (spec.sample_format, spec.bits_per_sample),
        (SampleFormat::Float, 32) | (SampleFormat::Int, 1..=32)
    );
    if !supported {
        return Err(Failure::new(
            "invalid_audio",
            format!(
                "unsupported WAV sample format: {:?} with {} bits",
                spec.sample_format, spec.bits_per_sample
            ),
            false,
        ));
    }
    let declared_audio_bytes = u64::from(reader.duration())
        .saturating_mul(u64::from(spec.channels))
        .saturating_mul(u64::from(spec.bits_per_sample).div_ceil(8));
    if declared_audio_bytes > audio.len() as u64 {
        return Err(Failure::new(
            "invalid_audio",
            format!(
                "invalid WAV recording: header declares {declared_audio_bytes} audio bytes but the buffer has only {} bytes",
                audio.len()
            ),
            false,
        ));
    }
    let duration_ms = (u64::from(reader.duration()) * 1_000).div_ceil(u64::from(spec.sample_rate));
    if duration_ms == 0 {
        return Err(Failure::new(
            "invalid_audio",
            "WAV contains no audio samples",
            false,
        ));
    }
    Ok(WavInfo { duration_ms })
}

fn chunk_boundaries(duration_ms: u64) -> Vec<(u64, u64)> {
    if duration_ms == 0 {
        return Vec::new();
    }
    if duration_ms <= MAX_CHUNK_MILLISECONDS {
        return vec![(0, duration_ms)];
    }
    let advance = MAX_CHUNK_MILLISECONDS - CHUNK_OVERLAP_MILLISECONDS;
    let chunks = (duration_ms - CHUNK_OVERLAP_MILLISECONDS).div_ceil(advance);
    let window = (duration_ms + (chunks - 1) * CHUNK_OVERLAP_MILLISECONDS).div_ceil(chunks);
    let step = window - CHUNK_OVERLAP_MILLISECONDS;
    (0..chunks)
        .map(|index| {
            let start = index * step;
            (start, (start + window).min(duration_ms))
        })
        .filter(|(start, end)| end > start)
        .collect()
}

fn wav_interval_to_opus(audio: &[u8], start_ms: u64, end_ms: u64) -> anyhow::Result<Vec<u8>> {
    let mut reader = WavReader::new(Cursor::new(audio)).context("opening in-memory WAV audio")?;
    let spec = reader.spec();
    ensure!(end_ms > start_ms, "audio interval is empty");
    let start_frame = u32::try_from(start_ms * u64::from(spec.sample_rate) / 1_000)
        .context("audio interval starts beyond WAV limits")?;
    let end_frame = u32::try_from(end_ms * u64::from(spec.sample_rate) / 1_000)
        .context("audio interval ends beyond WAV limits")?;
    let sample_values = usize::try_from(
        u64::from(end_frame.saturating_sub(start_frame)) * u64::from(spec.channels),
    )
    .context("audio interval is too large for this platform")?;
    reader.seek(start_frame).context("seeking WAV interval")?;
    let samples = match (spec.sample_format, spec.bits_per_sample) {
        (SampleFormat::Float, 32) => reader
            .samples::<f32>()
            .take(sample_values)
            .map(|sample| sample.context("reading 32-bit float WAV sample"))
            .collect::<anyhow::Result<Vec<_>>>()?,
        (SampleFormat::Int, 1..=8) => {
            let scale = 2.0_f32.powi(i32::from(spec.bits_per_sample) - 1);
            reader
                .samples::<i8>()
                .take(sample_values)
                .map(|sample| {
                    sample
                        .map(|value| f32::from(value) / scale)
                        .context("reading 8-bit WAV sample")
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        }
        (SampleFormat::Int, 9..=16) => {
            let scale = 2.0_f32.powi(i32::from(spec.bits_per_sample) - 1);
            reader
                .samples::<i16>()
                .take(sample_values)
                .map(|sample| {
                    sample
                        .map(|value| f32::from(value) / scale)
                        .context("reading 16-bit WAV sample")
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        }
        (SampleFormat::Int, 17..=32) => {
            let scale = 2.0_f64.powi(i32::from(spec.bits_per_sample) - 1) as f32;
            reader
                .samples::<i32>()
                .take(sample_values)
                .map(|sample| {
                    sample
                        .map(|value| value as f32 / scale)
                        .context("reading high-resolution integer WAV sample")
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        }
        _ => anyhow::bail!(
            "unsupported WAV sample format: {:?} with {} bits",
            spec.sample_format,
            spec.bits_per_sample
        ),
    };
    ensure!(
        samples.len() == sample_values,
        "WAV audio ended before the planned interval"
    );
    let channels = usize::from(spec.channels);
    ensure!(
        samples.len().is_multiple_of(channels),
        "WAV audio ended with an incomplete frame"
    );
    ensure!(
        !samples.is_empty(),
        "WAV audio interval contains no samples"
    );
    ensure!(
        samples.iter().all(|sample| sample.is_finite()),
        "WAV audio contains a non-finite sample"
    );
    let pcm = samples
        .into_iter()
        .map(|sample| sample.clamp(-1.0, 1.0))
        .collect::<Vec<_>>();
    let pcm = resample_interleaved(&pcm, spec.sample_rate, channels)?;
    let bitrate = OPUS_BITRATE_PER_CHANNEL_BPS * u32::from(spec.channels);
    Ok(encode_ogg_opus(&pcm, channels, bitrate))
}

fn resample_interleaved(
    source: &[f32],
    source_rate: u32,
    channels: usize,
) -> anyhow::Result<Vec<f32>> {
    ensure!(source_rate > 0, "WAV sample rate must be positive");
    ensure!(
        (1..=OPUS_MAX_CHANNELS).contains(&channels),
        "Ogg Opus encoding supports mono or stereo PCM"
    );
    ensure!(
        source.len().is_multiple_of(channels),
        "PCM ended with an incomplete frame"
    );
    ensure!(!source.is_empty(), "PCM contains no samples");
    if source_rate == OPUS_SAMPLE_RATE {
        return Ok(source.to_vec());
    }
    let source_frames = source.len() / channels;
    let output_frames = usize::try_from(
        (source_frames as u128 * u128::from(OPUS_SAMPLE_RATE)).div_ceil(u128::from(source_rate)),
    )
    .context("resampled audio is too large for this platform")?;
    let output_samples = output_frames
        .checked_mul(channels)
        .context("resampled audio is too large for this platform")?;
    let mut output = Vec::with_capacity(output_samples);
    for output_frame in 0..output_frames {
        let source_position = output_frame as u128 * u128::from(source_rate);
        let lower = usize::try_from(source_position / u128::from(OPUS_SAMPLE_RATE))
            .context("resampling position is too large for this platform")?
            .min(source_frames - 1);
        let upper = (lower + 1).min(source_frames - 1);
        let fraction =
            (source_position % u128::from(OPUS_SAMPLE_RATE)) as f32 / OPUS_SAMPLE_RATE as f32;
        for channel in 0..channels {
            let lower_sample = source[lower * channels + channel];
            let upper_sample = source[upper * channels + channel];
            output.push(lower_sample + (upper_sample - lower_sample) * fraction);
        }
    }
    Ok(output)
}

fn transcription_prompt(plan: ChunkPlan) -> String {
    format!(
        "Transcribe this audio faithfully and completely. Distinguish every discernible speaker with chunk-local labels such as speaker_1. Do not guess a real identity. Preserve the original language. When an utterance is not English, also provide an accurate English translation; for English, use an empty translation string. Add concise annotations when speech is unclear, overlapping, interrupted, emotional in a materially relevant way, or accompanied by relevant non-speech audio. Timestamps are seconds relative to this audio chunk. Do not omit quiet or difficult portions.\n\nChunk index: {} of {}\nThis chunk covers source offsets {:.3} through {:.3} seconds. Adjacent chunks overlap by up to 15 seconds; transcribe the entire supplied chunk even when boundary material will be repeated elsewhere.",
        plan.index + 1,
        plan.total,
        plan.start_ms as f64 / 1_000.0,
        plan.end_ms as f64 / 1_000.0,
    )
}

fn transcription_schema() -> Value {
    json!({
        "type":"object",
        "properties":{
            "utterances":{
                "type":"array",
                "items":{
                    "type":"object",
                    "properties":{
                        "start_seconds":{"type":"number"},
                        "end_seconds":{"type":"number"},
                        "speaker":{"type":"string"},
                        "language":{"type":"string"},
                        "original_text":{"type":"string"},
                        "english_translation":{"type":"string"},
                        "annotations":{"type":"array","items":{"type":"string"}},
                        "confidence":{"type":"string","enum":["high","medium","low"]}
                    },
                    "required":["start_seconds","end_seconds","speaker","language","original_text","english_translation","annotations","confidence"]
                }
            },
            "chunk_notes":{"type":"array","items":{"type":"string"}}
        },
        "required":["utterances","chunk_notes"]
    })
}

fn reconciliation_prompt(chunks: &[ChunkTranscript]) -> String {
    let mut prompt = format!(
        "You are producing the canonical final transcript of one audio recording. The chunk transcripts below are already in exact chronological order. They were independently transcribed from audio windows that overlap their neighbors by 15 seconds. Faithfully copy all spoken content into one coherent transcript, remove only duplicated boundary material, and reconcile chunk-local speaker labels across the complete conversation. Use real speaker names only when supported by the conversation; otherwise assign stable labels such as Speaker A. Preserve useful uncertainty and annotations. For every non-English utterance, show its English translation alongside it. Preserve chronological timestamps, converting chunk-relative timestamps using each chunk's supplied source offset. Do not summarize or omit content.\n\nOutput only the final readable Markdown transcript. When the transcript would exceed an estimated 50,000 tokens using one token per four Unicode characters, insert the exact line `{TRANSCRIPT_BREAK}` at sensible conversational or topical boundaries so every resulting piece stays at or below that estimate. Do not make pieces equal-sized merely for symmetry.\n\nORDERED CHUNK TRANSCRIPTS\n"
    );
    for chunk in chunks {
        prompt.push_str(&format!(
            "\n\nCHUNK {:05} | source offsets {:.3}–{:.3} seconds\n{}",
            chunk.plan.index,
            chunk.plan.start_ms as f64 / 1_000.0,
            chunk.plan.end_ms as f64 / 1_000.0,
            chunk.transcript,
        ));
    }
    prompt
}

fn split_prompt(transcript: &str) -> String {
    format!(
        "Copy the following final transcript completely and exactly, adding only the exact boundary line `{TRANSCRIPT_BREAK}` at sensible conversational or topical boundaries. Using the conservative estimate of one token per four Unicode characters, every resulting piece must be no more than 50,000 estimated tokens. Do not summarize, rewrite, reorder, or omit anything. Output only the complete marked transcript.\n\nFINAL TRANSCRIPT\n\n{transcript}"
    )
}

fn transcript_pieces(transcript: &str) -> Vec<String> {
    transcript
        .split(TRANSCRIPT_BREAK)
        .map(str::trim)
        .filter(|piece| !piece.is_empty())
        .map(str::to_owned)
        .collect()
}

fn estimate_tokens(value: &str) -> u64 {
    (value.chars().count() as u64).div_ceil(ESTIMATED_CHARACTERS_PER_TOKEN)
}

fn set_job_running(status: &Arc<RwLock<TranscriptionStatus>>) {
    status.write().unwrap_or_else(PoisonError::into_inner).state = JobState::Running;
}

fn set_job_failed(status: &Arc<RwLock<TranscriptionStatus>>) {
    status.write().unwrap_or_else(PoisonError::into_inner).state = JobState::Failed;
}

fn install_chunk_steps(status: &Arc<RwLock<TranscriptionStatus>>, total: usize) {
    let mut snapshot = status.write().unwrap_or_else(PoisonError::into_inner);
    let insertion = snapshot
        .steps
        .iter()
        .position(|entry| entry.step == Step::ReconcileTranscript)
        .expect("initial status contains reconciliation");
    snapshot.steps.splice(
        insertion..insertion,
        (0..total).map(|index| pending(Step::TranscribeChunk { index, total })),
    );
}

fn mutate_step(
    status: &Arc<RwLock<TranscriptionStatus>>,
    step: &Step,
    change: impl FnOnce(&mut StepStatus),
) {
    let mut snapshot = status.write().unwrap_or_else(PoisonError::into_inner);
    if let Some(entry) = snapshot.steps.iter_mut().find(|entry| &entry.step == step) {
        change(entry);
    }
}

fn set_step_running(status: &Arc<RwLock<TranscriptionStatus>>, step: &Step, attempt: u32) {
    mutate_step(status, step, |entry| {
        entry.state = StepState::Running;
        entry.attempts = attempt;
        entry.retry_after = None;
        entry.error = None;
    });
}

fn set_step_retrying(
    status: &Arc<RwLock<TranscriptionStatus>>,
    step: &Step,
    attempt: u32,
    delay: Duration,
    error: &Failure,
) {
    mutate_step(status, step, |entry| {
        entry.state = StepState::Retrying;
        entry.attempts = attempt;
        entry.retry_after = Some(delay);
        entry.error = Some(error.step_error());
    });
}

fn set_step_completed(status: &Arc<RwLock<TranscriptionStatus>>, step: &Step) {
    mutate_step(status, step, |entry| {
        entry.state = StepState::Completed;
        entry.retry_after = None;
        entry.error = None;
    });
}

fn set_step_skipped(status: &Arc<RwLock<TranscriptionStatus>>, step: &Step) {
    mutate_step(status, step, |entry| {
        entry.state = StepState::Skipped;
        entry.retry_after = None;
        entry.error = None;
    });
}

fn fail_step(status: &Arc<RwLock<TranscriptionStatus>>, step: &Step, error: &Failure) {
    mutate_step(status, step, |entry| {
        entry.state = StepState::Failed;
        entry.retry_after = None;
        entry.error = Some(error.step_error());
    });
}

fn fail_job(status: &Arc<RwLock<TranscriptionStatus>>, step: &Step, error: Failure) {
    fail_step(status, step, &error);
    set_job_failed(status);
}

fn concise(value: &str, limit: usize) -> String {
    let clean = value.split_whitespace().collect::<Vec<_>>().join(" ");
    clean.chars().take(limit).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hound::{WavSpec, WavWriter};

    fn wav_bytes(channels: u16, sample_rate: u32, frames: u32) -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = WavWriter::new(
                &mut cursor,
                WavSpec {
                    channels,
                    sample_rate,
                    bits_per_sample: 16,
                    sample_format: SampleFormat::Int,
                },
            )
            .unwrap();
            for frame in 0..frames {
                let phase = frame as f32 * 440.0 * std::f32::consts::TAU / sample_rate as f32;
                for _ in 0..channels {
                    writer.write_sample((phase.sin() * 8_192.0) as i16).unwrap();
                }
            }
            writer.finalize().unwrap();
        }
        cursor.into_inner()
    }

    #[test]
    fn long_recordings_are_equalized_with_overlap() {
        let chunks = chunk_boundaries(8 * 60 * 1_000);
        assert_eq!(chunks.len(), 3);
        assert!(
            chunks
                .iter()
                .all(|(start, end)| end - start <= MAX_CHUNK_MILLISECONDS)
        );
        assert_eq!(chunks[0].1 - chunks[1].0, CHUNK_OVERLAP_MILLISECONDS);
        assert_eq!(chunks[1].1 - chunks[2].0, CHUNK_OVERLAP_MILLISECONDS);
        assert!((chunks[0].1 - chunks[0].0).abs_diff(chunks[2].1 - chunks[2].0) <= 1);
    }

    #[test]
    fn interval_encoding_is_entirely_in_memory() {
        let wav = wav_bytes(2, 44_100, 4_410);
        validate_wav(&wav).unwrap();
        let opus = wav_interval_to_opus(&wav, 0, 100).unwrap();
        assert_eq!(&opus[..4], b"OggS");
        let (decoded, head) = ruopus::decode_ogg_opus(&opus).unwrap();
        assert_eq!(head.channel_count, 2);
        assert_eq!(head.input_sample_rate, OPUS_SAMPLE_RATE);
        assert!(!decoded.is_empty());
    }

    #[test]
    fn initial_status_has_only_serial_steps() {
        let status = initial_status();
        assert_eq!(status.state, JobState::Queued);
        assert_eq!(status.steps.len(), 4);
        assert!(
            status
                .steps
                .iter()
                .all(|step| step.state == StepState::Pending)
        );
        assert!(status.transcript.is_none());
    }

    #[test]
    fn status_serializes_chunk_steps_for_frontends() {
        let status = Arc::new(RwLock::new(initial_status()));
        install_chunk_steps(&status, 2);
        let value = serde_json::to_value(
            status
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        )
        .unwrap();
        assert_eq!(value["steps"][2]["step"]["kind"], "transcribe_chunk");
        assert_eq!(value["steps"][2]["step"]["index"], 0);
        assert_eq!(value["steps"][3]["step"]["index"], 1);
    }

    #[test]
    fn empty_and_truncated_wav_data_are_rejected() {
        assert_eq!(validate_wav(&[]).unwrap_err().code, "invalid_audio");
        let mut wav = wav_bytes(1, 8_000, 8_000);
        wav.truncate(100);
        assert_eq!(validate_wav(&wav).unwrap_err().code, "invalid_audio");
    }
}
