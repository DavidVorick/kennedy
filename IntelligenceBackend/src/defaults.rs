pub(crate) const MAX_REQUEST_BYTES: usize = 26 * 1024 * 1024;

pub(crate) const DEFAULT_PROVIDER_NAME: &str = "primary";
pub(crate) const CODEX_PROVIDER_KIND: &str = "codex";
pub(crate) const CODEX_EXECUTABLE: &str = "codex-safe";
pub(crate) const DEFAULT_MODEL: &str = "gpt-5.6-sol";
pub(crate) const GENERATION_REASONING_EFFORT: &str = "xhigh";
pub(crate) const GENERATION_TIMEOUT_SECONDS: u64 = 600;
pub(crate) const CONTEXT_WINDOW_TOKENS: u64 = 1_050_000;
pub(crate) const MAX_INPUT_TOKENS: u64 = 922_000;

pub(crate) const FAST_SEARCH_REASONING_EFFORT: &str = "low";
pub(crate) const FAST_SEARCH_CONTEXT_SIZE: &str = "low";
pub(crate) const FAST_SEARCH_MAX_SOURCES: usize = 6;
pub(crate) const FAST_SEARCH_TIMEOUT_SECONDS: u64 = 90;
pub(crate) const QUALITY_SEARCH_REASONING_EFFORT: &str = "xhigh";
pub(crate) const QUALITY_SEARCH_CONTEXT_SIZE: &str = "high";
pub(crate) const QUALITY_SEARCH_MAX_SOURCES: usize = 12;
pub(crate) const QUALITY_SEARCH_TIMEOUT_SECONDS: u64 = 600;

pub(crate) const FETCH_TIMEOUT_SECONDS: u64 = 30;
pub(crate) const MAX_FETCH_BYTES: usize = 2_000_000;
pub(crate) const MAX_FETCH_CHARACTERS: usize = 50_000;
pub(crate) const MAX_REDIRECTS: usize = 5;

pub(crate) const TRANSCRIPTION_API_BASE: &str = "https://api.openai.com/v1/";
pub(crate) const TRANSCRIPTION_API_KEY_SECRET: &str = "openai-api-key";
pub(crate) const TRANSCRIPTION_MODEL: &str = "gpt-4o-transcribe";
pub(crate) const TRANSCRIPTION_PROMPT: &str = "Transcribe faithfully. When discernible and relevant, include non-speech sounds, speaker changes, tone, pauses, music, and background audio in concise brackets.";
pub(crate) const TRANSCRIPTION_TIMEOUT_SECONDS: u64 = 120;
pub(crate) const MAX_AUDIO_UPLOAD_BYTES: usize = 25 * 1024 * 1024;
