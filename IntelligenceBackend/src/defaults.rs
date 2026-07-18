pub(crate) const MAX_REQUEST_BYTES: usize = 26 * 1024 * 1024;
pub(crate) const MAX_CODEX_INPUT_CHARACTERS: usize = 1_048_576;

pub(crate) const DEFAULT_PROVIDER_NAME: &str = "primary";
pub(crate) const CODEX_PROVIDER_KIND: &str = "codex";
pub(crate) const CODEX_EXECUTABLE: &str = "codex-safe";
pub(crate) const DEFAULT_MODEL: &str = "gpt-5.6-sol";
pub(crate) const GENERATION_REASONING_EFFORT: &str = "xhigh";
pub(crate) const GENERATION_TIMEOUT_SECONDS: u64 = 600;
pub(crate) const DISABLED_AUTO_COMPACT_TOKEN_LIMIT: i64 = i64::MAX;
pub(crate) const KENNEDY_CODEX_BASE_INSTRUCTION: &str = concat!(
    "Kennedy's outer harness provides tools through KENNEDY_TOOL_CALLS; ",
    "those tools are available even when absent from Codex's native tool list."
);
pub(crate) const CODEX_PROMPT_BOUNDARY_SENTINEL: &str =
    "KENNEDY_CODEX_PROMPT_BOUNDARY_SENTINEL_7F15C3A9";

pub(crate) const QUALITY_SEARCH_MODEL: &str = "gpt-5.6-sol";
pub(crate) const QUALITY_SEARCH_REASONING_EFFORT: &str = "xhigh";
pub(crate) const QUALITY_SEARCH_CONTEXT_SIZE: &str = "high";
pub(crate) const QUALITY_SEARCH_MAX_SOURCES: usize = 12;
pub(crate) const QUALITY_SEARCH_TIMEOUT_SECONDS: u64 = 40 * 60;

pub(crate) const BALANCED_SEARCH_MODEL: &str = "gpt-5.6-terra";
pub(crate) const BALANCED_SEARCH_REASONING_EFFORT: &str = "low";
pub(crate) const BALANCED_SEARCH_CONTEXT_SIZE: &str = "low";
pub(crate) const BALANCED_SEARCH_MAX_SOURCES: usize = 8;
pub(crate) const BALANCED_SEARCH_TIMEOUT_SECONDS: u64 = 180;

pub(crate) const GEMINI_SEARCH_API_BASE: &str =
    "https://generativelanguage.googleapis.com/v1beta/interactions";
pub(crate) const GEMINI_SEARCH_API_KEY_SECRET: &str = "gemini-api-key";
pub(crate) const FAST_SEARCH_MODEL: &str = "gemini-3.1-flash-lite";
pub(crate) const FAST_SEARCH_THINKING_LEVEL: &str = "low";
pub(crate) const FAST_SEARCH_SERVICE_TIER: &str = "priority";
pub(crate) const FAST_SEARCH_MAX_SOURCES: usize = 6;
pub(crate) const FAST_SEARCH_MAX_OUTPUT_TOKENS: u64 = 2_048;
pub(crate) const FAST_SEARCH_TIMEOUT_SECONDS: u64 = 90;

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

pub(crate) const MAX_DOCUMENT_UPLOAD_BYTES: usize = 20 * 1024 * 1024;
pub(crate) const MAX_DOCUMENT_CHARACTERS: usize = 1_000_000;
