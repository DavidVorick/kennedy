use std::time::Duration;

use kcode_codex_runtime::{ReasoningEffort, SearchDepth, WebSearchContext};

pub(crate) const MAX_REQUEST_BYTES: usize = 26 * 1024 * 1024;
pub(crate) const MAX_CODEX_INPUT_CHARACTERS: usize = 1_048_576;

pub(crate) const DEFAULT_PROVIDER_NAME: &str = "primary";
pub(crate) const CODEX_PROVIDER_KIND: &str = "codex";
pub(crate) const DEFAULT_MODEL: &str = "gpt-5.6-sol";
pub(crate) const GENERATION_REASONING_EFFORT: ReasoningEffort = ReasoningEffort::XHigh;
pub(crate) const GENERATION_TIMEOUT: Duration = Duration::from_secs(600);
pub(crate) const KENNEDY_CODEX_BASE_INSTRUCTION: &str = concat!(
    "Kennedy's outer harness provides tools through KENNEDY_TOOL_CALLS; ",
    "those tools are available even when absent from Codex's native tool list."
);

pub(crate) const QUALITY_SEARCH_MODEL: &str = "gpt-5.6-sol";
pub(crate) const QUALITY_SEARCH_REASONING: ReasoningEffort = ReasoningEffort::XHigh;
pub(crate) const QUALITY_SEARCH_CONTEXT: WebSearchContext = WebSearchContext::High;
pub(crate) const QUALITY_SEARCH_DEPTH: SearchDepth = SearchDepth::Thorough;
pub(crate) const QUALITY_SEARCH_TIMEOUT: Duration = Duration::from_secs(40 * 60);

pub(crate) const BALANCED_SEARCH_MODEL: &str = "gpt-5.6-terra";
pub(crate) const BALANCED_SEARCH_REASONING: ReasoningEffort = ReasoningEffort::Low;
pub(crate) const BALANCED_SEARCH_CONTEXT: WebSearchContext = WebSearchContext::Low;
pub(crate) const BALANCED_SEARCH_DEPTH: SearchDepth = SearchDepth::Focused;
pub(crate) const BALANCED_SEARCH_TIMEOUT: Duration = Duration::from_secs(180);

pub(crate) const FAST_SEARCH_TIMEOUT: Duration = Duration::from_secs(90);

pub(crate) const TRANSCRIPTION_MODEL: &str = kcode_openai_api::GPT_4O_TRANSCRIBE;
