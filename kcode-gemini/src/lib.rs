//! Gemini inference with in-memory session usage accounting and spending limits.
//!
//! [`Gemini`] intentionally supports Gemini 3.1 Flash-Lite, Gemini 3.1 Pro
//! Preview, Nano Banana Pro, Pro multimodal inference, and Flash-Lite grounded
//! search. Nothing is persisted by this crate.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod accounting;
mod api;
mod error;
mod model;

pub use accounting::{
    GroupedUsage, LimitPeriod, LimitStatus, SpendingLimits, UsageBreakdown, UsageRecord,
    UsageTotals, UsageWindow,
};
pub use api::Gemini;
pub use error::{Error, Result};
pub use model::{
    AspectRatio, CompletionStatus, CostAccuracy, CostBreakdown, GeneratedImage, GenerationOptions,
    GroundedSearchRequest, GroundedSearchResponse, InferenceRequest, InferenceResponse, MediaInput,
    MediaKind, Modality, ModalityTokens, Money, MultimodalRequest, NanoBananaProRequest,
    ServiceTier, TextModel, ThinkingLevel, TokenUsage, WebSource,
};

/// Stable Gemini 3.1 Flash-Lite model identifier.
pub const GEMINI_31_FLASH_LITE: &str = "gemini-3.1-flash-lite";
/// Current Gemini 3.1 Pro preview model identifier.
pub const GEMINI_31_PRO: &str = "gemini-3.1-pro-preview";
/// Stable Nano Banana Pro model identifier.
pub const NANO_BANANA_PRO: &str = "gemini-3-pro-image";
