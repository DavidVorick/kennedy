use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use crate::{Error, GEMINI_31_FLASH_LITE, GEMINI_31_PRO, Result};

pub(crate) const MAX_PROMPT_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_INLINE_MEDIA_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_NANO_BANANA_IMAGES: usize = 14;
pub(crate) const MAX_TEXT_OUTPUT_TOKENS: u32 = 65_536;
pub(crate) const MAX_IMAGE_OUTPUT_TOKENS: u32 = 32_768;

/// A non-negative USD amount represented exactly in billionths of one dollar.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Money(u64);

impl Money {
    /// Zero dollars.
    pub const ZERO: Self = Self(0);
    /// Constructs an amount from billionths of one US dollar.
    pub const fn from_usd_nanos(value: u64) -> Self {
        Self(value)
    }
    /// Constructs an amount from millionths of one US dollar.
    pub const fn from_usd_micros(value: u64) -> Self {
        Self(value.saturating_mul(1_000))
    }
    /// Returns the amount in billionths of one US dollar.
    pub const fn usd_nanos(self) -> u64 {
        self.0
    }
    /// Returns a display-oriented floating-point dollar value.
    pub fn as_usd(self) -> f64 {
        self.0 as f64 / 1_000_000_000.0
    }
    pub(crate) fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${:.9}", self.as_usd())
    }
}

/// Text inference models intentionally supported by this crate.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TextModel {
    /// Gemini 3.1 Flash-Lite.
    FlashLite,
    /// Gemini 3.1 Pro, currently exposed as a preview model.
    Pro,
}

impl TextModel {
    /// Returns the exact API model identifier.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FlashLite => GEMINI_31_FLASH_LITE,
            Self::Pro => GEMINI_31_PRO,
        }
    }
}

impl fmt::Display for TextModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Gemini inference service tier.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum ServiceTier {
    /// Standard synchronous pricing and scheduling.
    #[default]
    Standard,
    /// Priority scheduling at Google's higher published prices.
    Priority,
}

impl ServiceTier {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Priority => "priority",
        }
    }
}

/// Maximum model reasoning depth.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ThinkingLevel {
    /// Little to no reasoning; supported by Flash-Lite but not Pro.
    Minimal,
    /// Low reasoning depth.
    Low,
    /// Medium reasoning depth.
    Medium,
    /// High reasoning depth.
    High,
}

impl ThinkingLevel {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// Shared generation controls.
#[derive(Clone, Debug, PartialEq)]
pub struct GenerationOptions {
    /// Maximum generated token budget, including thinking on thinking models.
    pub max_output_tokens: u32,
    /// Optional sampling temperature in the inclusive range 0 through 2.
    pub temperature: Option<f32>,
    /// Optional maximum thinking depth; omission uses the model default.
    pub thinking_level: Option<ThinkingLevel>,
    /// Gemini scheduling and pricing tier.
    pub service_tier: ServiceTier,
}

impl Default for GenerationOptions {
    fn default() -> Self {
        Self {
            max_output_tokens: 8_192,
            temperature: None,
            thinking_level: None,
            service_tier: ServiceTier::Standard,
        }
    }
}

impl GenerationOptions {
    pub(crate) fn validate(&self, model: TextModel) -> Result<()> {
        validate_output_tokens(self.max_output_tokens, MAX_TEXT_OUTPUT_TOKENS)?;
        if self
            .temperature
            .is_some_and(|value| !value.is_finite() || !(0.0..=2.0).contains(&value))
        {
            return Err(Error::InvalidInput(
                "temperature must be finite and between 0 and 2".into(),
            ));
        }
        if model == TextModel::Pro && self.thinking_level == Some(ThinkingLevel::Minimal) {
            return Err(Error::InvalidInput(
                "Gemini 3.1 Pro does not support minimal thinking".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn generation_config(&self) -> Value {
        let mut value = json!({"max_output_tokens": self.max_output_tokens});
        let object = value.as_object_mut().expect("configuration is an object");
        if let Some(temperature) = self.temperature {
            object.insert("temperature".into(), json!(temperature));
        }
        if let Some(level) = self.thinking_level {
            object.insert("thinking_level".into(), json!(level.as_str()));
        }
        value
    }
}

/// One text-only inference request.
#[derive(Clone, Debug, PartialEq)]
pub struct InferenceRequest {
    /// User prompt.
    pub prompt: String,
    /// Optional system instruction.
    pub system_instruction: Option<String>,
    /// Generation controls.
    pub options: GenerationOptions,
}

impl InferenceRequest {
    /// Constructs a request with default generation controls.
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            system_instruction: None,
            options: GenerationOptions::default(),
        }
    }
}

/// Inline media category accepted by Gemini 3.1 Pro.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MediaKind {
    /// Image input.
    Image,
    /// Audio input.
    Audio,
}

impl MediaKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Audio => "audio",
        }
    }
}

/// One image or audio input held in memory for an inference request.
#[derive(Clone, Eq, PartialEq)]
pub struct MediaInput {
    kind: MediaKind,
    mime_type: String,
    data: Vec<u8>,
}

impl MediaInput {
    /// Constructs an image input after validation.
    pub fn image(mime_type: impl Into<String>, data: Vec<u8>) -> Result<Self> {
        Self::new(MediaKind::Image, mime_type.into(), data)
    }
    /// Constructs an audio input after validation.
    pub fn audio(mime_type: impl Into<String>, data: Vec<u8>) -> Result<Self> {
        Self::new(MediaKind::Audio, mime_type.into(), data)
    }
    /// Returns whether this is image or audio data.
    pub const fn kind(&self) -> MediaKind {
        self.kind
    }
    /// Returns the validated MIME type.
    pub fn mime_type(&self) -> &str {
        &self.mime_type
    }
    /// Returns the raw byte count.
    pub fn len(&self) -> usize {
        self.data.len()
    }
    /// Returns true when the input has no bytes.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn new(kind: MediaKind, mime_type: String, data: Vec<u8>) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::InvalidInput("media data must not be empty".into()));
        }
        let valid = match kind {
            MediaKind::Image => matches!(
                mime_type.as_str(),
                "image/png"
                    | "image/jpeg"
                    | "image/webp"
                    | "image/heic"
                    | "image/heif"
                    | "image/gif"
                    | "image/bmp"
                    | "image/tiff"
            ),
            MediaKind::Audio => matches!(
                mime_type.as_str(),
                "audio/wav"
                    | "audio/mp3"
                    | "audio/aiff"
                    | "audio/aac"
                    | "audio/ogg"
                    | "audio/flac"
                    | "audio/mpeg"
                    | "audio/m4a"
                    | "audio/l16"
                    | "audio/opus"
                    | "audio/alaw"
                    | "audio/mulaw"
            ),
        };
        if !valid {
            return Err(Error::InvalidInput(format!(
                "unsupported {} MIME type {mime_type:?}",
                kind.as_str()
            )));
        }
        Ok(Self {
            kind,
            mime_type,
            data,
        })
    }

    pub(crate) fn interaction_value(&self) -> Value {
        json!({
            "type": self.kind.as_str(),
            "mime_type": self.mime_type,
            "data": STANDARD.encode(&self.data),
        })
    }
}

impl fmt::Debug for MediaInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MediaInput")
            .field("kind", &self.kind)
            .field("mime_type", &self.mime_type)
            .field("bytes", &self.data.len())
            .finish()
    }
}

/// Image and/or audio inference request for Gemini 3.1 Pro.
#[derive(Clone, Debug, PartialEq)]
pub struct MultimodalRequest {
    /// User prompt accompanying the media.
    pub prompt: String,
    /// Image and audio inputs; at least one is required.
    pub media: Vec<MediaInput>,
    /// Optional system instruction.
    pub system_instruction: Option<String>,
    /// Generation controls.
    pub options: GenerationOptions,
}

impl MultimodalRequest {
    /// Constructs a request with default generation controls.
    pub fn new(prompt: impl Into<String>, media: Vec<MediaInput>) -> Self {
        Self {
            prompt: prompt.into(),
            media,
            system_instruction: None,
            options: GenerationOptions::default(),
        }
    }
}

/// Native-image aspect ratio supported by Nano Banana Pro at 2K.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum AspectRatio {
    /// 1:1 square output.
    #[default]
    Square,
    /// 3:2 landscape output.
    ThreeTwo,
    /// 2:3 portrait output.
    TwoThree,
    /// 4:3 landscape output.
    FourThree,
    /// 3:4 portrait output.
    ThreeFour,
    /// 5:4 landscape output.
    FiveFour,
    /// 4:5 portrait output.
    FourFive,
    /// 16:9 landscape output.
    SixteenNine,
    /// 9:16 portrait output.
    NineSixteen,
    /// 21:9 ultrawide output.
    TwentyOneNine,
}

impl AspectRatio {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Square => "1:1",
            Self::ThreeTwo => "3:2",
            Self::TwoThree => "2:3",
            Self::FourThree => "4:3",
            Self::ThreeFour => "3:4",
            Self::FiveFour => "5:4",
            Self::FourFive => "4:5",
            Self::SixteenNine => "16:9",
            Self::NineSixteen => "9:16",
            Self::TwentyOneNine => "21:9",
        }
    }
}

/// Nano Banana Pro generation or editing request with fixed 2K output.
#[derive(Clone, Debug, PartialEq)]
pub struct NanoBananaProRequest {
    /// Text generation or editing prompt.
    pub prompt: String,
    /// Optional image inputs for editing or visual reference.
    pub images: Vec<MediaInput>,
    /// Requested aspect ratio.
    pub aspect_ratio: AspectRatio,
    /// Generation controls; Nano Banana Pro supports Standard service only here.
    pub options: GenerationOptions,
}

impl NanoBananaProRequest {
    /// Constructs a 2K text-to-image request with defaults.
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            images: Vec::new(),
            aspect_ratio: AspectRatio::default(),
            options: GenerationOptions::default(),
        }
    }
}

/// Grounded Flash-Lite search request matching Kennedy fast search.
#[derive(Clone, Debug, PartialEq)]
pub struct GroundedSearchRequest {
    /// Research question.
    pub question: String,
    /// Maximum normalized public sources to return.
    pub max_sources: usize,
    /// Generation controls.
    pub options: GenerationOptions,
}

impl GroundedSearchRequest {
    /// Constructs a low-thinking, 2048-token Priority request.
    pub fn new(question: impl Into<String>) -> Self {
        Self {
            question: question.into(),
            max_sources: 6,
            options: GenerationOptions {
                max_output_tokens: 2_048,
                temperature: None,
                thinking_level: Some(ThinkingLevel::Low),
                service_tier: ServiceTier::Priority,
            },
        }
    }
}

/// Final interaction state returned by Gemini.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CompletionStatus {
    /// The turn completed normally.
    Completed,
    /// The turn returned usable partial output.
    Incomplete,
}

/// A generated image returned by Nano Banana Pro.
#[derive(Clone, Eq, PartialEq)]
pub struct GeneratedImage {
    /// Returned image MIME type.
    pub mime_type: String,
    /// Decoded image bytes.
    pub data: Vec<u8>,
}

impl fmt::Debug for GeneratedImage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeneratedImage")
            .field("mime_type", &self.mime_type)
            .field("bytes", &self.data.len())
            .finish()
    }
}

/// Provider modality used for token accounting.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Modality {
    /// Text tokens.
    Text,
    /// Image tokens.
    Image,
    /// Audio tokens.
    Audio,
    /// Video tokens.
    Video,
    /// Document tokens, billed at image rates.
    Document,
    /// A provider modality unknown to this version.
    Unknown,
}

impl Modality {
    pub(crate) fn parse(value: &str) -> Self {
        match value {
            "text" => Self::Text,
            "image" => Self::Image,
            "audio" => Self::Audio,
            "video" => Self::Video,
            "document" => Self::Document,
            _ => Self::Unknown,
        }
    }
}

/// One modality-specific token count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModalityTokens {
    /// Token modality.
    pub modality: Modality,
    /// Token count.
    pub tokens: u64,
}

/// Token usage reported for one interaction.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TokenUsage {
    /// Total effective input tokens.
    pub input_tokens: u64,
    /// Cached input tokens.
    pub cached_tokens: u64,
    /// Visible output tokens.
    pub output_tokens: u64,
    /// Internal thinking tokens billed at output rates.
    pub thought_tokens: u64,
    /// Tool-use prompt tokens reported separately.
    pub tool_use_tokens: u64,
    /// Total tokens reported by Gemini.
    pub total_tokens: u64,
    /// Input counts by modality.
    pub input_by_modality: Vec<ModalityTokens>,
    /// Cached input counts by modality.
    pub cached_by_modality: Vec<ModalityTokens>,
    /// Output counts by modality.
    pub output_by_modality: Vec<ModalityTokens>,
    /// Tool-use prompt counts by modality.
    pub tool_use_by_modality: Vec<ModalityTokens>,
    /// Reported Google Search query count.
    pub grounding_search_queries: u64,
}

impl TokenUsage {
    pub(crate) fn modality_total(values: &[ModalityTokens], modality: Modality) -> u64 {
        values
            .iter()
            .filter(|entry| entry.modality == modality)
            .map(|entry| entry.tokens)
            .sum()
    }

    pub(crate) fn saturating_add(&mut self, other: &Self) {
        self.input_tokens = self.input_tokens.saturating_add(other.input_tokens);
        self.cached_tokens = self.cached_tokens.saturating_add(other.cached_tokens);
        self.output_tokens = self.output_tokens.saturating_add(other.output_tokens);
        self.thought_tokens = self.thought_tokens.saturating_add(other.thought_tokens);
        self.tool_use_tokens = self.tool_use_tokens.saturating_add(other.tool_use_tokens);
        self.total_tokens = self.total_tokens.saturating_add(other.total_tokens);
        self.grounding_search_queries = self
            .grounding_search_queries
            .saturating_add(other.grounding_search_queries);
        add_modalities(&mut self.input_by_modality, &other.input_by_modality);
        add_modalities(&mut self.cached_by_modality, &other.cached_by_modality);
        add_modalities(&mut self.output_by_modality, &other.output_by_modality);
        add_modalities(&mut self.tool_use_by_modality, &other.tool_use_by_modality);
    }
}

fn add_modalities(target: &mut Vec<ModalityTokens>, source: &[ModalityTokens]) {
    for entry in source {
        if let Some(existing) = target
            .iter_mut()
            .find(|value| value.modality == entry.modality)
        {
            existing.tokens = existing.tokens.saturating_add(entry.tokens);
        } else {
            target.push(entry.clone());
        }
    }
}

/// Quality of a locally calculated cost.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CostAccuracy {
    /// Complete provider usage mapped directly to published rates.
    Exact,
    /// Missing modality detail required a documented fallback.
    Estimated,
    /// Grounding cost assumes every reported query is chargeable.
    Conservative,
}

/// Locally calculated cost for one request or aggregate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CostBreakdown {
    /// Non-cached input cost.
    pub input: Money,
    /// Cached input cost.
    pub cached_input: Money,
    /// Visible text output plus thinking cost.
    pub text_output_and_thinking: Money,
    /// Native image output cost.
    pub image_output: Money,
    /// Conservative Search grounding cost.
    pub grounding: Money,
    /// Sum of cost components.
    pub total: Money,
    /// Cost calculation quality.
    pub accuracy: CostAccuracy,
    /// Compiled pricing-table identifier.
    pub pricing_version: String,
}

impl Default for CostBreakdown {
    fn default() -> Self {
        Self {
            input: Money::ZERO,
            cached_input: Money::ZERO,
            text_output_and_thinking: Money::ZERO,
            image_output: Money::ZERO,
            grounding: Money::ZERO,
            total: Money::ZERO,
            accuracy: CostAccuracy::Exact,
            pricing_version: "google-gemini-2026-07-20".into(),
        }
    }
}

impl CostBreakdown {
    pub(crate) fn saturating_add(&mut self, other: &Self) {
        self.input = self.input.saturating_add(other.input);
        self.cached_input = self.cached_input.saturating_add(other.cached_input);
        self.text_output_and_thinking = self
            .text_output_and_thinking
            .saturating_add(other.text_output_and_thinking);
        self.image_output = self.image_output.saturating_add(other.image_output);
        self.grounding = self.grounding.saturating_add(other.grounding);
        self.total = self.total.saturating_add(other.total);
        if other.accuracy == CostAccuracy::Conservative {
            self.accuracy = CostAccuracy::Conservative;
        } else if other.accuracy == CostAccuracy::Estimated && self.accuracy == CostAccuracy::Exact
        {
            self.accuracy = CostAccuracy::Estimated;
        }
    }
}

/// A successful text or image interaction.
#[derive(Clone, Debug, PartialEq)]
pub struct InferenceResponse {
    /// Gemini interaction identifier.
    pub id: String,
    /// Actual model identifier returned by Gemini.
    pub model: String,
    /// Completion state.
    pub status: CompletionStatus,
    /// Concatenated text output, when present.
    pub text: Option<String>,
    /// Decoded native image output.
    pub images: Vec<GeneratedImage>,
    /// Provider-reported usage.
    pub usage: TokenUsage,
    /// Locally calculated cost.
    pub cost: CostBreakdown,
    /// In-memory session usage-record identifier.
    pub usage_record_id: u64,
}

/// One public source returned by grounded search.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebSource {
    /// Sanitized source title.
    pub title: String,
    /// Public HTTP(S) URL without a fragment.
    pub url: String,
}

/// Successful grounded-search output.
#[derive(Clone, Debug, PartialEq)]
pub struct GroundedSearchResponse {
    /// Normalized interaction response.
    pub interaction: InferenceResponse,
    /// Deduplicated citations and search results.
    pub sources: Vec<WebSource>,
}

pub(crate) fn validate_prompt(prompt: &str) -> Result<()> {
    if prompt.trim().is_empty() {
        return Err(Error::InvalidInput("prompt must not be empty".into()));
    }
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(Error::InvalidInput(format!(
            "prompt exceeds the {MAX_PROMPT_BYTES}-byte limit"
        )));
    }
    Ok(())
}

pub(crate) fn validate_system_instruction(value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        if value.trim().is_empty() {
            return Err(Error::InvalidInput(
                "system_instruction must be omitted instead of empty".into(),
            ));
        }
        if value.len() > MAX_PROMPT_BYTES {
            return Err(Error::InvalidInput(format!(
                "system_instruction exceeds the {MAX_PROMPT_BYTES}-byte limit"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_media(media: &[MediaInput], required: bool) -> Result<()> {
    if required && media.is_empty() {
        return Err(Error::InvalidInput(
            "multimodal inference requires at least one media input".into(),
        ));
    }
    let total = media.iter().try_fold(0_usize, |total, input| {
        total
            .checked_add(input.len())
            .ok_or_else(|| Error::InvalidInput("aggregate media size overflowed".into()))
    })?;
    if total > MAX_INLINE_MEDIA_BYTES {
        return Err(Error::InvalidInput(format!(
            "aggregate inline media exceeds the {MAX_INLINE_MEDIA_BYTES}-byte safety limit"
        )));
    }
    Ok(())
}

pub(crate) fn validate_output_tokens(value: u32, maximum: u32) -> Result<()> {
    if value == 0 || value > maximum {
        return Err(Error::InvalidInput(format!(
            "max_output_tokens must be between 1 and {maximum}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_debug_redacts_bytes() {
        let media = MediaInput::image("image/png", b"sensitive bytes".to_vec()).unwrap();
        let debug = format!("{media:?}");
        assert!(debug.contains("bytes: 15"));
        assert!(!debug.contains("sensitive"));
    }

    #[test]
    fn pro_rejects_minimal_thinking() {
        let options = GenerationOptions {
            thinking_level: Some(ThinkingLevel::Minimal),
            ..GenerationOptions::default()
        };
        assert!(options.validate(TextModel::Pro).is_err());
        assert!(options.validate(TextModel::FlashLite).is_ok());
    }

    #[test]
    fn media_uses_current_interactions_schema() {
        let value = MediaInput::audio("audio/wav", vec![1, 2, 3])
            .unwrap()
            .interaction_value();
        assert_eq!(value["type"], "audio");
        assert_eq!(value["mime_type"], "audio/wav");
        assert_eq!(value["data"], "AQID");
    }

    #[test]
    fn nano_banana_aspect_ratios_include_five_four_pair() {
        assert_eq!(AspectRatio::FiveFour.as_str(), "5:4");
        assert_eq!(AspectRatio::FourFive.as_str(), "4:5");
    }
}
