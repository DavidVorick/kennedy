//! Durable, provider-independent session context.
//!
//! A [`SessionJournal`] is the sole authority for an in-progress session.  It
//! stores JSON transitions and raw object bodies in one framed append-only
//! file.  [`Chatend`] is a deterministic materialized view of those records.

use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

use anyhow::{Context as _, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"KCHAT01\n";
const FRAME_HEADER_BYTES: u64 = 1 + 8 + 32;
const JSON_FRAME: u8 = 1;
const OBJECT_FRAME: u8 = 2;
const OBJECT_PREFIX_BYTES: usize = 8 + 4;

pub const FORMAT_VERSION: u32 = 1;
pub const MAX_OBJECT_BYTES: u64 = 32 * 1024 * 1024 * 1024;
pub const ESTIMATED_CHARACTERS_PER_TOKEN: u64 = 3;

#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct EventId(pub u64);

impl std::fmt::Display for EventId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct BoxId(pub u64);

impl std::fmt::Display for BoxId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PendingId(String);

impl PendingId {
    pub fn from_event(id: EventId) -> Self {
        Self(format!("pending:{}", id.0))
    }

    pub fn parse(value: impl Into<String>) -> anyhow::Result<Self> {
        let value = value.into();
        let number = value
            .strip_prefix("pending:")
            .context("pending identity must begin with `pending:`")?
            .parse::<u64>()
            .context("pending identity must end in an unsigned integer")?;
        ensure!(number > 0, "pending identity zero is reserved");
        Ok(Self(value))
    }

    pub fn number(&self) -> u64 {
        self.0["pending:".len()..]
            .parse()
            .expect("validated PendingId")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PendingId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    Conversation,
    Telegram,
    TelegramGroup,
    SelfTime,
    AudioIngress,
    HistoryIngress,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionMetadata {
    pub session_id: String,
    pub kind: SessionKind,
    pub created_at: String,
    pub effective_context_tokens: u64,
    #[serde(default)]
    pub channel: Value,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BoxOwner {
    User,
    Kennedy,
    Controller,
    System,
    Tool { tool_instance: String, slot: String },
}

impl BoxOwner {
    fn label(&self) -> String {
        match self {
            Self::User => "user".into(),
            Self::Kennedy => "kennedy".into(),
            Self::Controller => "controller".into(),
            Self::System => "system".into(),
            Self::Tool {
                tool_instance,
                slot,
            } => format!("tool:{tool_instance}:{slot}"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BoxContent {
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub objects: Vec<String>,
    #[serde(default)]
    pub metadata: Value,
}

impl BoxContent {
    pub fn text(value: impl Into<String>) -> Self {
        Self {
            text: value.into(),
            ..Self::default()
        }
    }

    fn render(&self) -> String {
        let mut rendered = self.text.clone();
        for object in &self.objects {
            if !rendered.is_empty() && !rendered.ends_with('\n') {
                rendered.push('\n');
            }
            rendered.push_str("Object provided: ");
            rendered.push_str(object);
        }
        rendered
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Representation {
    Hydrated { canonical_event: EventId },
    Dehydrated { based_on: EventId },
    Summarized { based_on: EventId, text: String },
}

impl Representation {
    fn based_on(&self) -> EventId {
        match self {
            Self::Hydrated { canonical_event } => *canonical_event,
            Self::Dehydrated { based_on } | Self::Summarized { based_on, .. } => *based_on,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalRevision {
    pub event_id: EventId,
    pub content: BoxContent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BoxState {
    pub id: BoxId,
    pub name: String,
    pub owner: BoxOwner,
    pub created_at: EventId,
    pub canonical: CanonicalRevision,
    pub representation: Representation,
    pub occurrence_events: Vec<EventId>,
    pub active: bool,
}

impl BoxState {
    pub fn stale(&self) -> bool {
        !matches!(self.representation, Representation::Hydrated { .. })
            && self.representation.based_on() != self.canonical.event_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingKind {
    Node,
    Object,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventKind {
    SessionConfigured {
        effective_context_tokens: u64,
        kind: SessionKind,
    },
    BoxCreated {
        box_id: BoxId,
        name: String,
        owner: BoxOwner,
        content: BoxContent,
    },
    CanonicalUpdated {
        box_id: BoxId,
        content: BoxContent,
    },
    BoxDehydrated {
        box_id: BoxId,
    },
    BoxSummarized {
        box_id: BoxId,
        text: String,
    },
    BoxRehydrated {
        box_id: BoxId,
    },
    BoxRetired {
        box_id: BoxId,
    },
    PendingAllocated {
        pending_id: PendingId,
        resource: PendingKind,
    },
    ToolInvoked {
        tool_instance: String,
        tool_name: String,
        arguments: Value,
    },
    ToolCompleted {
        tool_instance: String,
        tool_name: String,
        outcome: Value,
    },
    InferenceSubmitted {
        manifest_hash: String,
        estimated_input_tokens: u64,
        #[serde(default)]
        raw_estimated_input_tokens: Option<u64>,
    },
    ProviderReceipt {
        manifest_hash: String,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        provider_data: Value,
    },
    CapacityError {
        attempted_operation: String,
        projected_tokens: u64,
        limit_tokens: u64,
    },
    SourceTerminated {
        reason: String,
    },
    HistoryIngressStarted,
    HistoryEventInspected {
        source_event: EventId,
    },
    HistoryEventReleased {
        source_event: EventId,
    },
    KwebPlanChanged {
        operation: Value,
    },
    KwebCommitted {
        transaction_id: String,
        session_object_id: String,
        mappings: Value,
    },
    SessionCompleted {
        session_object_id: String,
    },
    Note {
        label: String,
        value: Value,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub id: EventId,
    pub recorded_at: String,
    pub kind: EventKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transition {
    pub recorded_at: String,
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum JsonRecord {
    SessionOpened {
        format_version: u32,
        metadata: SessionMetadata,
    },
    Transition {
        transition: Transition,
    },
    Sideband {
        kind: String,
        recorded_at: String,
        value: Value,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SidebandRecord {
    pub kind: String,
    pub recorded_at: String,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMetadata {
    pub pending_id: PendingId,
    pub event_id: EventId,
    pub recorded_at: String,
    pub media_type: String,
    pub file_name: Option<String>,
    #[serde(default)]
    pub transport: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectLocation {
    pub metadata: ObjectMetadata,
    pub payload_offset: u64,
    pub payload_len: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolSlot {
    pub slot: String,
    pub box_id: BoxId,
    pub retired: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolState {
    pub slots: Vec<ToolSlot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolSlotInput {
    pub slot: String,
    pub name: String,
    pub content: BoxContent,
    pub retired: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Chatend {
    pub metadata: SessionMetadata,
    pub next_id: u64,
    pub events: Vec<Event>,
    pub boxes: BTreeMap<BoxId, BoxState>,
    pub pending: BTreeMap<PendingId, PendingKind>,
    pub tools: BTreeMap<String, ToolState>,
    pub source_terminated: bool,
    pub history_ingress_started: bool,
    pub completed_session_object: Option<String>,
}

impl Chatend {
    fn opened(metadata: SessionMetadata) -> Self {
        Self {
            metadata,
            next_id: 1,
            events: Vec::new(),
            boxes: BTreeMap::new(),
            pending: BTreeMap::new(),
            tools: BTreeMap::new(),
            source_terminated: false,
            history_ingress_started: false,
            completed_session_object: None,
        }
    }

    pub fn event(&self, id: EventId) -> Option<&Event> {
        self.events.iter().find(|event| event.id == id)
    }

    pub fn box_state(&self, id: BoxId) -> Option<&BoxState> {
        self.boxes.get(&id)
    }

    pub fn active_boxes(&self) -> impl Iterator<Item = &BoxState> {
        self.boxes.values().filter(|state| state.active)
    }

    pub fn live_context_limit(&self) -> u64 {
        self.metadata.effective_context_tokens.saturating_mul(70) / 100
    }

    pub fn emergency_context_limit(&self) -> u64 {
        self.metadata.effective_context_tokens.saturating_mul(72) / 100
    }

    pub fn history_context_limit(&self) -> u64 {
        self.metadata.effective_context_tokens
    }

    pub fn projection(&self) -> ContextProjection {
        let mut next_occurrence = HashMap::new();
        for state in self.boxes.values() {
            for pair in state.occurrence_events.windows(2) {
                next_occurrence.insert(pair[0], pair[1]);
            }
        }
        let mut items = Vec::new();
        for event in &self.events {
            let Some(box_id) = event_box_id(&event.kind) else {
                continue;
            };
            let Some(state) = self.boxes.get(&box_id) else {
                continue;
            };
            if let Some(next) = next_occurrence.get(&event.id) {
                let text = format!(
                    "[box {} / {} continued at event {}]",
                    box_id, state.name, next
                );
                items.push(ProjectionItem::marker(event.id, box_id, text));
                continue;
            }
            if !state.active || state.occurrence_events.last() != Some(&event.id) {
                continue;
            }
            let (representation, body) = match &state.representation {
                Representation::Hydrated { .. } => ("hydrated", state.canonical.content.render()),
                Representation::Dehydrated { .. } => (
                    "dehydrated",
                    format!(
                        "[contents dehydrated; hydrate box {} to inspect the latest canonical revision]",
                        box_id
                    ),
                ),
                Representation::Summarized { text, .. } => ("summarized", text.clone()),
            };
            let stale = state.stale();
            let mut text = format!(
                "[box {} | {} | owner={} | {}{}]\n{}",
                box_id,
                state.name,
                state.owner.label(),
                representation,
                if stale { " | stale" } else { "" },
                body
            );
            if text.ends_with('\n') {
                text.pop();
            }
            items.push(ProjectionItem {
                event_id: event.id,
                box_id,
                marker: false,
                stale,
                approximate_tokens: estimate_tokens(&text),
                text,
            });
        }
        let stale_boxes = self
            .active_boxes()
            .filter(|state| state.stale())
            .map(|state| state.id)
            .collect::<Vec<_>>();
        let body_tokens = items
            .iter()
            .map(|item| item.approximate_tokens)
            .sum::<u64>();
        let preliminary_footer = format!(
            "[context budget | estimated={} | live_limit={} | effective={}]\n[stale boxes: {}]",
            body_tokens,
            self.live_context_limit(),
            self.metadata.effective_context_tokens,
            if stale_boxes.is_empty() {
                "none".into()
            } else {
                stale_boxes
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        );
        let preliminary_raw = body_tokens.saturating_add(estimate_tokens(&preliminary_footer));
        let preliminary_estimate = self.calibrated_estimate(preliminary_raw);
        let footer = format!(
            "[context budget | estimated={} | live_limit={} | effective={}]\n[stale boxes: {}]",
            preliminary_estimate,
            self.live_context_limit(),
            self.metadata.effective_context_tokens,
            if stale_boxes.is_empty() {
                "none".into()
            } else {
                stale_boxes
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        );
        let raw_estimated_tokens = body_tokens.saturating_add(estimate_tokens(&footer));
        let estimated_tokens = self.calibrated_estimate(raw_estimated_tokens);
        ContextProjection {
            items,
            stale_boxes,
            footer,
            estimated_tokens,
            raw_estimated_tokens,
        }
    }

    fn calibrated_estimate(&self, raw_current: u64) -> u64 {
        let Some((manifest_hash, measured)) = self.events.iter().rev().find_map(|event| {
            let EventKind::ProviderReceipt {
                manifest_hash,
                input_tokens: Some(input_tokens),
                ..
            } = &event.kind
            else {
                return None;
            };
            Some((manifest_hash, *input_tokens))
        }) else {
            return raw_current;
        };
        let Some(raw_at_measurement) = self.events.iter().rev().find_map(|event| {
            let EventKind::InferenceSubmitted {
                manifest_hash: submitted,
                estimated_input_tokens,
                raw_estimated_input_tokens,
            } = &event.kind
            else {
                return None;
            };
            (submitted == manifest_hash)
                .then_some(raw_estimated_input_tokens.unwrap_or(*estimated_input_tokens))
        }) else {
            return raw_current;
        };
        if raw_current >= raw_at_measurement {
            measured.saturating_add(raw_current - raw_at_measurement)
        } else {
            measured.saturating_sub(raw_at_measurement - raw_current)
        }
    }

    pub fn history_skeleton(&self) -> Vec<HistorySkeletonItem> {
        self.events
            .iter()
            .map(|event| {
                let (label, approximate_tokens) = match &event.kind {
                    EventKind::BoxCreated { name, content, .. } => (
                        format!("box created: {name}"),
                        estimate_tokens(&content.render()),
                    ),
                    EventKind::CanonicalUpdated { content, .. } => (
                        "canonical box update".into(),
                        estimate_tokens(&content.render()),
                    ),
                    kind => (
                        event_kind_label(kind).into(),
                        estimate_tokens(
                            &serde_json::to_string(kind).unwrap_or_else(|_| "{}".into()),
                        ),
                    ),
                };
                HistorySkeletonItem {
                    event_id: event.id,
                    label,
                    approximate_hydration_tokens: approximate_tokens,
                }
            })
            .collect()
    }

    pub fn render(&self) -> String {
        let projection = self.projection();
        let mut blocks = projection
            .items
            .iter()
            .map(|item| item.text.as_str())
            .collect::<Vec<_>>();
        blocks.push(&projection.footer);
        blocks.join("\n\n")
    }

    fn apply_transition(&mut self, transition: &Transition) -> anyhow::Result<()> {
        ensure!(
            !transition.events.is_empty(),
            "a transition cannot be empty"
        );
        let mut candidate = self.clone();
        for event in &transition.events {
            candidate.apply_event(event)?;
        }
        *self = candidate;
        Ok(())
    }

    fn apply_event(&mut self, event: &Event) -> anyhow::Result<()> {
        ensure!(
            event.id.0 >= self.next_id,
            "event {} reuses an allocated identity (next is {})",
            event.id,
            self.next_id
        );
        self.next_id = event.id.0.checked_add(1).context("event ID overflow")?;
        match &event.kind {
            EventKind::SessionConfigured {
                effective_context_tokens,
                kind,
            } => {
                ensure!(
                    *effective_context_tokens > 0,
                    "effective context window must be positive"
                );
                self.metadata.effective_context_tokens = *effective_context_tokens;
                self.metadata.kind = kind.clone();
            }
            EventKind::BoxCreated {
                box_id,
                name,
                owner,
                content,
            } => {
                ensure!(
                    box_id.0 == event.id.0,
                    "BoxId must equal its creation EventId"
                );
                ensure!(
                    !self.boxes.contains_key(box_id),
                    "box {} already exists",
                    box_id
                );
                self.boxes.insert(
                    *box_id,
                    BoxState {
                        id: *box_id,
                        name: name.clone(),
                        owner: owner.clone(),
                        created_at: event.id,
                        canonical: CanonicalRevision {
                            event_id: event.id,
                            content: content.clone(),
                        },
                        representation: Representation::Hydrated {
                            canonical_event: event.id,
                        },
                        occurrence_events: vec![event.id],
                        active: true,
                    },
                );
            }
            EventKind::CanonicalUpdated { box_id, content } => {
                let state = active_box_mut(&mut self.boxes, *box_id)?;
                state.canonical = CanonicalRevision {
                    event_id: event.id,
                    content: content.clone(),
                };
                if matches!(state.representation, Representation::Hydrated { .. }) {
                    state.representation = Representation::Hydrated {
                        canonical_event: event.id,
                    };
                }
                state.occurrence_events.push(event.id);
            }
            EventKind::BoxDehydrated { box_id } => {
                let state = active_box_mut(&mut self.boxes, *box_id)?;
                state.representation = Representation::Dehydrated {
                    based_on: state.canonical.event_id,
                };
                state.occurrence_events.push(event.id);
            }
            EventKind::BoxSummarized { box_id, text } => {
                ensure!(!text.trim().is_empty(), "a box summary cannot be empty");
                let state = active_box_mut(&mut self.boxes, *box_id)?;
                state.representation = Representation::Summarized {
                    based_on: state.canonical.event_id,
                    text: text.clone(),
                };
                state.occurrence_events.push(event.id);
            }
            EventKind::BoxRehydrated { box_id } => {
                let state = active_box_mut(&mut self.boxes, *box_id)?;
                state.representation = Representation::Hydrated {
                    canonical_event: state.canonical.event_id,
                };
                state.occurrence_events.push(event.id);
            }
            EventKind::BoxRetired { box_id } => {
                let state = active_box_mut(&mut self.boxes, *box_id)?;
                state.active = false;
                state.occurrence_events.push(event.id);
            }
            EventKind::PendingAllocated {
                pending_id,
                resource,
            } => {
                ensure!(
                    pending_id.number() == event.id.0,
                    "pending identity must equal its allocation EventId"
                );
                ensure!(
                    self.pending
                        .insert(pending_id.clone(), resource.clone())
                        .is_none(),
                    "pending identity {} already exists",
                    pending_id
                );
            }
            EventKind::SourceTerminated { .. } => self.source_terminated = true,
            EventKind::HistoryIngressStarted => {
                ensure!(
                    self.source_terminated,
                    "history ingress requires source termination"
                );
                self.history_ingress_started = true;
            }
            EventKind::SessionCompleted { session_object_id } => {
                self.completed_session_object = Some(session_object_id.clone());
            }
            EventKind::ToolInvoked { .. }
            | EventKind::ToolCompleted { .. }
            | EventKind::InferenceSubmitted { .. }
            | EventKind::ProviderReceipt { .. }
            | EventKind::CapacityError { .. }
            | EventKind::HistoryEventInspected { .. }
            | EventKind::HistoryEventReleased { .. }
            | EventKind::KwebPlanChanged { .. }
            | EventKind::KwebCommitted { .. }
            | EventKind::Note { .. } => {}
        }
        self.events.push(event.clone());
        Ok(())
    }
}

fn active_box_mut(
    boxes: &mut BTreeMap<BoxId, BoxState>,
    box_id: BoxId,
) -> anyhow::Result<&mut BoxState> {
    let state = boxes
        .get_mut(&box_id)
        .with_context(|| format!("box {box_id} does not exist"))?;
    ensure!(state.active, "box {box_id} is retired");
    Ok(state)
}

fn event_box_id(kind: &EventKind) -> Option<BoxId> {
    match kind {
        EventKind::BoxCreated { box_id, .. }
        | EventKind::CanonicalUpdated { box_id, .. }
        | EventKind::BoxDehydrated { box_id }
        | EventKind::BoxSummarized { box_id, .. }
        | EventKind::BoxRehydrated { box_id }
        | EventKind::BoxRetired { box_id } => Some(*box_id),
        _ => None,
    }
}

fn event_kind_label(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::SessionConfigured { .. } => "session configured",
        EventKind::BoxCreated { .. } => "box created",
        EventKind::CanonicalUpdated { .. } => "canonical box update",
        EventKind::BoxDehydrated { .. } => "box dehydrated",
        EventKind::BoxSummarized { .. } => "box summarized",
        EventKind::BoxRehydrated { .. } => "box rehydrated",
        EventKind::BoxRetired { .. } => "box retired",
        EventKind::PendingAllocated { .. } => "pending identity allocated",
        EventKind::ToolInvoked { .. } => "tool invoked",
        EventKind::ToolCompleted { .. } => "tool completed",
        EventKind::InferenceSubmitted { .. } => "inference submitted",
        EventKind::ProviderReceipt { .. } => "provider receipt",
        EventKind::CapacityError { .. } => "context capacity error",
        EventKind::SourceTerminated { .. } => "source session terminated",
        EventKind::HistoryIngressStarted => "history ingress started",
        EventKind::HistoryEventInspected { .. } => "history event inspected",
        EventKind::HistoryEventReleased { .. } => "history event released",
        EventKind::KwebPlanChanged { .. } => "Kweb plan changed",
        EventKind::KwebCommitted { .. } => "Kweb transaction committed",
        EventKind::SessionCompleted { .. } => "session completed",
        EventKind::Note { .. } => "session note",
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionItem {
    pub event_id: EventId,
    pub box_id: BoxId,
    pub marker: bool,
    pub stale: bool,
    pub approximate_tokens: u64,
    pub text: String,
}

impl ProjectionItem {
    fn marker(event_id: EventId, box_id: BoxId, text: String) -> Self {
        Self {
            event_id,
            box_id,
            marker: true,
            stale: false,
            approximate_tokens: estimate_tokens(&text),
            text,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextProjection {
    pub items: Vec<ProjectionItem>,
    pub stale_boxes: Vec<BoxId>,
    pub footer: String,
    pub estimated_tokens: u64,
    pub raw_estimated_tokens: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistorySkeletonItem {
    pub event_id: EventId,
    pub label: String,
    pub approximate_hydration_tokens: u64,
}

pub fn estimate_tokens(text: &str) -> u64 {
    (text.chars().count() as u64).div_ceil(ESTIMATED_CHARACTERS_PER_TOKEN)
}

pub struct SessionJournal {
    path: PathBuf,
    file: File,
    chatend: Chatend,
    objects: BTreeMap<PendingId, ObjectLocation>,
    sidebands: Vec<SidebandRecord>,
    append_lock: Arc<Mutex<()>>,
}

impl SessionJournal {
    pub fn create(path: impl AsRef<Path>, metadata: SessionMetadata) -> anyhow::Result<Self> {
        ensure!(
            metadata.effective_context_tokens > 0,
            "effective context window must be positive"
        );
        let path = path.as_ref().to_path_buf();
        let append_lock = journal_lock(&path);
        let _append_guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session journal append lock is poisoned"))?;
        if let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("creating {}", path.display()))?;
        file.write_all(MAGIC)?;
        let opened = JsonRecord::SessionOpened {
            format_version: FORMAT_VERSION,
            metadata: metadata.clone(),
        };
        append_frame(&mut file, JSON_FRAME, &serde_json::to_vec(&opened)?)?;
        file.sync_all()?;
        sync_parent_directory(&path)?;
        drop(_append_guard);
        Ok(Self {
            path,
            file,
            chatend: Chatend::opened(metadata),
            objects: BTreeMap::new(),
            sidebands: Vec::new(),
            append_lock,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let append_lock = journal_lock(&path);
        let _append_guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session journal append lock is poisoned"))?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        let mut magic = [0; MAGIC.len()];
        file.read_exact(&mut magic)
            .with_context(|| format!("reading header from {}", path.display()))?;
        ensure!(
            &magic == MAGIC,
            "{} is not a Chatend journal",
            path.display()
        );
        let file_len = file.metadata()?.len();
        let mut cursor = MAGIC.len() as u64;
        let mut chatend = None;
        let mut objects = BTreeMap::new();
        let mut sidebands = Vec::new();
        while cursor < file_len {
            let remaining = file_len - cursor;
            if remaining < FRAME_HEADER_BYTES {
                file.set_len(cursor)?;
                break;
            }
            file.seek(SeekFrom::Start(cursor))?;
            let mut header = [0; FRAME_HEADER_BYTES as usize];
            file.read_exact(&mut header)?;
            let kind = header[0];
            let payload_len = u64::from_le_bytes(header[1..9].try_into().unwrap());
            let frame_end = cursor
                .checked_add(FRAME_HEADER_BYTES)
                .and_then(|value| value.checked_add(payload_len))
                .context("journal frame length overflow")?;
            if frame_end > file_len {
                file.set_len(cursor)?;
                break;
            }
            match kind {
                JSON_FRAME => {
                    let payload_len_usize = usize::try_from(payload_len)
                        .context("journal JSON frame does not fit address space")?;
                    let mut payload = vec![0; payload_len_usize];
                    file.read_exact(&mut payload)?;
                    let checksum = Sha256::digest(&payload);
                    ensure!(
                        checksum.as_slice() == &header[9..],
                        "checksum mismatch in complete frame at byte {cursor}"
                    );
                    let record: JsonRecord = serde_json::from_slice(&payload)
                        .with_context(|| format!("decoding JSON frame at byte {cursor}"))?;
                    match record {
                        JsonRecord::SessionOpened {
                            format_version,
                            metadata,
                        } => {
                            ensure!(cursor == MAGIC.len() as u64, "duplicate session header");
                            ensure!(
                                format_version == FORMAT_VERSION,
                                "unsupported Chatend journal version {format_version}"
                            );
                            chatend = Some(Chatend::opened(metadata));
                        }
                        JsonRecord::Transition { transition } => chatend
                            .as_mut()
                            .context("transition precedes session header")?
                            .apply_transition(&transition)?,
                        JsonRecord::Sideband {
                            kind,
                            recorded_at,
                            value,
                        } => sidebands.push(SidebandRecord {
                            kind,
                            recorded_at,
                            value,
                        }),
                    }
                }
                OBJECT_FRAME => {
                    ensure!(
                        payload_len >= OBJECT_PREFIX_BYTES as u64,
                        "raw object frame is shorter than its prefix"
                    );
                    let mut prefix = [0_u8; OBJECT_PREFIX_BYTES];
                    file.read_exact(&mut prefix)?;
                    let event_id = EventId(u64::from_le_bytes(prefix[..8].try_into().unwrap()));
                    let metadata_len =
                        u32::from_le_bytes(prefix[8..12].try_into().unwrap()) as usize;
                    let body_start = OBJECT_PREFIX_BYTES
                        .checked_add(metadata_len)
                        .context("object metadata length overflow")?;
                    ensure!(
                        body_start as u64 <= payload_len,
                        "raw object frame has truncated metadata"
                    );
                    let mut metadata_bytes = vec![0_u8; metadata_len];
                    file.read_exact(&mut metadata_bytes)?;
                    let metadata: ObjectMetadata = serde_json::from_slice(&metadata_bytes)?;
                    ensure!(
                        metadata.event_id == event_id,
                        "raw object prefix and metadata event IDs differ"
                    );
                    ensure!(
                        metadata.pending_id.number() == event_id.0,
                        "raw object pending identity differs from its event ID"
                    );
                    let body_len = payload_len
                        .checked_sub(body_start as u64)
                        .context("invalid raw object frame length")?;
                    ensure!(body_len <= MAX_OBJECT_BYTES, "staged object exceeds 32 GiB");
                    let payload_offset = cursor + FRAME_HEADER_BYTES + body_start as u64;
                    let mut hasher = Sha256::new();
                    hasher.update(prefix);
                    hasher.update(&metadata_bytes);
                    let mut remaining = body_len;
                    let mut buffer = vec![0_u8; 1024 * 1024];
                    while remaining > 0 {
                        let take = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
                        file.read_exact(&mut buffer[..take])?;
                        hasher.update(&buffer[..take]);
                        remaining -= take as u64;
                    }
                    let checksum = hasher.finalize();
                    ensure!(
                        checksum.as_slice() == &header[9..],
                        "checksum mismatch in complete frame at byte {cursor}"
                    );
                    let state = chatend.as_mut().context("object precedes session header")?;
                    let allocation = Event {
                        id: metadata.event_id,
                        recorded_at: metadata.recorded_at.clone(),
                        kind: EventKind::PendingAllocated {
                            pending_id: metadata.pending_id.clone(),
                            resource: PendingKind::Object,
                        },
                    };
                    state.apply_transition(&Transition {
                        recorded_at: metadata.recorded_at.clone(),
                        events: vec![allocation],
                    })?;
                    let location = ObjectLocation {
                        metadata: metadata.clone(),
                        payload_offset,
                        payload_len: body_len,
                    };
                    ensure!(
                        objects
                            .insert(metadata.pending_id.clone(), location)
                            .is_none(),
                        "duplicate staged object {}",
                        metadata.pending_id
                    );
                }
                other => bail!("unknown complete journal frame kind {other} at byte {cursor}"),
            }
            cursor = frame_end;
        }
        let mut chatend = chatend.context("journal has no session header")?;
        chatend.tools = derive_tool_states(&chatend);
        file.seek(SeekFrom::End(0))?;
        drop(_append_guard);
        Ok(Self {
            path,
            file,
            chatend,
            objects,
            sidebands,
            append_lock,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn state(&self) -> &Chatend {
        &self.chatend
    }

    pub fn objects(&self) -> &BTreeMap<PendingId, ObjectLocation> {
        &self.objects
    }

    pub fn sidebands(&self) -> &[SidebandRecord] {
        &self.sidebands
    }

    pub fn append_sideband(
        &mut self,
        kind: impl Into<String>,
        recorded_at: impl Into<String>,
        value: Value,
    ) -> anyhow::Result<()> {
        let kind = kind.into();
        let recorded_at = recorded_at.into();
        let record = JsonRecord::Sideband {
            kind: kind.clone(),
            recorded_at: recorded_at.clone(),
            value: value.clone(),
        };
        let append_lock = self.append_lock.clone();
        let _append_guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session journal append lock is poisoned"))?;
        append_frame(&mut self.file, JSON_FRAME, &serde_json::to_vec(&record)?)?;
        self.file.flush()?;
        self.file.sync_data()?;
        self.sidebands.push(SidebandRecord {
            kind,
            recorded_at,
            value,
        });
        Ok(())
    }

    pub fn create_box(
        &mut self,
        recorded_at: impl Into<String>,
        name: impl Into<String>,
        owner: BoxOwner,
        content: BoxContent,
    ) -> anyhow::Result<BoxId> {
        let recorded_at = recorded_at.into();
        let id = EventId(self.chatend.next_id);
        let box_id = BoxId(id.0);
        self.commit_events(
            recorded_at.clone(),
            vec![Event {
                id,
                recorded_at,
                kind: EventKind::BoxCreated {
                    box_id,
                    name: name.into(),
                    owner,
                    content,
                },
            }],
        )?;
        Ok(box_id)
    }

    pub fn update_box(
        &mut self,
        recorded_at: impl Into<String>,
        box_id: BoxId,
        content: BoxContent,
    ) -> anyhow::Result<Option<EventId>> {
        let state = self
            .chatend
            .boxes
            .get(&box_id)
            .with_context(|| format!("box {box_id} does not exist"))?;
        ensure!(state.active, "box {box_id} is retired");
        if state.canonical.content == content {
            return Ok(None);
        }
        let id = EventId(self.chatend.next_id);
        let recorded_at = recorded_at.into();
        self.commit_events(
            recorded_at.clone(),
            vec![Event {
                id,
                recorded_at,
                kind: EventKind::CanonicalUpdated { box_id, content },
            }],
        )?;
        Ok(Some(id))
    }

    pub fn dehydrate_box(
        &mut self,
        recorded_at: impl Into<String>,
        box_id: BoxId,
    ) -> anyhow::Result<EventId> {
        self.box_operation(recorded_at, EventKind::BoxDehydrated { box_id })
    }

    pub fn summarize_box(
        &mut self,
        recorded_at: impl Into<String>,
        box_id: BoxId,
        text: impl Into<String>,
    ) -> anyhow::Result<EventId> {
        self.box_operation(
            recorded_at,
            EventKind::BoxSummarized {
                box_id,
                text: text.into(),
            },
        )
    }

    pub fn rehydrate_box(
        &mut self,
        recorded_at: impl Into<String>,
        box_id: BoxId,
    ) -> anyhow::Result<EventId> {
        self.box_operation(recorded_at, EventKind::BoxRehydrated { box_id })
    }

    pub fn retire_box(
        &mut self,
        recorded_at: impl Into<String>,
        box_id: BoxId,
    ) -> anyhow::Result<EventId> {
        self.box_operation(recorded_at, EventKind::BoxRetired { box_id })
    }

    fn box_operation(
        &mut self,
        recorded_at: impl Into<String>,
        kind: EventKind,
    ) -> anyhow::Result<EventId> {
        let id = EventId(self.chatend.next_id);
        let recorded_at = recorded_at.into();
        self.commit_events(
            recorded_at.clone(),
            vec![Event {
                id,
                recorded_at,
                kind,
            }],
        )?;
        Ok(id)
    }

    pub fn allocate_pending_node(
        &mut self,
        recorded_at: impl Into<String>,
    ) -> anyhow::Result<PendingId> {
        let id = EventId(self.chatend.next_id);
        let pending_id = PendingId::from_event(id);
        let recorded_at = recorded_at.into();
        self.commit_events(
            recorded_at.clone(),
            vec![Event {
                id,
                recorded_at,
                kind: EventKind::PendingAllocated {
                    pending_id: pending_id.clone(),
                    resource: PendingKind::Node,
                },
            }],
        )?;
        Ok(pending_id)
    }

    pub fn stage_object(
        &mut self,
        recorded_at: impl Into<String>,
        media_type: impl Into<String>,
        file_name: Option<String>,
        transport: Value,
        bytes: &[u8],
    ) -> anyhow::Result<PendingId> {
        ensure!(
            bytes.len() as u64 <= MAX_OBJECT_BYTES,
            "object exceeds the 32 GiB V1 limit"
        );
        let aggregate = self
            .objects
            .values()
            .try_fold(bytes.len() as u64, |total, object| {
                total.checked_add(object.payload_len)
            })
            .context("staged object aggregate length overflow")?;
        ensure!(
            aggregate <= MAX_OBJECT_BYTES,
            "session object payload total exceeds the 32 GiB V1 limit"
        );
        let event_id = EventId(self.chatend.next_id);
        let pending_id = PendingId::from_event(event_id);
        let metadata = ObjectMetadata {
            pending_id: pending_id.clone(),
            event_id,
            recorded_at: recorded_at.into(),
            media_type: media_type.into(),
            file_name,
            transport,
        };
        let metadata_bytes = serde_json::to_vec(&metadata)?;
        let metadata_len =
            u32::try_from(metadata_bytes.len()).context("object metadata exceeds 4 GiB")?;
        let payload_len =
            OBJECT_PREFIX_BYTES as u64 + metadata_bytes.len() as u64 + bytes.len() as u64;
        let append_lock = self.append_lock.clone();
        let _append_guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session journal append lock is poisoned"))?;
        let frame_start = self.file.seek(SeekFrom::End(0))?;
        let mut hasher = Sha256::new();
        hasher.update(event_id.0.to_le_bytes());
        hasher.update(metadata_len.to_le_bytes());
        hasher.update(&metadata_bytes);
        hasher.update(bytes);
        let checksum = hasher.finalize();
        self.file.write_all(&[OBJECT_FRAME])?;
        self.file.write_all(&payload_len.to_le_bytes())?;
        self.file.write_all(&checksum)?;
        self.file.write_all(&event_id.0.to_le_bytes())?;
        self.file.write_all(&metadata_len.to_le_bytes())?;
        self.file.write_all(&metadata_bytes)?;
        self.file.write_all(bytes)?;
        self.file.flush()?;
        self.file.sync_data()?;
        let allocation = Event {
            id: event_id,
            recorded_at: metadata.recorded_at.clone(),
            kind: EventKind::PendingAllocated {
                pending_id: pending_id.clone(),
                resource: PendingKind::Object,
            },
        };
        self.chatend.apply_transition(&Transition {
            recorded_at: metadata.recorded_at.clone(),
            events: vec![allocation],
        })?;
        self.objects.insert(
            pending_id.clone(),
            ObjectLocation {
                metadata,
                payload_offset: frame_start
                    + FRAME_HEADER_BYTES
                    + OBJECT_PREFIX_BYTES as u64
                    + metadata_bytes.len() as u64,
                payload_len: bytes.len() as u64,
            },
        );
        Ok(pending_id)
    }

    pub fn read_object(&mut self, id: &PendingId) -> anyhow::Result<Vec<u8>> {
        let location = self
            .objects
            .get(id)
            .with_context(|| format!("staged object {id} does not exist"))?
            .clone();
        let len =
            usize::try_from(location.payload_len).context("object does not fit address space")?;
        let mut bytes = vec![0; len];
        self.file.seek(SeekFrom::Start(location.payload_offset))?;
        self.file.read_exact(&mut bytes)?;
        self.file.seek(SeekFrom::End(0))?;
        Ok(bytes)
    }

    pub fn record(
        &mut self,
        recorded_at: impl Into<String>,
        kind: EventKind,
    ) -> anyhow::Result<EventId> {
        let id = EventId(self.chatend.next_id);
        let recorded_at = recorded_at.into();
        self.commit_events(
            recorded_at.clone(),
            vec![Event {
                id,
                recorded_at,
                kind,
            }],
        )?;
        Ok(id)
    }

    pub fn commit_events(
        &mut self,
        recorded_at: impl Into<String>,
        events: Vec<Event>,
    ) -> anyhow::Result<()> {
        let transition = Transition {
            recorded_at: recorded_at.into(),
            events,
        };
        let mut candidate = self.chatend.clone();
        candidate.apply_transition(&transition)?;
        candidate.tools = derive_tool_states(&candidate);
        let record = JsonRecord::Transition {
            transition: transition.clone(),
        };
        let append_lock = self.append_lock.clone();
        let _append_guard = append_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("session journal append lock is poisoned"))?;
        append_frame(&mut self.file, JSON_FRAME, &serde_json::to_vec(&record)?)?;
        self.file.flush()?;
        self.file.sync_data()?;
        self.chatend = candidate;
        Ok(())
    }

    pub fn apply_tool_slots(
        &mut self,
        recorded_at: impl Into<String>,
        tool_instance: impl Into<String>,
        slots: Vec<ToolSlotInput>,
    ) -> anyhow::Result<Vec<EventId>> {
        let recorded_at = recorded_at.into();
        let tool_instance = tool_instance.into();
        let current = self
            .chatend
            .tools
            .get(&tool_instance)
            .cloned()
            .unwrap_or_default();
        ensure!(
            slots.len() >= current.slots.len(),
            "stateful tool slot sequence was truncated"
        );
        for (index, existing) in current.slots.iter().enumerate() {
            ensure!(
                slots[index].slot == existing.slot,
                "stateful tool slot sequence was reordered at index {index}"
            );
            ensure!(
                !existing.retired || slots[index].retired,
                "retired tool slot {} cannot be reactivated",
                existing.slot
            );
        }
        let mut events = Vec::new();
        let mut next = self.chatend.next_id;
        let mut next_state = current.clone();
        for (index, input) in slots.iter().enumerate() {
            if let Some(existing) = current.slots.get(index) {
                let state = self
                    .chatend
                    .boxes
                    .get(&existing.box_id)
                    .context("tool slot references a missing box")?;
                if input.retired && !existing.retired {
                    let id = EventId(next);
                    next += 1;
                    events.push(Event {
                        id,
                        recorded_at: recorded_at.clone(),
                        kind: EventKind::BoxRetired {
                            box_id: existing.box_id,
                        },
                    });
                    next_state.slots[index].retired = true;
                } else if !input.retired && state.canonical.content != input.content {
                    let id = EventId(next);
                    next += 1;
                    events.push(Event {
                        id,
                        recorded_at: recorded_at.clone(),
                        kind: EventKind::CanonicalUpdated {
                            box_id: existing.box_id,
                            content: input.content.clone(),
                        },
                    });
                }
            } else {
                ensure!(
                    !input.retired,
                    "a newly appended tool slot cannot start retired"
                );
                let id = EventId(next);
                next += 1;
                let box_id = BoxId(id.0);
                events.push(Event {
                    id,
                    recorded_at: recorded_at.clone(),
                    kind: EventKind::BoxCreated {
                        box_id,
                        name: input.name.clone(),
                        owner: BoxOwner::Tool {
                            tool_instance: tool_instance.clone(),
                            slot: input.slot.clone(),
                        },
                        content: input.content.clone(),
                    },
                });
                next_state.slots.push(ToolSlot {
                    slot: input.slot.clone(),
                    box_id,
                    retired: false,
                });
            }
        }
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
        self.commit_events(recorded_at, events)?;
        self.chatend.tools.insert(tool_instance, next_state);
        // Tool slot mappings are derived from box-owner events on replay.
        self.rebuild_tools();
        Ok(ids)
    }

    fn rebuild_tools(&mut self) {
        self.chatend.tools = derive_tool_states(&self.chatend);
    }

    pub fn fully_dehydrate_for_ingress(
        &mut self,
        recorded_at: impl Into<String>,
        keep_hydrated: &[BoxId],
    ) -> anyhow::Result<Vec<EventId>> {
        let recorded_at = recorded_at.into();
        let mut next = self.chatend.next_id;
        let mut events = Vec::new();
        for state in self.chatend.active_boxes() {
            if keep_hydrated.contains(&state.id)
                || matches!(state.representation, Representation::Dehydrated { .. })
            {
                continue;
            }
            events.push(Event {
                id: EventId(next),
                recorded_at: recorded_at.clone(),
                kind: EventKind::BoxDehydrated { box_id: state.id },
            });
            next += 1;
        }
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let ids = events.iter().map(|event| event.id).collect();
        self.commit_events(recorded_at, events)?;
        Ok(ids)
    }

    pub fn sync_all(&mut self) -> anyhow::Result<()> {
        self.file.sync_all().context("syncing Chatend journal")
    }
}

fn journal_lock(path: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().expect("journal lock registry is poisoned");
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

fn sync_parent_directory(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .with_context(|| format!("opening journal directory {} for sync", parent.display()))?
        .sync_all()
        .with_context(|| format!("syncing journal directory {}", parent.display()))
}

fn derive_tool_states(chatend: &Chatend) -> BTreeMap<String, ToolState> {
    let mut output = BTreeMap::<String, ToolState>::new();
    let mut boxes = chatend.boxes.values().collect::<Vec<_>>();
    boxes.sort_by_key(|state| state.created_at);
    for state in boxes {
        let BoxOwner::Tool {
            tool_instance,
            slot,
        } = &state.owner
        else {
            continue;
        };
        output
            .entry(tool_instance.clone())
            .or_default()
            .slots
            .push(ToolSlot {
                slot: slot.clone(),
                box_id: state.id,
                retired: !state.active,
            });
    }
    output
}

fn append_frame(file: &mut File, kind: u8, payload: &[u8]) -> anyhow::Result<()> {
    file.seek(SeekFrom::End(0))?;
    file.write_all(&[kind])?;
    file.write_all(&(payload.len() as u64).to_le_bytes())?;
    file.write_all(&Sha256::digest(payload))?;
    file.write_all(payload)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::Write,
        time::{SystemTime, UNIX_EPOCH},
    };

    use serde_json::json;

    use super::*;

    fn path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "kennedy-chatend-{label}-{}-{}.journal",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn metadata() -> SessionMetadata {
        SessionMetadata {
            session_id: "session-1".into(),
            kind: SessionKind::Conversation,
            created_at: "2026-07-23T00:00:00Z".into(),
            effective_context_tokens: 1_000,
            channel: json!({"kind":"test"}),
        }
    }

    #[test]
    fn box_identity_continuations_staleness_and_replay_are_exact() {
        let path = path("boxes");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        let id = journal
            .create_box(
                "t1",
                "message",
                BoxOwner::User,
                BoxContent::text("original"),
            )
            .unwrap();
        assert_eq!(id, BoxId(1));
        journal.summarize_box("t2", id, "summary").unwrap();
        journal
            .update_box("t3", id, BoxContent::text("changed"))
            .unwrap();
        let state = journal.state().box_state(id).unwrap().clone();
        assert!(state.stale());
        assert_eq!(
            state.representation,
            Representation::Summarized {
                based_on: EventId(1),
                text: "summary".into()
            }
        );
        let projection = journal.state().projection();
        assert!(projection.items[0].text.contains("continued at event 2"));
        assert!(projection.items[1].text.contains("continued at event 3"));
        assert!(projection.items[2].text.contains("summary"));
        assert!(projection.items[2].stale);
        drop(journal);
        let reopened = SessionJournal::open(&path).unwrap();
        assert_eq!(reopened.state().box_state(id), Some(&state));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn shared_pending_and_box_identity_space_never_overlaps() {
        let path = path("pending");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        let first = journal.allocate_pending_node("t1").unwrap();
        let box_id = journal
            .create_box("t2", "box", BoxOwner::Kennedy, BoxContent::text("hello"))
            .unwrap();
        let object = journal
            .stage_object(
                "t3",
                "application/octet-stream",
                None,
                Value::Null,
                b"\0binary\xff",
            )
            .unwrap();
        assert_eq!(first.as_str(), "pending:1");
        assert_eq!(box_id, BoxId(2));
        assert_eq!(object.as_str(), "pending:3");
        assert_eq!(journal.read_object(&object).unwrap(), b"\0binary\xff");
        drop(journal);
        let mut reopened = SessionJournal::open(&path).unwrap();
        assert_eq!(reopened.read_object(&object).unwrap(), b"\0binary\xff");
        assert_eq!(reopened.state().next_id, 4);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn incomplete_final_frame_is_discarded_and_append_can_resume() {
        let path = path("tail");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        journal
            .create_box("t1", "box", BoxOwner::User, BoxContent::text("safe"))
            .unwrap();
        drop(journal);
        let good_len = std::fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[JSON_FRAME]).unwrap();
        file.write_all(&500_u64.to_le_bytes()).unwrap();
        file.write_all(&[0; 7]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let mut recovered = SessionJournal::open(&path).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), good_len);
        let id = recovered
            .create_box("t2", "next", BoxOwner::Kennedy, BoxContent::text("resumed"))
            .unwrap();
        assert_eq!(id, BoxId(2));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn provider_measurements_recalibrate_the_matching_manifest_then_track_deltas() {
        let path = path("calibration");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        journal
            .create_box(
                "t1",
                "system",
                BoxOwner::System,
                BoxContent::text("baseline provider content"),
            )
            .unwrap();
        let submitted = journal.state().projection();
        journal
            .record(
                "t2",
                EventKind::InferenceSubmitted {
                    manifest_hash: "manifest-1".into(),
                    estimated_input_tokens: submitted.estimated_tokens,
                    raw_estimated_input_tokens: Some(submitted.raw_estimated_tokens),
                },
            )
            .unwrap();
        journal
            .record(
                "t3",
                EventKind::ProviderReceipt {
                    manifest_hash: "manifest-1".into(),
                    input_tokens: Some(777),
                    output_tokens: Some(3),
                    provider_data: Value::Null,
                },
            )
            .unwrap();
        assert_eq!(journal.state().projection().estimated_tokens, 777);
        journal
            .create_box(
                "t4",
                "new material",
                BoxOwner::User,
                BoxContent::text("x".repeat(300)),
            )
            .unwrap();
        let expanded = journal.state().projection();
        assert!(expanded.estimated_tokens > 777);
        assert!(expanded.raw_estimated_tokens > submitted.raw_estimated_tokens);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn stateful_tool_slots_are_atomic_append_only_and_do_not_see_summaries() {
        let path = path("slots");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        journal
            .apply_tool_slots(
                "t1",
                "rust-1",
                vec![
                    ToolSlotInput {
                        slot: "a.rs".into(),
                        name: "a.rs".into(),
                        content: BoxContent::text("a"),
                        retired: false,
                    },
                    ToolSlotInput {
                        slot: "b.rs".into(),
                        name: "b.rs".into(),
                        content: BoxContent::text("b"),
                        retired: false,
                    },
                ],
            )
            .unwrap();
        let a = journal.state().tools["rust-1"].slots[0].box_id;
        journal.summarize_box("t2", a, "Kennedy summary").unwrap();
        let before = journal.state().clone();
        assert!(
            journal
                .apply_tool_slots(
                    "t3",
                    "rust-1",
                    vec![ToolSlotInput {
                        slot: "b.rs".into(),
                        name: "b.rs".into(),
                        content: BoxContent::text("b"),
                        retired: false,
                    }]
                )
                .is_err()
        );
        assert_eq!(journal.state(), &before);
        journal
            .apply_tool_slots(
                "t4",
                "rust-1",
                vec![
                    ToolSlotInput {
                        slot: "a.rs".into(),
                        name: "a.rs".into(),
                        content: BoxContent::text("a2"),
                        retired: false,
                    },
                    ToolSlotInput {
                        slot: "b.rs".into(),
                        name: "b.rs".into(),
                        content: BoxContent::text("b"),
                        retired: true,
                    },
                    ToolSlotInput {
                        slot: "c.rs".into(),
                        name: "c.rs".into(),
                        content: BoxContent::text("c"),
                        retired: false,
                    },
                ],
            )
            .unwrap();
        let a_state = journal.state().box_state(a).unwrap();
        assert_eq!(a_state.canonical.content.text, "a2");
        assert!(matches!(
            a_state.representation,
            Representation::Summarized { .. }
        ));
        drop(journal);
        let reopened = SessionJournal::open(&path).unwrap();
        assert_eq!(reopened.state().tools["rust-1"].slots.len(), 3);
        assert!(reopened.state().tools["rust-1"].slots[1].retired);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn limits_use_exact_floor_percentages() {
        let state = Chatend::opened(SessionMetadata {
            effective_context_tokens: 101,
            ..metadata()
        });
        assert_eq!(state.live_context_limit(), 70);
        assert_eq!(state.emergency_context_limit(), 72);
        assert_eq!(state.history_context_limit(), 101);
        assert_eq!(estimate_tokens("1234"), 2);
    }
}
