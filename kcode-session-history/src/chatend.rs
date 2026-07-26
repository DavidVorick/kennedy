//! Kennedy's reconstructed, provider-independent durable session context.
//!
//! This module owns Kennedy's box model, context projection, context
//! representations, token policy, and replay rules. Durable session history
//! is owned by the separate `kcode-session-log` package.

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

use anyhow::{Context as _, ensure};
use kcode_session_log::{
    EventPosition, Role, Session as DurableSession, SessionStore as DurableSessionStore,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BoxRepresentation {
    Hydrated,
    Dehydrated,
    Summarized(String),
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
    BoxRenamed {
        box_id: BoxId,
        name: String,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        invocation_id: Option<String>,
    },
    ToolCompleted {
        tool_instance: String,
        tool_name: String,
        outcome: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        invocation_id: Option<String>,
    },
    ToolLayoutChanged {
        tool_instance: String,
        box_ids: Vec<BoxId>,
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
        #[serde(default)]
        raw_context_tokens: Option<u64>,
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
#[serde(rename_all = "camelCase")]
struct PersistedContextEvent {
    context_event_version: u32,
    recorded_at: String,
    kind: EventKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedContextEventWire {
    context_event_version: u32,
    recorded_at: String,
    kind: Value,
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
    #[serde(default)]
    pub tool_layouts: BTreeMap<String, Vec<BoxId>>,
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
            tool_layouts: BTreeMap::new(),
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

    pub fn forced_ingress_context_limit(&self) -> u64 {
        self.metadata.effective_context_tokens.saturating_mul(75) / 100
    }

    pub fn ingress_initial_context_limit(&self) -> u64 {
        self.metadata.effective_context_tokens.saturating_mul(75) / 100
    }

    pub fn ingress_context_limit(&self) -> u64 {
        self.metadata.effective_context_tokens
    }

    pub fn active_context_limit(&self) -> u64 {
        if matches!(
            self.metadata.kind,
            SessionKind::HistoryIngress | SessionKind::AudioIngress
        ) {
            self.ingress_context_limit()
        } else {
            self.live_context_limit()
        }
    }

    pub fn projection_with_new_boxes(
        &self,
        boxes: &[(String, BoxOwner, BoxContent)],
    ) -> anyhow::Result<ContextProjection> {
        self.projection_with_new_boxes_and_updates(boxes, &BTreeMap::new())
    }

    pub fn projection_with_new_boxes_and_updates(
        &self,
        boxes: &[(String, BoxOwner, BoxContent)],
        updates: &BTreeMap<BoxId, BoxContent>,
    ) -> anyhow::Result<ContextProjection> {
        let mut preview = self.clone();
        let mut next = preview.next_id;
        let mut events = Vec::with_capacity(boxes.len() + updates.len());
        for (name, owner, content) in boxes {
            let id = EventId(next);
            let box_id = BoxId(next);
            next = next.checked_add(1).context("event identity overflow")?;
            events.push(Event {
                id,
                recorded_at: "preview".into(),
                kind: EventKind::BoxCreated {
                    box_id,
                    name: name.clone(),
                    owner: owner.clone(),
                    content: content.clone(),
                },
            });
        }
        for (box_id, content) in updates {
            let state = preview
                .box_state(*box_id)
                .with_context(|| format!("box {box_id} does not exist"))?;
            ensure!(state.active, "box {box_id} is retired");
            if state.canonical.content == *content {
                continue;
            }
            let id = EventId(next);
            next = next.checked_add(1).context("event identity overflow")?;
            events.push(Event {
                id,
                recorded_at: "preview".into(),
                kind: EventKind::CanonicalUpdated {
                    box_id: *box_id,
                    content: content.clone(),
                },
            });
        }
        if !events.is_empty() {
            preview.apply_transition(&Transition {
                recorded_at: "preview".into(),
                events,
            })?;
        }
        Ok(preview.projection())
    }

    pub fn projection_with_box_representations(
        &self,
        desired: &BTreeMap<BoxId, BoxRepresentation>,
    ) -> anyhow::Result<ContextProjection> {
        let mut preview = self.clone();
        let events = preview.box_representation_events("preview", desired)?;
        if !events.is_empty() {
            preview.apply_transition(&Transition {
                recorded_at: "preview".into(),
                events,
            })?;
        }
        Ok(preview.projection())
    }

    fn box_representation_events(
        &self,
        recorded_at: &str,
        desired: &BTreeMap<BoxId, BoxRepresentation>,
    ) -> anyhow::Result<Vec<Event>> {
        let mut next = self.next_id;
        let mut events = Vec::new();
        for (box_id, desired) in desired {
            let state = self
                .box_state(*box_id)
                .with_context(|| format!("box {box_id} does not exist"))?;
            ensure!(state.active, "box {box_id} is retired");
            let kind = match (desired, &state.representation) {
                (BoxRepresentation::Hydrated, Representation::Hydrated { .. })
                | (BoxRepresentation::Dehydrated, Representation::Dehydrated { .. }) => None,
                (
                    BoxRepresentation::Summarized(desired),
                    Representation::Summarized { text, .. },
                ) if desired == text => None,
                (BoxRepresentation::Hydrated, _) => {
                    Some(EventKind::BoxRehydrated { box_id: *box_id })
                }
                (BoxRepresentation::Dehydrated, _) => {
                    Some(EventKind::BoxDehydrated { box_id: *box_id })
                }
                (BoxRepresentation::Summarized(text), _) => Some(EventKind::BoxSummarized {
                    box_id: *box_id,
                    text: text.clone(),
                }),
            };
            let Some(kind) = kind else {
                continue;
            };
            events.push(Event {
                id: EventId(next),
                recorded_at: recorded_at.into(),
                kind,
            });
            next = next.checked_add(1).context("event ID overflow")?;
        }
        Ok(events)
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
        for box_ids in self.tool_layouts.values() {
            arrange_tool_projection(&mut items, box_ids);
        }
        let body_tokens = items
            .iter()
            .map(|item| item.approximate_tokens)
            .sum::<u64>();
        let preliminary_footer = format!(
            "[context budget | estimated={} | turn_limit={} | effective={}]\n[stale boxes: {}]",
            body_tokens,
            self.active_context_limit(),
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
            "[context budget | estimated={} | turn_limit={} | effective={}]\n[stale boxes: {}]",
            preliminary_estimate,
            self.active_context_limit(),
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
        let Some((manifest_hash, measured, raw_at_receipt)) =
            self.events.iter().rev().find_map(|event| {
                let EventKind::ProviderReceipt {
                    manifest_hash,
                    input_tokens: Some(input_tokens),
                    raw_context_tokens,
                    ..
                } = &event.kind
                else {
                    return None;
                };
                Some((manifest_hash, *input_tokens, *raw_context_tokens))
            })
        else {
            return raw_current;
        };
        let raw_at_measurement = match raw_at_receipt {
            Some(raw) => raw,
            None => {
                let Some(raw) = self.events.iter().rev().find_map(|event| {
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
                raw
            }
        };
        if raw_current >= raw_at_measurement {
            measured.saturating_add(raw_current - raw_at_measurement)
        } else {
            measured.saturating_sub(raw_at_measurement - raw_current)
        }
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
        for event in &transition.events {
            self.apply_event(event)?;
        }
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
                if let BoxOwner::Tool {
                    tool_instance,
                    slot,
                } = owner
                {
                    self.tools
                        .entry(tool_instance.clone())
                        .or_default()
                        .slots
                        .push(ToolSlot {
                            slot: slot.clone(),
                            box_id: *box_id,
                            retired: false,
                        });
                }
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
            EventKind::BoxRenamed { box_id, name } => {
                ensure!(!name.trim().is_empty(), "a box name cannot be empty");
                let state = active_box_mut(&mut self.boxes, *box_id)?;
                state.name = name.clone();
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
                let tool_instance = self.boxes.get(box_id).and_then(|state| {
                    let BoxOwner::Tool { tool_instance, .. } = &state.owner else {
                        return None;
                    };
                    Some(tool_instance.clone())
                });
                let state = active_box_mut(&mut self.boxes, *box_id)?;
                state.active = false;
                state.occurrence_events.push(event.id);
                if let Some(tool_instance) = tool_instance {
                    let slot = self
                        .tools
                        .get_mut(&tool_instance)
                        .and_then(|tool| tool.slots.iter_mut().find(|slot| slot.box_id == *box_id))
                        .with_context(|| {
                            format!("tool box {box_id} is missing from {tool_instance}")
                        })?;
                    slot.retired = true;
                }
            }
            EventKind::ToolLayoutChanged {
                tool_instance,
                box_ids,
            } => {
                let mut unique = std::collections::HashSet::new();
                for box_id in box_ids {
                    ensure!(
                        unique.insert(*box_id),
                        "tool layout contains duplicate box {box_id}"
                    );
                    let state = self
                        .boxes
                        .get(box_id)
                        .with_context(|| format!("tool layout references missing box {box_id}"))?;
                    ensure!(state.active, "tool layout references retired box {box_id}");
                    ensure!(
                        matches!(
                            &state.owner,
                            BoxOwner::Tool {
                                tool_instance: owner,
                                ..
                            } if owner == tool_instance
                        ),
                        "tool layout box {box_id} belongs to another tool"
                    );
                }
                self.tool_layouts
                    .insert(tool_instance.clone(), box_ids.clone());
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

fn arrange_tool_projection(items: &mut Vec<ProjectionItem>, box_ids: &[BoxId]) {
    if box_ids.is_empty() {
        return;
    }
    let ranks = box_ids
        .iter()
        .enumerate()
        .map(|(rank, box_id)| (*box_id, rank))
        .collect::<HashMap<_, _>>();
    let insertion = items
        .iter()
        .position(|item| !item.marker && ranks.contains_key(&item.box_id));
    let Some(insertion) = insertion else {
        return;
    };
    let mut arranged = Vec::with_capacity(box_ids.len());
    let mut retained = Vec::with_capacity(items.len());
    for item in std::mem::take(items) {
        if !item.marker && ranks.contains_key(&item.box_id) {
            arranged.push(item);
        } else {
            retained.push(item);
        }
    }
    arranged.sort_by_key(|item| ranks[&item.box_id]);
    let insertion = insertion.min(retained.len());
    retained.splice(insertion..insertion, arranged);
    *items = retained;
}

fn event_box_id(kind: &EventKind) -> Option<BoxId> {
    match kind {
        EventKind::BoxCreated { box_id, .. }
        | EventKind::CanonicalUpdated { box_id, .. }
        | EventKind::BoxRenamed { box_id, .. }
        | EventKind::BoxDehydrated { box_id }
        | EventKind::BoxSummarized { box_id, .. }
        | EventKind::BoxRehydrated { box_id }
        | EventKind::BoxRetired { box_id } => Some(*box_id),
        _ => None,
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

pub fn estimate_tokens(text: &str) -> u64 {
    (text.chars().count() as u64).div_ceil(ESTIMATED_CHARACTERS_PER_TOKEN)
}

fn context_event_role(kind: &EventKind) -> Role {
    match kind {
        EventKind::BoxCreated { owner, content, .. } => match owner {
            BoxOwner::System | BoxOwner::Controller => {
                if content
                    .metadata
                    .get("capacityError")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    Role::SystemError
                } else {
                    Role::SystemMessage
                }
            }
            BoxOwner::User => Role::UserMessage,
            BoxOwner::Kennedy => Role::KennedyMessage,
            BoxOwner::Tool { .. } => Role::ToolResult,
        },
        EventKind::ToolInvoked { .. } => Role::KennedyToolCall,
        EventKind::ToolCompleted { outcome, .. } => {
            if outcome.get("ok").and_then(Value::as_bool).unwrap_or(true) {
                Role::ToolResult
            } else {
                Role::ToolError
            }
        }
        EventKind::CapacityError { .. } => Role::SystemError,
        EventKind::PendingAllocated {
            resource: PendingKind::Object,
            ..
        } => Role::PendingObject,
        EventKind::BoxDehydrated { .. }
        | EventKind::BoxSummarized { .. }
        | EventKind::BoxRehydrated { .. }
        | EventKind::BoxRetired { .. } => Role::KennedyToolCall,
        _ => Role::SystemMessage,
    }
}

fn encode_context_event(recorded_at: &str, kind: &EventKind) -> anyhow::Result<String> {
    let mut kind = serde_json::to_value(kind)?;
    let kind_object = kind
        .as_object_mut()
        .context("Kennedy context event kind must encode as an object")?;
    match kind_object.get("type").and_then(Value::as_str) {
        Some("box_created") => {
            kind_object.remove("box_id");
        }
        Some("pending_allocated") => {
            kind_object.remove("pending_id");
        }
        _ => {}
    }
    Ok(serde_json::to_string(&PersistedContextEventWire {
        context_event_version: FORMAT_VERSION,
        recorded_at: recorded_at.into(),
        kind,
    })?)
}

fn decode_context_event(
    event: &kcode_session_log::SessionEvent,
) -> anyhow::Result<PersistedContextEvent> {
    let wire: PersistedContextEventWire = match serde_json::from_str(&event.text) {
        Ok(persisted) => persisted,
        Err(_) if event.role == Role::PendingObject => {
            return Ok(PersistedContextEvent {
                context_event_version: FORMAT_VERSION,
                recorded_at: String::new(),
                kind: EventKind::PendingAllocated {
                    pending_id: PendingId::from_event(EventId(1)),
                    resource: PendingKind::Object,
                },
            });
        }
        Err(error) => {
            return Err(error).context("decoding Kennedy context event from session log");
        }
    };
    ensure!(
        wire.context_event_version == FORMAT_VERSION,
        "unsupported Kennedy context event version {}",
        wire.context_event_version
    );
    let mut kind = wire.kind;
    let kind_object = kind
        .as_object_mut()
        .context("Kennedy context event kind must be an object")?;
    match kind_object.get("type").and_then(Value::as_str) {
        Some("box_created") if !kind_object.contains_key("box_id") => {
            kind_object.insert("box_id".into(), Value::from(0));
        }
        Some("pending_allocated") if !kind_object.contains_key("pending_id") => {
            kind_object.insert("pending_id".into(), Value::String("pending:1".into()));
        }
        _ => {}
    }
    Ok(PersistedContextEvent {
        context_event_version: wire.context_event_version,
        recorded_at: wire.recorded_at,
        kind: serde_json::from_value(kind).context("decoding Kennedy context event kind")?,
    })
}

fn normalize_derived_identity(kind: &mut EventKind, id: EventId) -> anyhow::Result<()> {
    match kind {
        EventKind::BoxCreated { box_id, .. } => *box_id = BoxId(id.0),
        EventKind::PendingAllocated { pending_id, .. } => {
            *pending_id = PendingId::from_event(id);
        }
        _ => {}
    }
    Ok(())
}

pub struct Session {
    durable: DurableSession,
    chatend: Chatend,
    objects: BTreeMap<PendingId, ObjectLocation>,
}

struct UnfinishedToolInvocation {
    invocation_id: Option<String>,
    tool_instance: String,
    tool_name: String,
}

impl Session {
    pub(crate) fn create(
        path: impl AsRef<Path>,
        metadata: SessionMetadata,
    ) -> anyhow::Result<Self> {
        ensure!(
            metadata.effective_context_tokens > 0,
            "effective context window must be positive"
        );
        let requested = path.as_ref();
        let directory = requested
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let durable = DurableSessionStore::new(directory)
            .create_session(&metadata.session_id, &metadata.created_at)?;
        Ok(Self {
            durable,
            chatend: Chatend::opened(metadata),
            objects: BTreeMap::new(),
        })
    }

    pub(crate) fn open_with_metadata(
        path: impl AsRef<Path>,
        metadata: SessionMetadata,
    ) -> anyhow::Result<Self> {
        let requested = path.as_ref();
        ensure!(
            requested.extension().and_then(|value| value.to_str()) == Some("session-log"),
            "{} is not a session-log path",
            requested.display()
        );
        let directory = requested
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let session_id = requested
            .file_stem()
            .and_then(|value| value.to_str())
            .context("session-log filename is not valid UTF-8")?;
        let durable = DurableSessionStore::new(directory).open_session(session_id)?;
        let log = durable.list();
        ensure!(
            metadata.session_id == log.header.session_id,
            "session metadata and session-log identities differ"
        );
        ensure!(
            metadata.created_at == log.header.created_at,
            "session metadata and session-log creation times differ"
        );
        let mut chatend = Chatend::opened(metadata);
        let mut objects = BTreeMap::new();
        for (position, stored) in log.events.iter().enumerate() {
            let persisted = decode_context_event(stored)?;
            let id = EventId(position as u64 + 1);
            let mut kind = persisted.kind;
            normalize_derived_identity(&mut kind, id)?;
            let event = Event {
                id,
                recorded_at: persisted.recorded_at.clone(),
                kind: kind.clone(),
            };
            chatend.apply_transition(&Transition {
                recorded_at: persisted.recorded_at,
                events: vec![event],
            })?;
            if let EventKind::PendingAllocated {
                pending_id,
                resource: PendingKind::Object,
            } = kind
            {
                let object = durable.read_pending_object(EventPosition(position as u64))?;
                let metadata = ObjectMetadata {
                    pending_id: pending_id.clone(),
                    event_id: id,
                    recorded_at: chatend
                        .event(id)
                        .map(|event| event.recorded_at.clone())
                        .unwrap_or_default(),
                    media_type: object.media_type,
                    file_name: Some(object.file_name),
                    transport: Value::Null,
                };
                objects.insert(
                    pending_id,
                    ObjectLocation {
                        metadata,
                        payload_offset: position as u64,
                        payload_len: object.bytes.len() as u64,
                    },
                );
            }
        }
        Ok(Self {
            durable,
            chatend,
            objects,
        })
    }

    pub fn id(&self) -> &str {
        &self.chatend.metadata.session_id
    }

    pub fn state(&self) -> &Chatend {
        &self.chatend
    }

    pub fn objects(&self) -> &BTreeMap<PendingId, ObjectLocation> {
        &self.objects
    }

    #[cfg(test)]
    fn session_log(&self) -> kcode_session_log::SessionLog {
        self.durable.list()
    }

    pub fn archive_bytes(&self) -> anyhow::Result<Vec<u8>> {
        serde_json::to_vec(&self.durable.list()).context("serializing the session archive")
    }

    pub fn is_sealed(&self) -> bool {
        self.durable.is_sealed()
    }

    pub fn seal(&mut self) -> anyhow::Result<()> {
        let unfinished_tools = self.unfinished_tool_invocations()?;
        ensure!(
            unfinished_tools.is_empty(),
            "session ends with unfinished tools {}",
            unfinished_tools
                .iter()
                .map(|tool| tool.tool_name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        self.durable.seal()?;
        Ok(())
    }

    pub fn repair_unfinished_tools(
        &mut self,
        recorded_at: impl Into<String>,
    ) -> anyhow::Result<Vec<EventId>> {
        let unfinished = self.unfinished_tool_invocations()?;
        if unfinished.is_empty() {
            return Ok(Vec::new());
        }
        let recorded_at = recorded_at.into();
        let mut repaired = Vec::with_capacity(unfinished.len());
        for tool in unfinished.iter().rev() {
            let message = format!(
                "{} was interrupted before a durable completion was recorded; the abandoned invocation was closed during session recovery.",
                tool.tool_name
            );
            let kind = if let Some(invocation_id) = &tool.invocation_id {
                EventKind::ToolCompleted {
                    tool_instance: tool.tool_instance.clone(),
                    tool_name: tool.tool_name.clone(),
                    outcome: serde_json::json!({"ok":false,"recovered":true,"result":message}),
                    invocation_id: Some(invocation_id.clone()),
                }
            } else {
                // Historical native Ktool completions used one generic
                // `call_ktool` event to close the most recent invocation.
                EventKind::ToolCompleted {
                    tool_instance: "call_ktool".into(),
                    tool_name: "call_ktool".into(),
                    outcome: serde_json::json!({"ok":false,"recovered":true,"result":message}),
                    invocation_id: None,
                }
            };
            repaired.push(self.record(recorded_at.clone(), kind)?);
        }
        ensure!(
            self.unfinished_tool_invocations()?.is_empty(),
            "session tool recovery left unfinished invocations"
        );
        Ok(repaired)
    }

    fn unfinished_tool_invocations(&self) -> anyhow::Result<Vec<UnfinishedToolInvocation>> {
        let mut identified = BTreeMap::<String, UnfinishedToolInvocation>::new();
        let mut legacy = Vec::<UnfinishedToolInvocation>::new();
        for event in &self.chatend.events {
            match &event.kind {
                EventKind::ToolInvoked {
                    tool_instance,
                    tool_name,
                    invocation_id,
                    ..
                } => {
                    let pending = UnfinishedToolInvocation {
                        invocation_id: invocation_id.clone(),
                        tool_instance: tool_instance.clone(),
                        tool_name: tool_name.clone(),
                    };
                    if let Some(invocation_id) = invocation_id {
                        ensure!(
                            identified.insert(invocation_id.clone(), pending).is_none(),
                            "duplicate tool invocation identity {invocation_id}"
                        );
                    } else {
                        legacy.push(pending);
                    }
                }
                EventKind::ToolCompleted {
                    tool_instance,
                    tool_name,
                    invocation_id,
                    ..
                } => {
                    if let Some(invocation_id) = invocation_id {
                        let invoked = identified.remove(invocation_id).with_context(|| {
                            format!(
                                "tool {tool_name} completed without matching invocation {invocation_id}"
                            )
                        })?;
                        ensure!(
                            invoked.tool_instance.as_str() == tool_instance
                                && invoked.tool_name.as_str() == tool_name,
                            "tool completion {invocation_id} does not match its invocation"
                        );
                    } else if tool_name == "call_ktool" {
                        // An invalid native call still produces an error result
                        // even when it could not be decoded into ToolInvoked.
                        legacy.pop();
                    } else {
                        let before = legacy.len();
                        legacy.retain(|unfinished| unfinished.tool_name.as_str() != tool_name);
                        ensure!(
                            legacy.len() != before,
                            "tool {tool_name} completed without a matching invocation"
                        );
                    }
                }
                _ => {}
            }
        }
        legacy.extend(identified.into_values());
        Ok(legacy)
    }

    pub fn mark_completed(&mut self, session_object_id: String) {
        self.chatend.completed_session_object = Some(session_object_id);
    }

    pub fn configure_context(&mut self, kind: SessionKind, effective_context_tokens: u64) {
        self.chatend.metadata.kind = kind;
        self.chatend.metadata.effective_context_tokens = effective_context_tokens;
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
        let box_id = event_box_id(&kind).context("box operation has no box identity")?;
        let state = self
            .chatend
            .box_state(box_id)
            .with_context(|| format!("box {box_id} does not exist"))?;
        ensure!(state.active, "box {box_id} is retired");
        if let EventKind::BoxSummarized { text, .. } = &kind {
            ensure!(!text.trim().is_empty(), "a box summary cannot be empty");
        }
        if matches!(kind, EventKind::BoxRetired { .. })
            && let BoxOwner::Tool { tool_instance, .. } = &state.owner
        {
            ensure!(
                self.chatend
                    .tools
                    .get(tool_instance)
                    .is_some_and(|tool| tool.slots.iter().any(|slot| slot.box_id == box_id)),
                "tool box {box_id} is missing from {tool_instance}"
            );
        }
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
        let kind = EventKind::PendingAllocated {
            pending_id: pending_id.clone(),
            resource: PendingKind::Object,
        };
        let text = encode_context_event(&metadata.recorded_at, &kind)?;
        let object_file_name = metadata
            .file_name
            .clone()
            .unwrap_or_else(|| format!("object-{}", event_id.0));
        let position = self.durable.add_pending_object(
            text,
            object_file_name,
            metadata.media_type.clone(),
            bytes,
        )?;
        ensure!(
            position.0 + 1 == event_id.0,
            "session-log event position diverged from Kennedy context identity"
        );
        let allocation = Event {
            id: event_id,
            recorded_at: metadata.recorded_at.clone(),
            kind,
        };
        self.chatend.apply_transition(&Transition {
            recorded_at: metadata.recorded_at.clone(),
            events: vec![allocation],
        })?;
        self.objects.insert(
            pending_id.clone(),
            ObjectLocation {
                metadata,
                payload_offset: position.0,
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
        Ok(self
            .durable
            .read_pending_object(EventPosition(location.payload_offset))?
            .bytes)
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
        ensure!(
            !transition.events.is_empty(),
            "a transition cannot be empty"
        );
        ensure!(
            !transition.events.iter().any(|event| {
                matches!(
                    event.kind,
                    EventKind::PendingAllocated {
                        resource: PendingKind::Object,
                        ..
                    }
                )
            }),
            "pending objects must be added through stage_object"
        );
        let mut preview = self.chatend.clone();
        preview.apply_transition(&transition)?;
        for event in &transition.events {
            let expected = self.durable.list().events.len() as u64 + 1;
            ensure!(
                event.id.0 == expected,
                "Kennedy context event {} does not match session-log position {}",
                event.id,
                expected - 1
            );
            self.durable.add_event(
                context_event_role(&event.kind),
                encode_context_event(&event.recorded_at, &event.kind)?,
            )?;
        }
        self.chatend = preview;
        Ok(())
    }

    pub fn apply_tool_slots(
        &mut self,
        recorded_at: impl Into<String>,
        tool_instance: impl Into<String>,
        slots: Vec<ToolSlotInput>,
    ) -> anyhow::Result<Vec<EventId>> {
        self.apply_tool_slots_inner(recorded_at, tool_instance, slots, None)
    }

    pub fn apply_tool_slots_with_layout(
        &mut self,
        recorded_at: impl Into<String>,
        tool_instance: impl Into<String>,
        slots: Vec<ToolSlotInput>,
        layout_slots: &[String],
    ) -> anyhow::Result<Vec<EventId>> {
        self.apply_tool_slots_inner(recorded_at, tool_instance, slots, Some(layout_slots))
    }

    fn apply_tool_slots_inner(
        &mut self,
        recorded_at: impl Into<String>,
        tool_instance: impl Into<String>,
        slots: Vec<ToolSlotInput>,
        layout_slots: Option<&[String]>,
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
                } else if !input.retired {
                    if state.name != input.name {
                        let id = EventId(next);
                        next += 1;
                        events.push(Event {
                            id,
                            recorded_at: recorded_at.clone(),
                            kind: EventKind::BoxRenamed {
                                box_id: existing.box_id,
                                name: input.name.clone(),
                            },
                        });
                    }
                    if state.canonical.content != input.content {
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
        if let Some(layout_slots) = layout_slots {
            let mut unique = std::collections::HashSet::new();
            let box_ids = layout_slots
                .iter()
                .map(|slot_name| {
                    ensure!(
                        unique.insert(slot_name),
                        "tool layout contains duplicate slot {slot_name}"
                    );
                    let slot = next_state
                        .slots
                        .iter()
                        .find(|slot| &slot.slot == slot_name)
                        .with_context(|| {
                            format!("tool layout references missing slot {slot_name}")
                        })?;
                    ensure!(
                        !slot.retired,
                        "tool layout references retired slot {slot_name}"
                    );
                    Ok(slot.box_id)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            if self.chatend.tool_layouts.get(&tool_instance) != Some(&box_ids) {
                let id = EventId(next);
                events.push(Event {
                    id,
                    recorded_at: recorded_at.clone(),
                    kind: EventKind::ToolLayoutChanged {
                        tool_instance: tool_instance.clone(),
                        box_ids,
                    },
                });
            }
        }
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
        self.commit_events(recorded_at, events)?;
        Ok(ids)
    }

    pub fn apply_box_representations(
        &mut self,
        recorded_at: impl Into<String>,
        desired: &BTreeMap<BoxId, BoxRepresentation>,
    ) -> anyhow::Result<Vec<EventId>> {
        let recorded_at = recorded_at.into();
        let events = self
            .chatend
            .box_representation_events(&recorded_at, desired)?;
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
        self.commit_events(recorded_at, events)?;
        Ok(ids)
    }
}

#[cfg(test)]
type SessionJournal = Session;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::json;

    use super::*;

    fn path(label: &str) -> PathBuf {
        std::env::temp_dir()
            .join(format!(
                "kennedy-chatend-{label}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .join("session-1.session-log")
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
        let reopened = SessionJournal::open_with_metadata(&path, metadata()).unwrap();
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
        assert_eq!(first.to_string(), "pending:1");
        assert_eq!(box_id, BoxId(2));
        assert_eq!(object.to_string(), "pending:3");
        assert_eq!(journal.read_object(&object).unwrap(), b"\0binary\xff");
        let stored = journal.session_log();
        assert!(!stored.events[0].text.contains("pending_id"));
        assert!(!stored.events[1].text.contains("box_id"));
        assert!(!stored.events[2].text.contains("pending_id"));
        drop(journal);
        let mut reopened = SessionJournal::open_with_metadata(&path, metadata()).unwrap();
        assert_eq!(reopened.read_object(&object).unwrap(), b"\0binary\xff");
        assert_eq!(reopened.state().next_id, 4);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn kennedy_tool_completion_is_validated_before_storage_seals() {
        let path = path("seal-tool");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        journal
            .record(
                "t1",
                EventKind::ToolInvoked {
                    tool_instance: "CreateNode:1".into(),
                    tool_name: "CreateNode".into(),
                    arguments: json!({}),
                    invocation_id: None,
                },
            )
            .unwrap();
        assert!(journal.seal().is_err());
        journal
            .record(
                "t2",
                EventKind::ToolCompleted {
                    tool_instance: "call_ktool".into(),
                    tool_name: "call_ktool".into(),
                    outcome: json!({"ok":true}),
                    invocation_id: None,
                },
            )
            .unwrap();
        journal.seal().unwrap();
        assert!(journal.is_sealed());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn interrupted_tools_are_recovered_by_identity_without_losing_legacy_journals() {
        let path = path("repair-tools");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        journal
            .record(
                "t1",
                EventKind::ToolInvoked {
                    tool_instance: "WebSearch:legacy".into(),
                    tool_name: "WebSearch".into(),
                    arguments: json!({"question":"legacy"}),
                    invocation_id: None,
                },
            )
            .unwrap();
        journal
            .record(
                "t2",
                EventKind::ToolInvoked {
                    tool_instance: "WebSearch:new".into(),
                    tool_name: "WebSearch".into(),
                    arguments: json!({"question":"identified"}),
                    invocation_id: Some("call-1".into()),
                },
            )
            .unwrap();
        assert!(journal.seal().is_err());

        let repaired = journal.repair_unfinished_tools("recovery").unwrap();
        assert_eq!(repaired.len(), 2);
        assert!(
            journal
                .state()
                .events
                .iter()
                .rev()
                .take(2)
                .all(|event| matches!(
                    &event.kind,
                    EventKind::ToolCompleted { outcome, .. }
                        if outcome.get("recovered").and_then(Value::as_bool) == Some(true)
                ))
        );
        journal.seal().unwrap();
        assert!(journal.is_sealed());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn identified_tool_completions_can_arrive_out_of_order() {
        let path = path("tool-identity");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        for id in ["call-1", "call-2"] {
            journal
                .record(
                    id,
                    EventKind::ToolInvoked {
                        tool_instance: format!("WebSearch:{id}"),
                        tool_name: "WebSearch".into(),
                        arguments: json!({"question":id}),
                        invocation_id: Some(id.into()),
                    },
                )
                .unwrap();
        }
        for id in ["call-1", "call-2"] {
            journal
                .record(
                    format!("{id}-complete"),
                    EventKind::ToolCompleted {
                        tool_instance: format!("WebSearch:{id}"),
                        tool_name: "WebSearch".into(),
                        outcome: json!({"ok":true}),
                        invocation_id: Some(id.into()),
                    },
                )
                .unwrap();
        }
        journal.seal().unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn invalid_box_operations_do_not_poison_the_append_only_journal() {
        let path = path("invalid-box-operation");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        let box_id = journal
            .create_box(
                "t1",
                "valid",
                BoxOwner::Controller,
                BoxContent::text("canonical"),
            )
            .unwrap();
        let valid_length = std::fs::metadata(&path).unwrap().len();

        for result in [
            journal.dehydrate_box("t2", BoxId(97)),
            journal.summarize_box("t3", BoxId(97), "summary"),
            journal.rehydrate_box("t4", BoxId(97)),
            journal.retire_box("t5", BoxId(97)),
        ] {
            assert_eq!(result.unwrap_err().to_string(), "box 97 does not exist");
            assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_length);
        }

        drop(journal);
        let reopened = SessionJournal::open_with_metadata(&path, metadata()).unwrap();
        assert_eq!(
            reopened
                .state()
                .box_state(box_id)
                .unwrap()
                .canonical
                .content,
            BoxContent::text("canonical")
        );
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
            .create_box(
                "t3",
                "tool result",
                BoxOwner::Controller,
                BoxContent::text("x".repeat(3_000)),
            )
            .unwrap();
        let measured_context = journal.state().projection();
        journal
            .record(
                "t4",
                EventKind::ProviderReceipt {
                    manifest_hash: "manifest-1".into(),
                    input_tokens: Some(777),
                    output_tokens: Some(3),
                    raw_context_tokens: Some(measured_context.raw_estimated_tokens),
                    provider_data: Value::Null,
                },
            )
            .unwrap();
        assert_eq!(journal.state().projection().estimated_tokens, 777);
        assert!(
            777 + measured_context
                .raw_estimated_tokens
                .saturating_sub(submitted.raw_estimated_tokens)
                > journal.state().projection().estimated_tokens
        );
        journal
            .create_box(
                "t5",
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
    fn legacy_provider_receipts_without_a_raw_context_anchor_remain_readable() {
        let receipt: EventKind = serde_json::from_value(json!({
            "type":"provider_receipt",
            "manifest_hash":"legacy-manifest",
            "input_tokens":123,
            "output_tokens":4,
            "provider_data":null
        }))
        .unwrap();
        assert!(matches!(
            receipt,
            EventKind::ProviderReceipt {
                raw_context_tokens: None,
                ..
            }
        ));
    }

    #[test]
    fn stateful_tool_slots_are_batched_append_only_and_do_not_see_summaries() {
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
        let reopened = SessionJournal::open_with_metadata(&path, metadata()).unwrap();
        assert_eq!(reopened.state().tools["rust-1"].slots.len(), 3);
        assert!(reopened.state().tools["rust-1"].slots[1].retired);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn tool_layout_orders_current_boxes_without_changing_their_identities() {
        let path = path("tool-layout");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        journal
            .apply_tool_slots_with_layout(
                "t1",
                "kweb",
                vec![
                    ToolSlotInput {
                        slot: "active".into(),
                        name: "Active node".into(),
                        content: BoxContent::text("ACTIVE NODE"),
                        retired: false,
                    },
                    ToolSlotInput {
                        slot: "direct".into(),
                        name: "Direct node".into(),
                        content: BoxContent::text("DIRECT NODE"),
                        retired: false,
                    },
                ],
                &["direct".into(), "active".into()],
            )
            .unwrap();
        let tool = &journal.state().tools["kweb"];
        let active_id = tool.slots[0].box_id;
        let direct_id = tool.slots[1].box_id;
        let rendered = journal.state().render();
        assert!(rendered.find("DIRECT NODE") < rendered.find("ACTIVE NODE"));
        assert_eq!(
            journal.state().tool_layouts["kweb"],
            vec![direct_id, active_id]
        );
        drop(journal);
        let reopened = SessionJournal::open_with_metadata(&path, metadata()).unwrap();
        assert_eq!(reopened.state().render(), rendered);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn limits_use_exact_floor_percentages() {
        let state = Chatend::opened(SessionMetadata {
            effective_context_tokens: 101,
            ..metadata()
        });
        assert_eq!(state.live_context_limit(), 70);
        assert_eq!(state.forced_ingress_context_limit(), 75);
        assert_eq!(state.ingress_initial_context_limit(), 75);
        assert_eq!(state.ingress_context_limit(), 101);
        assert_eq!(state.active_context_limit(), 70);
        let ingress = Chatend::opened(SessionMetadata {
            kind: SessionKind::HistoryIngress,
            effective_context_tokens: 101,
            ..metadata()
        });
        assert_eq!(ingress.active_context_limit(), 101);
        assert_eq!(estimate_tokens("1234"), 2);
    }

    #[test]
    fn new_box_projection_preview_matches_the_committed_projection() {
        let path = path("new-box-preview");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        let boxes = vec![
            (
                "User message".into(),
                BoxOwner::User,
                BoxContent::text("prospective user text"),
            ),
            (
                "attachment".into(),
                BoxOwner::User,
                BoxContent::text("prospective attachment text"),
            ),
        ];
        let preview = journal.state().projection_with_new_boxes(&boxes).unwrap();
        for (name, owner, content) in boxes {
            journal.create_box("t1", name, owner, content).unwrap();
        }
        assert_eq!(journal.state().projection(), preview);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn box_representation_preview_matches_the_batched_append() {
        let path = path("representation-plan");
        let mut journal = SessionJournal::create(&path, metadata()).unwrap();
        let hydrated = journal
            .create_box(
                "t1",
                "large result",
                BoxOwner::Controller,
                BoxContent::text("x".repeat(3_000)),
            )
            .unwrap();
        let summarized = journal
            .create_box(
                "t2",
                "Kennedy summary",
                BoxOwner::Kennedy,
                BoxContent::text("y".repeat(3_000)),
            )
            .unwrap();
        journal
            .summarize_box("t3", summarized, "important points")
            .unwrap();
        let desired = BTreeMap::from([
            (hydrated, BoxRepresentation::Dehydrated),
            (
                summarized,
                BoxRepresentation::Summarized("important points".into()),
            ),
        ]);
        let preview = journal
            .state()
            .projection_with_box_representations(&desired)
            .unwrap();
        let next_id = journal.state().next_id;
        let ids = journal.apply_box_representations("t4", &desired).unwrap();
        assert_eq!(ids, vec![EventId(next_id)]);
        assert_eq!(journal.state().projection(), preview);
        assert!(matches!(
            journal.state().box_state(hydrated).unwrap().representation,
            Representation::Dehydrated { .. }
        ));
        assert_eq!(
            journal
                .state()
                .box_state(summarized)
                .unwrap()
                .representation,
            Representation::Summarized {
                based_on: EventId(2),
                text: "important points".into(),
            }
        );
        std::fs::remove_file(path).unwrap();
    }
}
