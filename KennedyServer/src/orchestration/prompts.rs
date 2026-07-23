use std::{collections::HashMap, path::Path};

use anyhow::Context;
use serde_json::Value;

const PROMPT_FILES: [(&str, &str); 9] = [
    ("identity", "KennedyIdentity.txt"),
    ("conversationSession", "ConversationSession.txt"),
    ("freeTimeSession", "SelfTimeSession.txt"),
    ("historyIngressSession", "HistoryIngressSession.txt"),
    ("audioIngressSession", "AudioIngressSession.txt"),
    ("codexHarness", "CodexHarness.txt"),
    ("kmapBasics", "KmapBasics.txt"),
    ("readTools", "ReadTools.txt"),
    ("writeTools", "WriteTools.txt"),
];

#[derive(Clone, Debug)]
pub(crate) struct Manuals(HashMap<String, String>);

#[derive(Clone, Debug)]
pub(crate) struct RuntimeModel {
    pub provider: String,
    pub provider_kind: String,
    pub model: String,
    pub reasoning_effort: String,
    pub context_window_tokens: u64,
}

impl RuntimeModel {
    pub(crate) fn from_provider_payload(payload: &Value) -> anyhow::Result<Self> {
        let provider = required_string(payload, "default_provider")?;
        let selected = payload
            .get("providers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|entry| entry.get("name").and_then(Value::as_str) == Some(&provider))
            .context("the intelligence service omitted its default provider")?;
        let provider_kind = required_string(selected, "kind")?;
        let model = required_string(selected, "default_model")?;
        let reasoning_effort = required_string(selected, "reasoning_effort")?;
        let capabilities = selected
            .get("model_capabilities")
            .and_then(|value| value.get(&model));
        let context_window_tokens = capabilities
            .and_then(|value| value.get("context_window_tokens"))
            .and_then(Value::as_u64)
            .or_else(|| {
                selected
                    .get("context_window_tokens")
                    .and_then(Value::as_u64)
            })
            .context("the intelligence service omitted context_window_tokens")?;
        let max_input_tokens = capabilities
            .and_then(|value| value.get("max_input_tokens"))
            .and_then(Value::as_u64)
            .or_else(|| selected.get("max_input_tokens").and_then(Value::as_u64))
            .context("the intelligence service omitted max_input_tokens")?;
        anyhow::ensure!(
            context_window_tokens > 0 && max_input_tokens > 0,
            "the intelligence service returned invalid model limits"
        );
        Ok(Self {
            provider,
            provider_kind,
            model,
            reasoning_effort,
            context_window_tokens,
        })
    }

    pub(crate) fn attribution(&self) -> String {
        format!("{}-{}", self.model, self.reasoning_effort)
    }
}

impl Manuals {
    pub(crate) fn load(directory: &Path) -> anyhow::Result<Self> {
        let mut manuals = HashMap::new();
        for (key, filename) in PROMPT_FILES {
            let path = directory.join(filename);
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading system prompt {}", path.display()))?;
            let text = text.trim().to_owned();
            anyhow::ensure!(
                !text.is_empty(),
                "system prompt {} is empty",
                path.display()
            );
            manuals.insert(key.to_owned(), text);
        }
        Ok(Self(manuals))
    }

    pub(crate) fn compose_conversation(
        &self,
        runtime: &RuntimeModel,
        session_type: &str,
        session_context: &str,
    ) -> anyhow::Result<String> {
        let session_key = if session_type == "free-time" {
            "freeTimeSession"
        } else {
            "conversationSession"
        };
        self.compose(
            runtime,
            session_key,
            session_detail("conversation", session_type),
            session_type == "free-time",
            session_context,
            if session_type == "free-time" {
                "Self-time schedule"
            } else {
                "Telegram group context"
            },
        )
    }

    pub(crate) fn compose_ingress(
        &self,
        runtime: &RuntimeModel,
        source_session_type: &str,
        session_context: &str,
    ) -> anyhow::Result<String> {
        let session_key = if source_session_type == "audio" {
            "audioIngressSession"
        } else {
            "historyIngressSession"
        };
        self.compose(
            runtime,
            session_key,
            session_detail("ingress", source_session_type),
            true,
            session_context,
            "Telegram group context",
        )
    }

    fn compose(
        &self,
        runtime: &RuntimeModel,
        session_key: &str,
        detail: &str,
        writes: bool,
        session_context: &str,
        context_title: &str,
    ) -> anyhow::Result<String> {
        let mut sections = vec![
            section("Kennedy's identity", self.required("identity")?),
            section(
                "Session type",
                &format!("{}\n\n{detail}", self.required(session_key)?),
            ),
            section("Kmap basics", self.required("kmapBasics")?),
            section("Read-only tools", self.required("readTools")?),
        ];
        if writes {
            sections.push(section("Write tools", self.required("writeTools")?));
        }
        if runtime.provider_kind == "codex" {
            sections.push(section("Codex harness", self.required("codexHarness")?));
        }
        if !session_context.trim().is_empty() {
            sections.push(section(context_title, session_context.trim()));
        }
        sections.push(section(
            "Current runtime",
            &format!(
                "You are currently running on {} with {} thinking mode.",
                runtime.model, runtime.reasoning_effort
            ),
        ));
        Ok(sections.join("\n\n"))
    }

    fn required(&self, key: &str) -> anyhow::Result<&str> {
        self.0
            .get(key)
            .map(String::as_str)
            .with_context(|| format!("missing system prompt section {key}"))
    }
}

fn required_string(value: &Value, key: &str) -> anyhow::Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .with_context(|| format!("provider metadata is missing {key}"))
}

fn section(title: &str, content: &str) -> String {
    format!("{title}\n\n{content}")
}

fn session_detail(mode: &str, session_type: &str) -> &'static str {
    if mode == "conversation" {
        return match session_type {
            "free-time" => {
                "Channel: autonomous self time in Kennedy's backend harness. No live user response is expected. Read, web, and Kmap write tools are all authorized for this session."
            }
            "telegram-group" => {
                "Channel: Telegram group. This is a persistent session scoped to one participant and one group. Every group message accumulates as passive context, but only this participant's direct invocations trigger your response; other participants have separate sessions. Other participant roots are references that you may load if useful."
            }
            "telegram" => {
                "Channel: Telegram private message. The final conversational response is relayed by Kennedy's backend; a browser may observe the durable Chatend but does not run it."
            }
            _ => {
                "Channel: Kennedy's web UI. The user submitted this message through the frontend, while Kennedy's Rust backend owns and persists the Chatend and tool loop."
            }
        };
    }
    match session_type {
        "audio" => "Source: one chronologically placed piece of a vnote transcript.",
        "telegram-group" => {
            "Source: an archived Telegram group invocation or background group-chat batch."
        }
        "telegram" => "Source: an archived Telegram conversation (private message).",
        "free-time" => "Source: one archived clean-slate session from an autonomous self-time run.",
        _ => "Source: an archived browser conversation.",
    }
}
