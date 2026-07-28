use std::{collections::HashMap, path::Path};

use anyhow::Context;
use chrono::{DateTime, Datelike, Timelike, Utc};

const PROMPT_FILES: [(&str, &str); 12] = [
    ("identity", "KennedyIdentity.txt"),
    ("conversationSession", "ConversationSession.txt"),
    ("freeTimeSession", "SelfTimeSession.txt"),
    ("wakeupSession", "WakeupSession.txt"),
    ("historyIngressSession", "HistoryIngressSession.txt"),
    ("audioIngressSession", "AudioIngressSession.txt"),
    ("telegramSession", "TelegramSession.txt"),
    ("telegramGroupSession", "TelegramGroupSession.txt"),
    ("codexHarness", "CodexHarness.txt"),
    ("kmapBasics", "KmapBasics.txt"),
    ("readTools", "ReadTools.txt"),
    ("writeTools", "WriteTools.txt"),
];

#[derive(Clone, Debug)]
pub(crate) struct Manuals(HashMap<String, String>);

#[derive(Clone, Debug)]
pub(crate) struct RuntimeModel {
    pub model: String,
    pub reasoning_effort: String,
    pub context_window_tokens: u64,
}

impl RuntimeModel {
    pub(crate) fn from_intelligence(runtime: kcode_intelligence_router::RuntimeModel) -> Self {
        Self {
            model: runtime.model,
            reasoning_effort: runtime.reasoning_effort,
            context_window_tokens: runtime.context_window_tokens,
        }
    }

    pub(crate) fn attribution(&self) -> String {
        format!("{}-{}", self.model, self.reasoning_effort)
    }

    #[cfg(test)]
    pub(crate) fn testing() -> Self {
        Self {
            model: "gpt-5.6-sol".into(),
            reasoning_effort: "xhigh".into(),
            context_window_tokens: 1_000_000,
        }
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
        let (session_key, writes) = match session_type {
            "free-time" => ("freeTimeSession", true),
            "wakeup" => ("wakeupSession", true),
            _ => ("conversationSession", false),
        };
        self.compose(runtime, session_key, writes, session_context, session_type)
    }

    pub(crate) fn compose_ingress(
        &self,
        runtime: &RuntimeModel,
        source_session_type: &str,
    ) -> anyhow::Result<String> {
        let session_key = if source_session_type == "audio" {
            "audioIngressSession"
        } else {
            "historyIngressSession"
        };
        self.compose(runtime, session_key, true, "", source_session_type)
    }

    fn compose(
        &self,
        runtime: &RuntimeModel,
        session_key: &str,
        writes: bool,
        session_context: &str,
        channel: &str,
    ) -> anyhow::Result<String> {
        let mut sections = vec![
            section("Kennedy's identity", self.required("identity")?),
            section("Session type", self.required(session_key)?),
        ];
        if matches!(channel, "telegram" | "telegram-group") {
            sections.push(section(
                "Telegram session",
                self.required("telegramSession")?,
            ));
        }
        if channel == "telegram-group" {
            sections.push(section(
                "Telegram group session",
                self.required("telegramGroupSession")?,
            ));
        }
        sections.extend([
            section("Kmap basics", self.required("kmapBasics")?),
            section(
                "Critical Kmap and context tools",
                self.required("readTools")?,
            ),
        ]);
        if writes {
            sections.push(section("Write tools", self.required("writeTools")?));
        }
        sections.push(section("Codex harness", self.required("codexHarness")?));
        if !session_context.trim().is_empty() {
            sections.push(section("Self-time schedule", session_context.trim()));
        }
        sections.push(section(
            "Current runtime",
            &runtime_description(runtime, Utc::now()),
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

fn section(title: &str, content: &str) -> String {
    format!("{title}\n\n{content}")
}

pub(crate) fn runtime_description(runtime: &RuntimeModel, current_time: DateTime<Utc>) -> String {
    format!(
        "You are currently running on {} with {} thinking mode. The current date and time is {}.",
        runtime.model,
        runtime.reasoning_effort,
        human_utc_datetime(current_time)
    )
}

pub(crate) fn human_utc_datetime(value: DateTime<Utc>) -> String {
    let day = value.day();
    let suffix = match day % 100 {
        11..=13 => "th",
        _ => match day % 10 {
            1 => "st",
            2 => "nd",
            3 => "rd",
            _ => "th",
        },
    };
    let hour = match value.hour() % 12 {
        0 => 12,
        hour => hour,
    };
    let period = if value.hour() < 12 { "am" } else { "pm" };
    format!(
        "{} {day}{suffix}, {}, {hour}:{:02}{period} UTC",
        value.format("%B"),
        value.year(),
        value.minute()
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::TimeZone;

    use super::*;

    fn testing_manuals() -> Manuals {
        Manuals(
            PROMPT_FILES
                .into_iter()
                .map(|(key, _)| (key.to_owned(), format!("[{key}]")))
                .collect::<HashMap<_, _>>(),
        )
    }

    #[test]
    fn human_time_uses_ordinals_and_unambiguous_twelve_hour_clock() {
        assert_eq!(
            human_utc_datetime(Utc.with_ymd_and_hms(2026, 7, 6, 4, 23, 0).unwrap()),
            "July 6th, 2026, 4:23am UTC"
        );
        assert_eq!(
            human_utc_datetime(Utc.with_ymd_and_hms(2026, 7, 11, 16, 34, 0).unwrap()),
            "July 11th, 2026, 4:34pm UTC"
        );
        assert_eq!(
            human_utc_datetime(Utc.with_ymd_and_hms(2026, 7, 22, 0, 5, 0).unwrap()),
            "July 22nd, 2026, 12:05am UTC"
        );
    }

    #[test]
    fn telegram_layers_are_scoped_to_telegram_channels() {
        let manuals = testing_manuals();
        let runtime = RuntimeModel::testing();
        let browser = manuals
            .compose_conversation(&runtime, "conversation", "")
            .unwrap();
        assert!(!browser.contains("[telegramSession]"));
        assert!(!browser.contains("[telegramGroupSession]"));

        let private = manuals
            .compose_conversation(&runtime, "telegram", "")
            .unwrap();
        assert!(private.contains("[telegramSession]"));
        assert!(!private.contains("[telegramGroupSession]"));

        let group = manuals
            .compose_conversation(&runtime, "telegram-group", "")
            .unwrap();
        assert!(group.contains("[telegramSession]"));
        assert!(group.contains("[telegramGroupSession]"));

        let group_ingress = manuals.compose_ingress(&runtime, "telegram-group").unwrap();
        assert!(group_ingress.contains("[telegramSession]"));
        assert!(group_ingress.contains("[telegramGroupSession]"));

        let audio = manuals.compose_ingress(&runtime, "audio").unwrap();
        assert!(!audio.contains("[telegramSession]"));
        assert!(!audio.contains("[telegramGroupSession]"));
    }

    #[test]
    fn wakeup_sessions_have_their_own_autonomous_write_prompt() {
        let prompt = testing_manuals()
            .compose_conversation(&RuntimeModel::testing(), "wakeup", "")
            .unwrap();
        assert!(prompt.contains("[wakeupSession]"));
        assert!(prompt.contains("[writeTools]"));
        assert!(!prompt.contains("[conversationSession]"));
        assert!(!prompt.contains("[telegramSession]"));
    }

    #[test]
    fn wakeup_manual_makes_confident_silence_explicit() {
        let manual = include_str!("../../runtime/system-prompts/WakeupSession.txt");
        assert!(manual.contains("only when you are confident"));
        assert!(manual.contains("choosing silence is a complete and valid outcome"));
        assert!(manual.contains("call EndSession"));
    }

    #[test]
    fn load_node_manual_shows_the_exact_bridge_payload() {
        let manual = include_str!("../../runtime/system-prompts/ReadTools.txt");
        assert!(manual.contains(r#"{"name":"LoadNode","arguments":{"identifier":"Nl_hWdsl"}}"#));
    }

    #[test]
    fn boxes_into_objects_guidance_covers_the_call_and_requested_use_cases() {
        let read_tools = include_str!("../../runtime/system-prompts/ReadTools.txt");
        assert!(
            read_tools.contains(r#"{"name":"BoxesIntoObjects","arguments":{"boxIds":[12,15]}}"#)
        );
        assert!(read_tools.contains("codebases"));
        assert!(read_tools.contains("transcripts"));
        assert!(read_tools.contains("Granola notes"));
        assert!(read_tools.contains("copy-pastes"));
    }
}
