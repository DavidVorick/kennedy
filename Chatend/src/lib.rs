use serde_json::Value;

const CHATEND_SEPARATOR: &str = "\n\n────────────────────────\n\n";

/// Compose the exact plain text supplied to the model from backend-owned messages.
pub fn canonical_chatend_text(messages: &[Value], usage: Option<&Value>) -> String {
    let mut value = messages
        .iter()
        .filter_map(|message| {
            let content = message.get("content").and_then(Value::as_str)?.trim();
            if content.is_empty() {
                return None;
            }
            let role = message
                .get("display_role")
                .and_then(Value::as_str)
                .unwrap_or_else(|| match message.get("role").and_then(Value::as_str) {
                    Some("user") => "David",
                    Some("assistant") => "Kennedy",
                    _ => "System context",
                });
            Some(format!("{role}\n\n{content}"))
        })
        .collect::<Vec<_>>()
        .join(CHATEND_SEPARATOR);

    if let Some(usage) = usage {
        let known = usage
            .get("contextKnown")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let tokens = usage
            .get("contextTokens")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let window = usage
            .get("contextWindowTokens")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let progress = if known {
            format!("context window usage: {tokens} / {window}")
        } else {
            format!("context window usage: unknown / {window}")
        };
        if !value.is_empty() {
            value.push_str(CHATEND_SEPARATOR);
        }
        value.push_str(&progress);
    }
    value
}

/// Add canonical text to an archive produced before `chatendText` was persisted.
/// Existing canonical text is never reinterpreted or replaced.
pub fn hydrate_archive_chatend_text(archive: &mut Value) {
    if archive.get("chatendText").and_then(Value::as_str).is_none()
        && let Some(messages) = archive.get("messages").and_then(Value::as_array)
    {
        let text = canonical_chatend_text(messages, archive.get("usage"));
        if !text.is_empty() {
            archive["chatendText"] = Value::String(text);
        }
    }

    if let Some(segments) = archive
        .pointer_mut("/fullHistory/segments")
        .and_then(Value::as_array_mut)
    {
        for segment in segments {
            hydrate_archive_chatend_text(segment);
        }
    }
}

/// Hydrate every Kennedy archive exposed within a durable backend state object.
pub fn hydrate_state_chatend_text(state: &mut Value) {
    if state.get("format").and_then(Value::as_str) == Some("kennedy-chatend") {
        hydrate_archive_chatend_text(state);
    }
    for key in ["archive", "historyIngress"] {
        if let Some(archive) = state.get_mut(key) {
            hydrate_archive_chatend_text(archive);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_text_preserves_the_model_facing_format() {
        let messages = json!([
            {"role":"system","display_role":"Agent manuals","content":"  Instructions  "},
            {"role":"user","content":"Hello"},
            {"role":"assistant","content":""}
        ]);
        let usage = json!({
            "contextKnown": true,
            "contextTokens": 12,
            "contextWindowTokens": 100
        });
        assert_eq!(
            canonical_chatend_text(messages.as_array().unwrap(), Some(&usage)),
            "Agent manuals\n\nInstructions\n\n────────────────────────\n\nDavid\n\nHello\n\n────────────────────────\n\ncontext window usage: 12 / 100"
        );
    }

    #[test]
    fn legacy_archives_and_reset_segments_receive_canonical_text() {
        let mut state = json!({
            "archive": {
                "format": "kennedy-chatend",
                "messages": [{"role":"user","content":"Current"}],
                "usage": {"contextWindowTokens":200},
                "fullHistory": {"segments":[{
                    "messages":[{"role":"assistant","content":"Earlier"}]
                }]}
            },
            "historyIngress": {
                "format":"kennedy-chatend",
                "messages":[{"role":"user","content":"Ingress"}]
            }
        });
        hydrate_state_chatend_text(&mut state);
        assert_eq!(
            state
                .pointer("/archive/chatendText")
                .and_then(Value::as_str),
            Some(
                "David\n\nCurrent\n\n────────────────────────\n\ncontext window usage: unknown / 200"
            )
        );
        assert_eq!(
            state
                .pointer("/archive/fullHistory/segments/0/chatendText")
                .and_then(Value::as_str),
            Some("Kennedy\n\nEarlier")
        );
        assert_eq!(
            state
                .pointer("/historyIngress/chatendText")
                .and_then(Value::as_str),
            Some("David\n\nIngress")
        );
    }

    #[test]
    fn persisted_canonical_text_is_never_reformatted() {
        let mut archive = json!({
            "chatendText":"Exact backend text\n  including spacing  ",
            "messages":[{"role":"user","content":"Different"}]
        });
        hydrate_archive_chatend_text(&mut archive);
        assert_eq!(
            archive.get("chatendText").and_then(Value::as_str),
            Some("Exact backend text\n  including spacing  ")
        );
    }
}
