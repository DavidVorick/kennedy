use std::path::Path;

use anyhow::Context;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct KmapSize {
    pub node_count: u64,
    pub full_node_characters: u64,
    pub full_node_words: u64,
    pub full_node_tokens: u64,
    pub long_description_characters: u64,
    pub long_description_words: u64,
    pub long_description_tokens: u64,
}

pub(crate) fn measure(path: &Path, artifact_directory: &Path) -> anyhow::Result<KmapSize> {
    let kmap = kweb_db_core::Kmap::open_with_artifacts(path, artifact_directory)
        .map_err(anyhow::Error::new)
        .with_context(|| format!("opening Kmap database {}", path.display()))?;
    let stats = kmap.stats().map_err(anyhow::Error::new)?;
    Ok(KmapSize {
        node_count: stats.node_count(),
        full_node_characters: stats.full_node_characters(),
        full_node_words: stats.full_node_words(),
        full_node_tokens: stats.estimated_full_node_tokens(),
        long_description_characters: stats.long_description_characters(),
        long_description_words: stats.long_description_words(),
        long_description_tokens: stats.estimated_long_description_tokens(),
    })
}

pub(crate) fn render(size: &KmapSize) -> String {
    format!(
        "Kmap size estimate\n\nNodes: {}\nFull node text: ~{} tokens ({} words, {} characters)\nLong descriptions only: ~{} tokens ({} words, {} characters)\n\nEstimate: one token per 4 Unicode characters; node history, provenance, connections, and other tables are excluded.",
        format_count(size.node_count),
        format_count(size.full_node_tokens),
        format_count(size.full_node_words),
        format_count(size.full_node_characters),
        format_count(size.long_description_tokens),
        format_count(size.long_description_words),
        format_count(size.long_description_characters),
    )
}

fn format_count(value: u64) -> String {
    let digits = value.to_string();
    let first_group = digits.len() % 3;
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);
    if first_group != 0 {
        formatted.push_str(&digits[..first_group]);
    }
    for chunk in digits.as_bytes()[first_group..].chunks(3) {
        if !formatted.is_empty() {
            formatted.push(',');
        }
        formatted.push_str(std::str::from_utf8(chunk).expect("decimal digits are UTF-8"));
    }
    formatted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_readable_grouped_estimates_and_scope() {
        let output = render(&KmapSize {
            node_count: 12_345,
            full_node_characters: 4_938_268,
            full_node_words: 987_654,
            full_node_tokens: 1_234_567,
            long_description_characters: 3_950_616,
            long_description_words: 800_000,
            long_description_tokens: 987_654,
        });
        assert!(output.contains("Nodes: 12,345"));
        assert!(output.contains("Full node text: ~1,234,567 tokens"));
        assert!(output.contains("Long descriptions only: ~987,654 tokens"));
        assert!(output.contains("history, provenance, connections, and other tables are excluded"));
    }

    #[test]
    fn count_formatting_handles_small_and_grouped_values() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(12), "12");
        assert_eq!(format_count(1_000), "1,000");
        assert_eq!(format_count(1_234_567), "1,234,567");
    }
}
