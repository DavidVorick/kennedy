use std::{ffi::OsStr, fs, path::Path, str::FromStr};

use anyhow::Context;
use kcode_kweb_db::{Config, KwebDb, NodeId};

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

pub(crate) fn measure(path: &Path, config: Config) -> anyhow::Result<KmapSize> {
    let database = KwebDb::open(path, config)
        .map_err(anyhow::Error::new)
        .with_context(|| format!("opening Kweb database {}", path.display()))?;
    let mut size = KmapSize {
        node_count: 0,
        full_node_characters: 0,
        full_node_words: 0,
        full_node_tokens: 0,
        long_description_characters: 0,
        long_description_words: 0,
        long_description_tokens: 0,
    };
    for id in node_ids(path)? {
        let node = database.get_node(id).map_err(anyhow::Error::new)?;
        let full = format!(
            "{}{}{}",
            node.data.short_name, node.data.short_description, node.data.long_description
        );
        size.node_count += 1;
        size.full_node_characters += count_characters(&full);
        size.full_node_words += count_words(&full);
        size.long_description_characters += count_characters(&node.data.long_description);
        size.long_description_words += count_words(&node.data.long_description);
    }
    size.full_node_tokens = size.full_node_characters.div_ceil(4);
    size.long_description_tokens = size.long_description_characters.div_ceil(4);
    Ok(size)
}

fn node_ids(root: &Path) -> anyhow::Result<Vec<NodeId>> {
    let directory = root.join("nodes");
    let mut ids = Vec::new();
    for shard in
        fs::read_dir(&directory).with_context(|| format!("reading {}", directory.display()))?
    {
        let shard = shard?;
        if !shard.file_type()?.is_dir() {
            continue;
        }
        let prefix = shard.file_name();
        let prefix = prefix.to_str().context("Kweb node shard is not UTF-8")?;
        for entry in fs::read_dir(shard.path())? {
            let entry = entry?;
            if !entry.file_type()?.is_file() || entry.path().extension() != Some(OsStr::new("kwn"))
            {
                continue;
            }
            let stem = entry
                .path()
                .file_stem()
                .and_then(OsStr::to_str)
                .context("Kweb node filename is not UTF-8")?
                .to_owned();
            ids.push(
                NodeId::from_str(&format!("{prefix}{stem}"))
                    .map_err(anyhow::Error::new)
                    .context("decoding a Kweb node filename")?,
            );
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn count_characters(value: &str) -> u64 {
    u64::try_from(value.chars().count()).unwrap_or(u64::MAX)
}

fn count_words(value: &str) -> u64 {
    u64::try_from(value.split_whitespace().count()).unwrap_or(u64::MAX)
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
