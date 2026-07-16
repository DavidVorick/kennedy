use std::path::Path;

use anyhow::Context;
use rusqlite::{Connection, OpenFlags};

const ESTIMATED_CHARACTERS_PER_TOKEN: u64 = 4;

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

pub(crate) fn measure(path: &Path) -> anyhow::Result<KmapSize> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening Kmap database {} read-only", path.display()))?;
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .context("configuring the Kmap size read")?;
    let mut statement = connection
        .prepare("SELECT short_name,short_description,long_description FROM knowledge_nodes")
        .context("reading Kmap knowledge nodes")?;
    let nodes = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    let mut node_count = 0_u64;
    let mut full_node_characters = 0_u64;
    let mut full_node_words = 0_u64;
    let mut long_description_characters = 0_u64;
    let mut long_description_words = 0_u64;
    for node in nodes {
        let (short_name, short_description, long_description) = node?;
        node_count = node_count.saturating_add(1);
        // Two newlines approximate the separators between the three text fields
        // when a complete node is placed into a model-facing context.
        full_node_characters = full_node_characters
            .saturating_add(character_count(&short_name))
            .saturating_add(character_count(&short_description))
            .saturating_add(character_count(&long_description))
            .saturating_add(2);
        full_node_words = full_node_words
            .saturating_add(word_count(&short_name))
            .saturating_add(word_count(&short_description))
            .saturating_add(word_count(&long_description));
        long_description_characters =
            long_description_characters.saturating_add(character_count(&long_description));
        long_description_words =
            long_description_words.saturating_add(word_count(&long_description));
    }

    Ok(KmapSize {
        node_count,
        full_node_characters,
        full_node_words,
        full_node_tokens: estimate_tokens(full_node_characters),
        long_description_characters,
        long_description_words,
        long_description_tokens: estimate_tokens(long_description_characters),
    })
}

pub(crate) fn render(size: &KmapSize) -> String {
    format!(
        "Kmap size estimate\n\nNodes: {}\nFull node text: ~{} tokens ({} words, {} characters)\nLong descriptions only: ~{} tokens ({} words, {} characters)\n\nEstimate: one token per {} Unicode characters; node history, provenance, connections, and other tables are excluded.",
        format_count(size.node_count),
        format_count(size.full_node_tokens),
        format_count(size.full_node_words),
        format_count(size.full_node_characters),
        format_count(size.long_description_tokens),
        format_count(size.long_description_words),
        format_count(size.long_description_characters),
        ESTIMATED_CHARACTERS_PER_TOKEN,
    )
}

fn character_count(value: &str) -> u64 {
    u64::try_from(value.chars().count()).unwrap_or(u64::MAX)
}

fn word_count(value: &str) -> u64 {
    u64::try_from(value.split_whitespace().count()).unwrap_or(u64::MAX)
}

fn estimate_tokens(characters: u64) -> u64 {
    characters.div_ceil(ESTIMATED_CHARACTERS_PER_TOKEN)
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
    use uuid::Uuid;

    struct TestDatabase(std::path::PathBuf);

    impl TestDatabase {
        fn new() -> Self {
            Self(
                std::env::temp_dir()
                    .join(format!("kennedy-kmap-size-test-{}.sqlite3", Uuid::new_v4())),
            )
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let mut wal = self.0.as_os_str().to_owned();
            wal.push("-wal");
            let _ = std::fs::remove_file(std::path::PathBuf::from(wal));
            let mut shm = self.0.as_os_str().to_owned();
            shm.push("-shm");
            let _ = std::fs::remove_file(std::path::PathBuf::from(shm));
        }
    }

    #[test]
    fn counts_only_current_knowledge_node_text() {
        let database = TestDatabase::new();
        let connection = Connection::open(&database.0).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE knowledge_nodes(short_name TEXT NOT NULL,short_description TEXT NOT NULL,long_description TEXT NOT NULL);\
                 CREATE TABLE data_history_nodes(data TEXT NOT NULL);\
                 INSERT INTO knowledge_nodes VALUES('Alpha','Short note','Long description here');\
                 INSERT INTO knowledge_nodes VALUES('Unicode 🦀','Second','More words');\
                 INSERT INTO data_history_nodes VALUES('this deliberately enormous history text is excluded');",
            )
            .unwrap();
        drop(connection);

        let size = measure(&database.0).unwrap();
        assert_eq!(size.node_count, 2);
        assert_eq!(size.full_node_characters, 65);
        assert_eq!(size.full_node_words, 11);
        assert_eq!(size.full_node_tokens, 17);
        assert_eq!(size.long_description_characters, 31);
        assert_eq!(size.long_description_words, 5);
        assert_eq!(size.long_description_tokens, 8);
    }

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
