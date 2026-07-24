use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};

use anyhow::{Context, ensure};
use kcode_kweb_db::WriterId;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const STATE_MAGIC: &[u8; 8] = b"KWSTATE3";

pub(crate) fn install_permanent_writer(
    root: &Path,
    permanent_writer: WriterId,
) -> anyhow::Result<Vec<WriterId>> {
    let state_path = root.join("state.kws");
    let bytes =
        fs::read(&state_path).with_context(|| format!("reading {}", state_path.display()))?;
    let (prefix, existing) = decode_state_writer_section(&bytes)?;
    if existing.first() == Some(&permanent_writer) {
        return Ok(existing);
    }
    ensure!(
        !existing.contains(&permanent_writer),
        "the permanent writer is configured at the wrong priority"
    );
    let mut writers = vec![permanent_writer];
    writers.extend(existing);
    let mut payload = prefix;
    put_u64(
        &mut payload,
        u64::try_from(writers.len()).context("too many configured writers")?,
    );
    for writer in &writers {
        payload.extend_from_slice(&writer.to_bytes());
    }
    let encoded = encode_state_record(&payload);
    write_atomic(&state_path, &encoded)?;
    Ok(writers)
}

fn decode_state_writer_section(bytes: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<WriterId>)> {
    ensure!(bytes.len() >= 48, "Kweb state record is truncated");
    ensure!(
        &bytes[..8] == STATE_MAGIC,
        "Kweb state record has unknown magic"
    );
    let payload_length = u64::from_be_bytes(bytes[8..16].try_into()?) as usize;
    ensure!(
        bytes.len() == 16 + payload_length + 32,
        "Kweb state record length is invalid"
    );
    ensure!(
        Sha256::digest(&bytes[..16 + payload_length]).as_slice() == &bytes[16 + payload_length..],
        "Kweb state checksum is invalid"
    );
    let payload = &bytes[16..16 + payload_length];
    ensure!(payload.len() >= 28, "Kweb state payload is truncated");
    let mut offset = 4 + 8 + 8;
    let heads = read_u64(payload, &mut offset)? as usize;
    offset = offset
        .checked_add(heads.checked_mul(32).context("head count overflow")?)
        .context("head offset overflow")?;
    ensure!(
        offset + 8 <= payload.len(),
        "Kweb state heads are truncated"
    );
    let prefix = payload[..offset].to_vec();
    let writer_count = read_u64(payload, &mut offset)? as usize;
    ensure!(
        offset + writer_count * 32 == payload.len(),
        "Kweb state writer list is invalid"
    );
    let mut writers = Vec::with_capacity(writer_count);
    for _ in 0..writer_count {
        let bytes: [u8; 32] = payload[offset..offset + 32].try_into()?;
        writers.push(WriterId::from_verifying_key(bytes).map_err(anyhow::Error::new)?);
        offset += 32;
    }
    ensure!(!writers.is_empty(), "Kweb state has no writers");
    Ok((prefix, writers))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> anyhow::Result<u64> {
    let end = offset.checked_add(8).context("state offset overflow")?;
    ensure!(end <= bytes.len(), "Kweb state field is truncated");
    let value = u64::from_be_bytes(bytes[*offset..end].try_into()?);
    *offset = end;
    Ok(value)
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn encode_state_record(payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16 + payload.len() + 32);
    bytes.extend_from_slice(STATE_MAGIC);
    bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(&Sha256::digest(&bytes));
    bytes
}

fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let temporary = parent.join(format!(".state.kws.{}.tmp", Uuid::new_v4()));
    let result = (|| -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    File::open(path)
        .with_context(|| format!("opening directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("syncing directory {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kcode_kweb_db::{Config, KwebDb, NoopGossip};

    use super::*;

    #[test]
    fn permanent_writer_installation_preserves_existing_writers() {
        let directory =
            std::env::temp_dir().join(format!("kennedy-kweb-writer-{}", Uuid::new_v4()));
        let existing_key = [7_u8; 32];
        let existing_writer = WriterId::from_signing_key(&existing_key);
        let database = KwebDb::open(
            &directory,
            Config {
                signing_key: existing_key,
                writers_by_priority: vec![existing_writer],
                gossip: Arc::new(NoopGossip),
            },
        )
        .unwrap();
        drop(database);

        let permanent_key = [8_u8; 32];
        let permanent_writer = WriterId::from_signing_key(&permanent_key);
        let writers = install_permanent_writer(&directory, permanent_writer).unwrap();
        assert_eq!(writers, vec![permanent_writer, existing_writer]);

        let reopened = KwebDb::open(
            &directory,
            Config {
                signing_key: permanent_key,
                writers_by_priority: writers,
                gossip: Arc::new(NoopGossip),
            },
        )
        .unwrap();
        drop(reopened);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
