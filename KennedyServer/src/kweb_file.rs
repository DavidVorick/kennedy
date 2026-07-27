use std::path::Path;

use anyhow::Context;
use kcode_kweb_db::ObjectId;

const FILE_MAGIC: &[u8; 8] = b"KFILE001";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredFile {
    pub object_id: ObjectId,
    pub file_name: String,
    pub media_type: String,
    pub transport_kind: Option<String>,
    pub bytes: Vec<u8>,
    pub enveloped: bool,
}

pub(crate) fn encode(
    pending_id: &str,
    file_name: Option<&str>,
    media_type: &str,
    transport_kind: Option<&str>,
    bytes: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    let file_name = safe_file_name(
        file_name.unwrap_or_default(),
        &format!("object-{}.bin", pending_id.trim_start_matches("pending:")),
    );
    let media_type = safe_media_type(media_type);
    let transport_kind = transport_kind
        .map(safe_transport_kind)
        .filter(|value| !value.is_empty());
    let file_name_len = u32::try_from(file_name.len()).context("object filename is too large")?;
    let media_type_len =
        u32::try_from(media_type.len()).context("object media type is too large")?;
    let transport_kind_len =
        u32::try_from(transport_kind.as_deref().map(str::len).unwrap_or_default())
            .context("object transport kind is too large")?;
    let content_len = u64::try_from(bytes.len()).context("object content is too large")?;
    let mut encoded = Vec::with_capacity(
        FILE_MAGIC.len()
            + 4
            + 4
            + 4
            + 8
            + file_name.len()
            + media_type.len()
            + transport_kind.as_deref().map(str::len).unwrap_or_default()
            + bytes.len(),
    );
    encoded.extend_from_slice(FILE_MAGIC);
    encoded.extend_from_slice(&file_name_len.to_be_bytes());
    encoded.extend_from_slice(&media_type_len.to_be_bytes());
    encoded.extend_from_slice(&transport_kind_len.to_be_bytes());
    encoded.extend_from_slice(&content_len.to_be_bytes());
    encoded.extend_from_slice(file_name.as_bytes());
    encoded.extend_from_slice(media_type.as_bytes());
    if let Some(transport_kind) = transport_kind {
        encoded.extend_from_slice(transport_kind.as_bytes());
    }
    encoded.extend_from_slice(&bytes);
    Ok(encoded)
}

pub(crate) fn decode(id: ObjectId, bytes: Vec<u8>) -> anyhow::Result<StoredFile> {
    if !bytes.starts_with(FILE_MAGIC) {
        let (media_type, extension) = sniff_media_type(&bytes);
        return Ok(StoredFile {
            object_id: id,
            file_name: format!("{id}.{extension}"),
            media_type: media_type.into(),
            transport_kind: None,
            bytes,
            enveloped: false,
        });
    }
    if bytes.len() < FILE_MAGIC.len() + 4 + 4 + 4 + 8 {
        anyhow::bail!("file object header is truncated");
    }
    let mut offset = FILE_MAGIC.len();
    let file_name_len = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let media_type_len = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let transport_kind_len =
        u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    let content_len = usize::try_from(u64::from_be_bytes(
        bytes[offset..offset + 8].try_into().unwrap(),
    ))
    .context("file object content length exceeds usize")?;
    offset += 8;
    let metadata_end = offset
        .checked_add(file_name_len)
        .and_then(|value| value.checked_add(media_type_len))
        .and_then(|value| value.checked_add(transport_kind_len))
        .context("file object metadata length overflow")?;
    let object_end = metadata_end
        .checked_add(content_len)
        .context("file object content length overflow")?;
    anyhow::ensure!(
        object_end == bytes.len(),
        "file object declared lengths differ from its payload"
    );
    let file_name = std::str::from_utf8(&bytes[offset..offset + file_name_len])
        .context("file object filename is not UTF-8")?;
    offset += file_name_len;
    let media_type = std::str::from_utf8(&bytes[offset..offset + media_type_len])
        .context("file object media type is not UTF-8")?;
    offset += media_type_len;
    let transport_kind = std::str::from_utf8(&bytes[offset..offset + transport_kind_len])
        .context("file object transport kind is not UTF-8")?;
    anyhow::ensure!(
        safe_file_name(file_name, "") == file_name && !file_name.is_empty(),
        "file object filename is unsafe"
    );
    anyhow::ensure!(
        safe_media_type(media_type) == media_type,
        "file object media type is unsafe"
    );
    anyhow::ensure!(
        transport_kind.is_empty() || safe_transport_kind(transport_kind) == transport_kind,
        "file object transport kind is unsafe"
    );
    Ok(StoredFile {
        object_id: id,
        file_name: file_name.into(),
        media_type: media_type.into(),
        transport_kind: (!transport_kind.is_empty()).then(|| transport_kind.into()),
        bytes: bytes[metadata_end..].to_vec(),
        enveloped: true,
    })
}

pub(crate) fn safe_file_name(value: &str, fallback: &str) -> String {
    let basename = Path::new(value)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let mut output = basename
        .chars()
        .filter(|character| !character.is_control())
        .map(|character| {
            if matches!(character, '/' | '\\' | '"' | '\r' | '\n') {
                '_'
            } else {
                character
            }
        })
        .collect::<String>();
    while output.len() > 255 {
        output.pop();
    }
    if output.trim().is_empty() {
        fallback.into()
    } else {
        output
    }
}

fn safe_media_type(value: &str) -> String {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 255
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
        || !value.contains('/')
    {
        "application/octet-stream".into()
    } else {
        value.into()
    }
}

fn safe_transport_kind(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        .take(64)
        .collect()
}

fn sniff_media_type(bytes: &[u8]) -> (&'static str, &'static str) {
    if bytes.starts_with(b"%PDF-") {
        ("application/pdf", "pdf")
    } else if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        ("image/png", "png")
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        ("image/jpeg", "jpg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        ("image/gif", "gif")
    } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        ("image/webp", "webp")
    } else if bytes.starts_with(b"OggS") {
        ("audio/ogg", "ogg")
    } else if bytes.starts_with(b"ID3") || bytes.starts_with(b"\xff\xfb") {
        ("audio/mpeg", "mp3")
    } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WAVE" {
        ("audio/wav", "wav")
    } else if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        ("video/mp4", "mp4")
    } else if bytes.starts_with(b"\x1a\x45\xdf\xa3") {
        ("video/webm", "webm")
    } else {
        ("application/octet-stream", "bin")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_preserves_metadata_and_bytes() {
        let id = ObjectId::from_bytes([128, 1, 2, 3, 4, 5]).unwrap();
        let encoded = encode(
            "pending:9",
            Some("../voice note.ogg"),
            "audio/ogg",
            Some("voice"),
            b"OggS payload".to_vec(),
        )
        .unwrap();
        let decoded = decode(id, encoded).unwrap();
        assert_eq!(decoded.file_name, "voice note.ogg");
        assert_eq!(decoded.media_type, "audio/ogg");
        assert_eq!(decoded.transport_kind.as_deref(), Some("voice"));
        assert_eq!(decoded.bytes, b"OggS payload");
        assert!(decoded.enveloped);
    }
}
