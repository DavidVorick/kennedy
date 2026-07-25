use std::net::SocketAddr;

use axum::{
    extract::{Multipart, Request},
    http::HeaderMap,
    middleware::Next,
};
use teloxide::{
    payloads::SendDocumentSetters,
    requests::Request as TelegramRequest,
    types::{InputFile, ReplyParameters},
};

use super::*;

const TELEGRAM_CAPTION_LIMIT: usize = 1_024;
const MAX_FILE_NAME_CHARACTERS: usize = 255;
const MAX_MIME_TYPE_CHARACTERS: usize = 255;

pub(super) fn loopback_bind(value: &str) -> anyhow::Result<SocketAddr> {
    let address: SocketAddr = value.parse().with_context(|| {
        format!("Telegram relay bind must be a literal socket address: {value}")
    })?;
    anyhow::ensure!(
        address.ip().is_loopback(),
        "Telegram relay API must bind to a loopback IPv4 or IPv6 address"
    );
    Ok(address)
}

fn browser_origin_allowed(
    headers: &HeaderMap,
    allowed_origins: &[HeaderValue],
) -> Result<(), ApiError> {
    let mut origins = headers.get_all(header::ORIGIN).iter();
    if let Some(origin) = origins.next() {
        if origins.next().is_some()
            || !allowed_origins
                .iter()
                .any(|allowed_origin| allowed_origin == origin)
        {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "origin_forbidden",
                "This browser origin is not allowed to access the Telegram relay.",
            ));
        }
        return Ok(());
    }

    let browser_metadata_present = headers.contains_key(HeaderName::from_static("sec-fetch-site"))
        || headers.contains_key(HeaderName::from_static("sec-fetch-mode"))
        || headers.contains_key(HeaderName::from_static("sec-fetch-dest"));
    if browser_metadata_present {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "origin_required",
            "Browser requests to the Telegram relay must include an allowed Origin header.",
        ));
    }

    Ok(())
}

pub(super) async fn enforce_browser_origin(
    State(allowed_origins): State<Vec<HeaderValue>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    browser_origin_allowed(request.headers(), &allowed_origins)?;

    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        HeaderName::from_static("pragma"),
        HeaderValue::from_static("no-cache"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DetachGroupSession {
    group_id: String,
    telegram_user_id: i64,
}

pub(super) async fn detach_group_session(
    State(state): State<AppState>,
    Path(conversation_id): Path<String>,
    Json(input): Json<DetachGroupSession>,
) -> Result<Json<Value>, ApiError> {
    validate_conversation_id(&conversation_id)?;
    let group_id = input.group_id.trim();
    if group_id.is_empty() || group_id.len() > 200 || group_id.chars().any(char::is_control) {
        return Err(ApiError::bad("groupId is not a valid opaque group ID."));
    }

    let db = state.db.lock().map_err(ApiError::internal)?;
    let changed = db
        .execute(
            "UPDATE telegram_group_sessions
             SET current_conversation_id=NULL,updated_at=?1
             WHERE group_id=?2 AND telegram_user_id=?3
               AND current_conversation_id=?4",
            params![
                Utc::now().to_rfc3339(),
                group_id,
                input.telegram_user_id,
                conversation_id
            ],
        )
        .map_err(ApiError::internal)?;
    if changed != 1 {
        return Err(ApiError::conflict(
            "This Telegram group session is absent, detached, or bound to a newer conversation.",
        ));
    }

    Ok(Json(json!({
        "conversationId":conversation_id,
        "groupId":group_id,
        "telegramUserId":input.telegram_user_id,
        "status":"detached",
    })))
}

#[derive(Debug, Default)]
struct OutboundFile {
    conversation_id: Option<String>,
    kind: Option<String>,
    explicit_file_name: Option<String>,
    part_file_name: Option<String>,
    mime_type: Option<String>,
    caption: Option<String>,
    complete: bool,
    complete_seen: bool,
    bytes: Option<Vec<u8>>,
}

fn reject_duplicate(seen: bool, field_name: &str) -> Result<(), ApiError> {
    if seen {
        return Err(ApiError::bad(format!(
            "Multipart field {field_name:?} may be supplied only once."
        )));
    }
    Ok(())
}

async fn parse_outbound_file(
    mut multipart: Multipart,
    maximum_bytes: usize,
    native_media: bool,
) -> Result<OutboundFile, ApiError> {
    let mut output = OutboundFile::default();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::bad(format!("Invalid multipart file request: {error}")))?
    {
        let field_name = field.name().unwrap_or("").to_owned();
        match field_name.as_str() {
            "kind" if native_media => {
                reject_duplicate(output.kind.is_some(), &field_name)?;
                output.kind = Some(
                    field
                        .text()
                        .await
                        .map_err(|error| ApiError::bad(format!("Invalid kind field: {error}")))?
                        .trim()
                        .to_owned(),
                );
            }
            "conversationId" => {
                reject_duplicate(output.conversation_id.is_some(), &field_name)?;
                output.conversation_id = Some(
                    field
                        .text()
                        .await
                        .map_err(|error| {
                            ApiError::bad(format!("Invalid conversationId field: {error}"))
                        })?
                        .trim()
                        .to_owned(),
                );
            }
            "fileName" => {
                reject_duplicate(output.explicit_file_name.is_some(), &field_name)?;
                output.explicit_file_name =
                    Some(field.text().await.map_err(|error| {
                        ApiError::bad(format!("Invalid fileName field: {error}"))
                    })?);
            }
            "caption" => {
                reject_duplicate(output.caption.is_some(), &field_name)?;
                output.caption =
                    Some(field.text().await.map_err(|error| {
                        ApiError::bad(format!("Invalid caption field: {error}"))
                    })?);
            }
            "complete" => {
                reject_duplicate(output.complete_seen, &field_name)?;
                output.complete_seen = true;
                let value = field
                    .text()
                    .await
                    .map_err(|error| ApiError::bad(format!("Invalid complete field: {error}")))?
                    .trim()
                    .to_ascii_lowercase();
                output.complete = match value.as_str() {
                    "true" | "1" => true,
                    "false" | "0" | "" => false,
                    _ => {
                        return Err(ApiError::bad("complete must be true, false, 1, or 0."));
                    }
                };
            }
            "file" => {
                reject_duplicate(output.bytes.is_some(), &field_name)?;
                output.part_file_name = field.file_name().map(ToOwned::to_owned);
                output.mime_type = field.content_type().map(ToOwned::to_owned);
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|error| ApiError::bad(format!("Invalid file part: {error}")))?;
                if bytes.len() > maximum_bytes {
                    return Err(ApiError::bad(format!(
                        "The file exceeds the configured {maximum_bytes}-byte Telegram media limit."
                    )));
                }
                output.bytes = Some(bytes.to_vec());
            }
            _ => {
                return Err(ApiError::bad(format!(
                    "Unknown multipart field {field_name:?}."
                )));
            }
        }
    }

    Ok(output)
}

fn validate_file_name(value: &str) -> Result<(), ApiError> {
    if value.trim().is_empty()
        || value.chars().count() > MAX_FILE_NAME_CHARACTERS
        || value
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | '\\'))
    {
        return Err(ApiError::bad(
            "fileName must be a nonempty path-free name of at most 255 characters.",
        ));
    }
    Ok(())
}

fn validate_mime_type(value: &str) -> Result<(), ApiError> {
    if value.trim().is_empty()
        || value.chars().count() > MAX_MIME_TYPE_CHARACTERS
        || value.chars().any(char::is_control)
    {
        return Err(ApiError::bad(
            "The file content type must be a nonempty value of at most 255 characters.",
        ));
    }
    Ok(())
}

fn outbound_event(
    db: &Connection,
    event_id: &str,
    conversation_id: &str,
) -> Result<RelayEvent, ApiError> {
    let event = fetch_event(db, event_id)?;
    if event.status == "complete" {
        return Err(ApiError::conflict(
            "The Telegram event is already complete.",
        ));
    }
    if event.conversation_id.as_deref() != Some(conversation_id) {
        return Err(ApiError::conflict(
            "The event is not bound to this conversation.",
        ));
    }
    Ok(event)
}

fn reconcile_outbound_event(
    db: &Connection,
    event_id: &str,
    conversation_id: &str,
    complete: bool,
    delivery_label: &str,
) -> Result<RelayEvent, ApiError> {
    if complete {
        let changed = db
            .execute(
                "UPDATE telegram_events SET status='complete',completed_at=?1
                 WHERE id=?2 AND status<>'complete' AND conversation_id=?3",
                params![Utc::now().to_rfc3339(), event_id, conversation_id],
            )
            .map_err(ApiError::internal)?;
        if changed != 1 {
            return Err(ApiError::conflict(format!(
                "The {delivery_label} was sent, but the event binding changed before completion."
            )));
        }
    } else {
        let current = fetch_event(db, event_id)?;
        if current.status == "complete"
            || current.conversation_id.as_deref() != Some(conversation_id)
        {
            return Err(ApiError::conflict(format!(
                "The {delivery_label} was sent, but the event binding changed before delivery could be reconciled."
            )));
        }
    }
    fetch_event(db, event_id)
}

pub(super) async fn send_event_file(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
    multipart: Multipart,
) -> Result<Json<Value>, ApiError> {
    let input = parse_outbound_file(multipart, state.max_voice_bytes, false).await?;
    let conversation_id = input
        .conversation_id
        .as_deref()
        .ok_or_else(|| ApiError::bad("conversationId is required."))?;
    validate_conversation_id(conversation_id)?;

    let file_name = input
        .explicit_file_name
        .as_deref()
        .or(input.part_file_name.as_deref())
        .ok_or_else(|| ApiError::bad("The file must have a fileName."))?;
    validate_file_name(file_name)?;

    let mime_type = input
        .mime_type
        .as_deref()
        .unwrap_or("application/octet-stream");
    validate_mime_type(mime_type)?;

    let caption = input.caption.as_deref().and_then(nonempty_verbatim);
    if caption.is_some_and(|value| value.encode_utf16().count() > TELEGRAM_CAPTION_LIMIT) {
        return Err(ApiError::bad(
            "The Telegram file caption exceeds 1024 UTF-16 code units.",
        ));
    }

    let bytes = input
        .bytes
        .ok_or_else(|| ApiError::bad("A nonempty file part is required."))?;
    if bytes.is_empty() {
        return Err(ApiError::bad("A nonempty file part is required."));
    }

    let event = {
        let db = state.db.lock().map_err(ApiError::internal)?;
        outbound_event(&db, &event_id, conversation_id)?
    };

    let bot = state.bot.as_ref().ok_or_else(ApiError::unavailable)?;
    let mut request = bot.send_document(
        ChatId(event.chat_id),
        InputFile::memory(bytes.clone()).file_name(file_name.to_owned()),
    );
    if let Some(caption) = caption {
        request = request.caption(caption.to_owned());
    }
    if event.session_kind == "group"
        && let Ok(message_id) = i32::try_from(event.message_id)
    {
        request = request.reply_parameters(
            ReplyParameters::new(teloxide::types::MessageId(message_id))
                .allow_sending_without_reply(),
        );
    }
    let sent = telegram_requests::retry_request("send_document", || request.clone().send())
        .await
        .map_err(|error| {
            tracing::warn!(
                event_id = %event_id,
                error_class = telegram_requests::request_error_class(&error),
                "Telegram file send failed"
            );
            ApiError::new(
                StatusCode::BAD_GATEWAY,
                "telegram_send_failed",
                "Telegram did not accept the file.",
            )
        })?;

    let db = state.db.lock().map_err(ApiError::internal)?;
    if event.session_kind == "group" {
        let archive_text = caption
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("[File: {file_name}]"));
        db.execute(
            "INSERT INTO telegram_group_messages(
                 chat_id,message_id,update_id,display_name,text,reply_to_message_id,
                 sent_by_kennedy,created_at,kind,media_bytes,mime_type,file_name,
                 source_conversation_id,group_id
             ) VALUES(?1,?2,0,'Kennedy',?3,?4,1,?5,'document',?6,?7,?8,?9,?10)
             ON CONFLICT(chat_id,message_id) DO NOTHING",
            params![
                event.chat_id,
                i64::from(sent.id.0),
                archive_text,
                event.message_id,
                sent.date.to_rfc3339(),
                bytes,
                mime_type,
                file_name,
                conversation_id,
                event.group_id
            ],
        )
        .map_err(ApiError::internal)?;
        if let Some(group_id) = event.group_id.as_deref() {
            queue_stale_group_session_resets(&db, event.chat_id, group_id, i64::from(sent.id.0))
                .map_err(ApiError::internal)?;
        }
    }

    let reconciled_event =
        reconcile_outbound_event(&db, &event_id, conversation_id, input.complete, "file")?;

    Ok(Json(json!({
        "event":reconciled_event,
        "fileName":file_name,
        "mimeType":mime_type,
        "telegramMessageId":i64::from(sent.id.0),
        "complete":input.complete,
    })))
}

pub(super) async fn send_event_media(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
    multipart: Multipart,
) -> Result<Json<Value>, ApiError> {
    let input = parse_outbound_file(multipart, state.max_voice_bytes, true).await?;
    let conversation_id = input
        .conversation_id
        .as_deref()
        .ok_or_else(|| ApiError::bad("conversationId is required."))?;
    validate_conversation_id(conversation_id)?;
    let kind_text = input
        .kind
        .as_deref()
        .ok_or_else(|| ApiError::bad("kind is required."))?;
    let kind = native_media::NativeMediaKind::parse(kind_text).ok_or_else(|| {
        ApiError::bad("kind must be photo, video, animation, audio, video_note, or sticker.")
    })?;
    if input.caption.is_some() && !kind.accepts_caption() {
        return Err(ApiError::bad(format!(
            "caption is not accepted for {} media.",
            kind.as_str()
        )));
    }
    let caption = input.caption.as_deref().and_then(nonempty_verbatim);
    if caption.is_some_and(|value| value.encode_utf16().count() > TELEGRAM_CAPTION_LIMIT) {
        return Err(ApiError::bad(
            "The Telegram media caption exceeds 1024 UTF-16 code units.",
        ));
    }
    let bytes = input
        .bytes
        .ok_or_else(|| ApiError::bad("A nonempty file part is required."))?;
    if bytes.is_empty() {
        return Err(ApiError::bad("A nonempty file part is required."));
    }

    let event = {
        let db = state.db.lock().map_err(ApiError::internal)?;
        outbound_event(&db, &event_id, conversation_id)?
    };

    let supplied_file_name = input
        .explicit_file_name
        .as_deref()
        .or(input.part_file_name.as_deref());
    if let Some(file_name) = supplied_file_name {
        validate_file_name(file_name)?;
    }
    let mime_type = input
        .mime_type
        .as_deref()
        .unwrap_or_else(|| kind.fallback_mime(supplied_file_name));
    validate_mime_type(mime_type)?;
    let file_name = supplied_file_name
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| kind.default_file_name(event.message_id, mime_type));
    validate_file_name(&file_name)?;

    let reply_parameters = if event.session_kind == "group" {
        i32::try_from(event.message_id).ok().map(|message_id| {
            ReplyParameters::new(teloxide::types::MessageId(message_id))
                .allow_sending_without_reply()
        })
    } else {
        None
    };
    let bot = state.bot.as_ref().ok_or_else(ApiError::unavailable)?;
    let sent = native_media::send_native_media(
        bot,
        event.chat_id,
        kind,
        &bytes,
        &file_name,
        caption,
        reply_parameters,
    )
    .await
    .map_err(|error| {
        tracing::warn!(
            event_id = %event_id,
            media_kind = kind.as_str(),
            error_class = telegram_requests::request_error_class(&error),
            "Telegram native-media send failed"
        );
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            "telegram_send_failed",
            "Telegram did not accept the native media.",
        )
    })?;

    let db = state.db.lock().map_err(ApiError::internal)?;
    if event.session_kind == "group" {
        let duration_seconds = native_media::message_duration(&sent, kind);
        db.execute(
            "INSERT INTO telegram_group_messages(
                 chat_id,message_id,update_id,display_name,text,reply_to_message_id,
                 sent_by_kennedy,created_at,kind,media_bytes,mime_type,file_name,
                 duration_seconds,source_conversation_id,group_id
             ) VALUES(?1,?2,0,'Kennedy',?3,?4,1,?5,?6,?7,?8,?9,?10,?11,?12)
             ON CONFLICT(chat_id,message_id) DO NOTHING",
            params![
                event.chat_id,
                i64::from(sent.id.0),
                caption.unwrap_or(""),
                event.message_id,
                sent.date.to_rfc3339(),
                kind.as_str(),
                bytes,
                mime_type,
                file_name,
                duration_seconds,
                conversation_id,
                event.group_id
            ],
        )
        .map_err(ApiError::internal)?;
        if let Some(group_id) = event.group_id.as_deref() {
            queue_stale_group_session_resets(&db, event.chat_id, group_id, i64::from(sent.id.0))
                .map_err(ApiError::internal)?;
        }
    }

    let reconciled_event = reconcile_outbound_event(
        &db,
        &event_id,
        conversation_id,
        input.complete,
        "native media",
    )?;

    Ok(Json(json!({
        "event":reconciled_event,
        "kind":kind.as_str(),
        "fileName":file_name,
        "mimeType":mime_type,
        "telegramMessageId":i64::from(sent.id.0),
        "complete":input.complete,
    })))
}

#[cfg(test)]
mod tests {
    use axum::extract::FromRequest;

    use super::*;

    type MultipartPart<'a> = (&'a str, Option<&'a str>, Option<&'a str>, &'a [u8]);

    #[derive(Default)]
    struct ExtensionIdentitySink;

    impl IdentitySink for ExtensionIdentitySink {
        fn observe_identity(&self, _observation: &IdentityObservation) -> anyhow::Result<()> {
            Ok(())
        }

        fn whitelist(&self) -> anyhow::Result<WhitelistSnapshot> {
            Ok(WhitelistSnapshot::default())
        }

        fn request_add_user(
            &self,
            _requested_by_telegram_user_id: i64,
            _handle: &str,
        ) -> anyhow::Result<AddUserOutcome> {
            Ok(AddUserOutcome::Forbidden)
        }

        fn observe_group(&self, _group_id: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn extension_state(database: Connection) -> AppState {
        AppState {
            db: Arc::new(Mutex::new(database)),
            identity_sink: Arc::new(ExtensionIdentitySink),
            bot: None,
            max_voice_bytes: 1024,
            bot_user_id: None,
            bot_username: None,
        }
    }

    fn group_database() -> (Connection, String) {
        let database = Connection::open_in_memory().unwrap();
        database.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        apply_migrations(&database).unwrap();
        let group = ensure_group(&database, -100, "Friends").unwrap();
        let now = Utc::now().to_rfc3339();
        database
            .execute(
                "INSERT INTO telegram_group_messages(
                     chat_id,message_id,update_id,display_name,text,created_at,kind,group_id
                 ) VALUES(-100,1,1,'Participant','hello',?1,'text',?2)",
                params![now, group.group_id],
            )
            .unwrap();
        (database, group.group_id)
    }

    async fn multipart(parts: &[MultipartPart<'_>]) -> Multipart {
        let boundary = "kennedy-test-boundary";
        let mut body = Vec::new();
        for (name, file_name, content_type, bytes) in parts {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            body.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"{name}\"").as_bytes(),
            );
            if let Some(file_name) = file_name {
                body.extend_from_slice(format!("; filename=\"{file_name}\"").as_bytes());
            }
            body.extend_from_slice(b"\r\n");
            if let Some(content_type) = content_type {
                body.extend_from_slice(format!("Content-Type: {content_type}\r\n").as_bytes());
            }
            body.extend_from_slice(b"\r\n");
            body.extend_from_slice(bytes);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        let request = axum::http::Request::builder()
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap();
        Multipart::from_request(request, &()).await.unwrap()
    }

    #[test]
    fn relay_api_accepts_only_literal_loopback_addresses() {
        assert_eq!(
            loopback_bind("127.0.0.1:4324").unwrap(),
            "127.0.0.1:4324".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            loopback_bind("[::1]:4324").unwrap(),
            "[::1]:4324".parse::<SocketAddr>().unwrap()
        );
        for unsafe_bind in [
            "0.0.0.0:4324",
            "[::]:4324",
            "192.168.1.4:4324",
            "8.8.8.8:4324",
            "localhost:4324",
        ] {
            assert!(loopback_bind(unsafe_bind).is_err(), "{unsafe_bind}");
        }
    }

    #[test]
    fn browser_requests_require_one_exact_allowed_origin() {
        let allowed = vec![HeaderValue::from_static("http://127.0.0.1:4321")];

        let mut headers = HeaderMap::new();
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:4321"),
        );
        assert!(browser_origin_allowed(&headers, &allowed).is_ok());

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://attacker.example"),
        );
        assert!(browser_origin_allowed(&headers, &allowed).is_err());

        let mut browser_without_origin = HeaderMap::new();
        browser_without_origin.insert(
            HeaderName::from_static("sec-fetch-site"),
            HeaderValue::from_static("cross-site"),
        );
        assert!(browser_origin_allowed(&browser_without_origin, &allowed).is_err());

        assert!(browser_origin_allowed(&HeaderMap::new(), &allowed).is_ok());
    }

    #[tokio::test]
    async fn matching_detach_clears_only_the_expected_group_user_pointer() {
        let (database, group_id) = group_database();
        let expected = "019f5ca7-020f-7b63-be2f-82785fb68c03";
        let other = "119f5ca7-020f-7b63-be2f-82785fb68c04";
        let now = Utc::now().to_rfc3339();
        for (user_id, conversation_id) in [(42, expected), (77, other)] {
            database
                .execute(
                    "INSERT INTO telegram_group_sessions(
                         group_id,telegram_user_id,current_conversation_id,updated_at,
                         last_context_message_id,last_invocation_message_id
                     ) VALUES(?1,?2,?3,?4,0,0)",
                    params![group_id, user_id, conversation_id, now],
                )
                .unwrap();
        }
        let state = extension_state(database);

        let before = list_group_session_updates(State(state.clone()))
            .await
            .unwrap();
        assert_eq!(before.0["updates"].as_array().unwrap().len(), 2);

        let _ = detach_group_session(
            State(state.clone()),
            Path(expected.to_owned()),
            Json(DetachGroupSession {
                group_id: group_id.clone(),
                telegram_user_id: 42,
            }),
        )
        .await
        .unwrap();

        {
            let database = state.db.lock().unwrap();
            assert_eq!(
                database
                    .query_row(
                        "SELECT current_conversation_id FROM telegram_group_sessions
                         WHERE group_id=?1 AND telegram_user_id=42",
                        [&group_id],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    .unwrap(),
                None
            );
            assert_eq!(
                database
                    .query_row(
                        "SELECT current_conversation_id FROM telegram_group_sessions
                         WHERE group_id=?1 AND telegram_user_id=77",
                        [&group_id],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    .unwrap()
                    .as_deref(),
                Some(other)
            );
        }

        let after = list_group_session_updates(State(state)).await.unwrap();
        let conversations = after.0["updates"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|update| update["conversationId"].as_str())
            .collect::<Vec<_>>();
        assert!(!conversations.contains(&expected));
        assert!(conversations.contains(&other));
    }

    #[tokio::test]
    async fn stale_detach_cannot_clear_a_rebound_group_session() {
        let (database, group_id) = group_database();
        let stale = "019f5ca7-020f-7b63-be2f-82785fb68c03";
        let current = "219f5ca7-020f-7b63-be2f-82785fb68c05";
        database
            .execute(
                "INSERT INTO telegram_group_sessions(
                     group_id,telegram_user_id,current_conversation_id,updated_at,
                     last_context_message_id,last_invocation_message_id
                 ) VALUES(?1,42,?2,?3,0,0)",
                params![group_id, current, Utc::now().to_rfc3339()],
            )
            .unwrap();
        let state = extension_state(database);

        let error = detach_group_session(
            State(state.clone()),
            Path(stale.to_owned()),
            Json(DetachGroupSession {
                group_id: group_id.clone(),
                telegram_user_id: 42,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT);

        assert_eq!(
            state
                .db
                .lock()
                .unwrap()
                .query_row(
                    "SELECT current_conversation_id FROM telegram_group_sessions
                     WHERE group_id=?1 AND telegram_user_id=42",
                    [&group_id],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            current
        );
    }

    #[test]
    fn outbound_file_names_are_bounded_and_path_free() {
        assert!(validate_file_name("report.pdf").is_ok());
        assert!(validate_file_name("../secret").is_err());
        assert!(validate_file_name("folder\\secret").is_err());
        assert!(validate_file_name("").is_err());
        assert!(validate_file_name(&"a".repeat(256)).is_err());
    }

    #[tokio::test]
    async fn native_media_multipart_rejects_unknown_duplicate_and_inapplicable_fields() {
        let duplicate = multipart(&[
            ("kind", None, None, b"photo"),
            ("kind", None, None, b"video"),
        ])
        .await;
        assert!(
            parse_outbound_file(duplicate, 1024, true)
                .await
                .unwrap_err()
                .message
                .contains("only once")
        );

        let unknown = multipart(&[("width", None, None, b"10")]).await;
        assert!(
            parse_outbound_file(unknown, 1024, true)
                .await
                .unwrap_err()
                .message
                .contains("Unknown multipart field")
        );

        let state = extension_state(group_database().0);
        let inapplicable = multipart(&[
            (
                "conversationId",
                None,
                None,
                b"019f5ca7-020f-7b63-be2f-82785fb68c03",
            ),
            ("kind", None, None, b"video_note"),
            ("caption", None, None, b"not allowed"),
            ("file", Some("note.mp4"), Some("video/mp4"), b"media"),
        ])
        .await;
        let error = send_event_media(State(state), Path("event".into()), inapplicable)
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_request");
        assert!(error.message.contains("caption is not accepted"));
    }

    #[tokio::test]
    async fn native_group_send_archives_exact_media_and_completes_after_telegram_success() {
        #[derive(Clone, Default)]
        struct Capture(Arc<Mutex<Vec<(String, String)>>>);

        async fn accept(
            State(capture): State<Capture>,
            uri: axum::http::Uri,
            body: axum::body::Bytes,
        ) -> Json<Value> {
            capture.0.lock().unwrap().push((
                uri.path().to_ascii_lowercase(),
                String::from_utf8_lossy(&body).into(),
            ));
            Json(json!({
                "ok":true,
                "result":{
                    "message_id":900,
                    "date":1629404938,
                    "from":{
                        "id":999,
                        "is_bot":true,
                        "first_name":"Kennedy",
                        "username":"KennedyBot"
                    },
                    "chat":{"id":-100,"title":"Friends","type":"supergroup"},
                    "photo":[
                        {
                            "file_id":"sent",
                            "file_unique_id":"sent-unique",
                            "width":100,
                            "height":100,
                            "file_size":5
                        }
                    ],
                    "caption":"exact caption"
                }
            }))
        }

        let capture = Capture::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_capture = capture.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .fallback(axum::routing::post(accept))
                    .with_state(server_capture),
            )
            .await
            .unwrap();
        });

        let (database, group_id) = group_database();
        let conversation_id = "019f5ca7-020f-7b63-be2f-82785fb68c03";
        database
            .execute(
                "INSERT INTO telegram_events(
                     id,update_id,message_id,telegram_user_id,chat_id,display_name,
                     kind,text,status,conversation_id,created_at,session_kind,group_id
                 ) VALUES(
                     'event',10,7,42,-100,'David','text','invoke','processing',
                     ?1,?2,'group',?3
                 )",
                params![conversation_id, Utc::now().to_rfc3339(), group_id],
            )
            .unwrap();
        let mut state = extension_state(database);
        state.bot =
            Some(Bot::new("test-token").set_api_url(format!("http://{address}").parse().unwrap()));
        let request = multipart(&[
            ("conversationId", None, None, conversation_id.as_bytes()),
            ("kind", None, None, b"photo"),
            ("caption", None, None, b"exact caption"),
            ("complete", None, None, b"true"),
            ("file", Some("photo.jpg"), Some("image/jpeg"), b"media"),
        ])
        .await;

        let response = send_event_media(State(state.clone()), Path("event".into()), request)
            .await
            .unwrap();
        assert_eq!(response.0["kind"], "photo");
        assert_eq!(response.0["complete"], true);
        assert_eq!(response.0["event"]["status"], "complete");
        let requests = capture.0.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].0.ends_with("/sendphoto"));
        assert!(requests[0].1.contains("\"message_id\":7"));
        assert!(
            requests[0]
                .1
                .contains("\"allow_sending_without_reply\":true")
        );
        drop(requests);

        let archived = state
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT kind,text,media_bytes,mime_type,file_name,
                        reply_to_message_id,source_conversation_id,group_id
                 FROM telegram_group_messages
                 WHERE chat_id=-100 AND message_id=900",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            archived,
            (
                "photo".into(),
                "exact caption".into(),
                b"media".to_vec(),
                "image/jpeg".into(),
                "photo.jpg".into(),
                7,
                conversation_id.into(),
                group_id,
            )
        );
        server.abort();
    }
}
