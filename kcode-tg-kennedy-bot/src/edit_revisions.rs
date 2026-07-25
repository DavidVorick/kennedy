use super::*;

const EDIT_REVISIONS_MIGRATION: &str = include_str!("../migrations/005_edit_revisions.sql");

pub(super) fn migrate(db: &Connection) -> anyhow::Result<()> {
    let event_columns = db
        .prepare("PRAGMA table_info(telegram_events)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !event_columns
        .iter()
        .any(|name| name == "revision_update_id")
    {
        db.execute_batch("ALTER TABLE telegram_events ADD COLUMN revision_update_id INTEGER;")?;
    }
    db.execute(
        "UPDATE telegram_events SET revision_update_id=update_id
         WHERE revision_update_id IS NULL",
        [],
    )?;

    let ingress_columns = db
        .prepare("PRAGMA table_info(telegram_group_ingress)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !ingress_columns
        .iter()
        .any(|name| name == "completion_reason")
    {
        db.execute_batch("ALTER TABLE telegram_group_ingress ADD COLUMN completion_reason TEXT;")?;
    }
    db.execute_batch(EDIT_REVISIONS_MIGRATION)?;
    Ok(())
}

fn source_event_for_message(
    db: &Connection,
    chat_id: i64,
    message_id: i64,
    session_kind: &str,
) -> anyhow::Result<Option<RelayEvent>> {
    Ok(db
        .query_row(
            "SELECT id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,mime_type,file_name,duration_seconds,status,conversation_id,transcription,transcription_model,created_at,session_kind,group_context_json,group_id,processing_started_at,completion_reason
             FROM telegram_events
             WHERE chat_id=?1 AND message_id=?2 AND session_kind=?3
             ORDER BY update_id DESC LIMIT 1",
            params![chat_id, message_id, session_kind],
            row_event,
        )
        .optional()?)
}

#[allow(clippy::too_many_arguments)]
fn replace_event_revision(
    db: &Connection,
    event_id: &str,
    revision_update_id: i64,
    username: Option<&str>,
    display_name: &str,
    input: Option<&MessageInput>,
    fallback_text: Option<&str>,
) -> anyhow::Result<bool> {
    let kind = input.map(|value| value.kind).unwrap_or("text");
    let text = match input {
        Some(value) => value.text.as_deref(),
        None => fallback_text,
    };
    let changed = db.execute(
        "UPDATE telegram_events SET
             revision_update_id=?1,username=?2,display_name=?3,kind=?4,text=?5,
             voice_bytes=?6,mime_type=?7,file_name=?8,duration_seconds=?9,
             transcription=NULL,transcription_model=NULL
         WHERE id=?10 AND ?1>COALESCE(revision_update_id,update_id)",
        params![
            revision_update_id,
            username,
            display_name,
            kind,
            text,
            input.and_then(|value| value.media_bytes.as_deref()),
            input.and_then(|value| value.mime_type.as_deref()),
            input.and_then(|value| value.file_name.as_deref()),
            input.and_then(|value| value.duration_seconds),
            event_id,
        ],
    )?;
    Ok(changed == 1)
}

fn complete_edit_invalidated_event(
    db: &Connection,
    event: &RelayEvent,
    completed_at: &str,
    reason: &str,
) -> anyhow::Result<()> {
    db.execute(
        "UPDATE telegram_events SET status='complete',completed_at=?1,
             completion_reason=?2
         WHERE id=?3 AND status<>'complete'",
        params![completed_at, reason, event.id],
    )?;
    if event.status == "processing" {
        clear_matching_session_binding(db, event, completed_at)?;
    }
    Ok(())
}

fn complete_source_edited_event(
    db: &Connection,
    event: &RelayEvent,
    completed_at: &str,
) -> anyhow::Result<()> {
    complete_edit_invalidated_event(db, event, completed_at, "source_edited")
}

pub(super) async fn process_private_message_edit(
    bot: &Bot,
    state: &AppState,
    update_id: i64,
    message: Message,
) -> anyhow::Result<()> {
    let Some(user) = message.from.as_ref() else {
        return Ok(());
    };
    let telegram_user_id =
        i64::try_from(user.id.0).context("Telegram user ID exceeds SQLite range")?;
    let username = user.username.clone();
    let display_name = user.full_name();
    if !report_identity(
        state.identity_sink.as_ref(),
        telegram_user_id,
        username.as_deref(),
        &display_name,
    )? {
        return Ok(());
    }
    let chat_id = message.chat.id.0;
    let message_id = i64::from(message.id.0);
    let exists = {
        let db = state
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("locking Telegram database"))?;
        source_event_for_message(&db, chat_id, message_id, "private")?.is_some()
    };
    if !exists {
        return Ok(());
    }

    let input = parse_message_input_with_feedback(bot, state, &message, false).await?;
    let db = state
        .db
        .lock()
        .map_err(|_| anyhow::anyhow!("locking Telegram database"))?;
    let Some(event) = source_event_for_message(&db, chat_id, message_id, "private")? else {
        return Ok(());
    };
    if !replace_event_revision(
        &db,
        &event.id,
        update_id,
        username.as_deref(),
        &display_name,
        input.as_ref(),
        None,
    )? {
        return Ok(());
    }
    if event.status == "processing" || (event.status == "pending" && input.is_none()) {
        complete_source_edited_event(&db, &event, &Utc::now().to_rfc3339())?;
    }
    Ok(())
}

fn event_context_contains_message(event: &RelayEvent, message_id: i64) -> bool {
    event
        .group_context
        .as_ref()
        .and_then(|context| context.get("messages"))
        .and_then(Value::as_array)
        .is_some_and(|messages| {
            messages
                .iter()
                .any(|message| message.get("messageId").and_then(Value::as_i64) == Some(message_id))
        })
}

fn refresh_group_event_contexts(
    db: &Connection,
    chat_id: i64,
    edited_message_id: i64,
    group_id: &str,
    title: &str,
) -> anyhow::Result<()> {
    let events = {
        let mut statement = db.prepare(
            "SELECT id,message_id,telegram_user_id,chat_id,username,display_name,kind,text,mime_type,file_name,duration_seconds,status,conversation_id,transcription,transcription_model,created_at,session_kind,group_context_json,group_id,processing_started_at,completion_reason
             FROM telegram_events
             WHERE session_kind='group' AND chat_id=?1 AND message_id>=?2
               AND status IN ('pending','processing')
             ORDER BY message_id,update_id",
        )?;
        statement
            .query_map(params![chat_id, edited_message_id], row_event)?
            .collect::<Result<Vec<_>, _>>()?
    };
    if events.is_empty() {
        return Ok(());
    }
    let participants = group_participants(db, group_id)?;
    for event in events {
        if event.status == "processing" {
            if event_context_contains_message(&event, edited_message_id) {
                complete_edit_invalidated_event(
                    db,
                    &event,
                    &Utc::now().to_rfc3339(),
                    "context_edited",
                )?;
            }
            continue;
        }
        let messages = recent_group_messages(db, chat_id, event.message_id, 51)?;
        let context = json!({
            "groupTitle":title,
            "chatId":chat_id,
            "invokingTelegramUserId":event.telegram_user_id,
            "participants":participants,
            "messages":messages,
        });
        db.execute(
            "UPDATE telegram_events SET group_context_json=?1 WHERE id=?2 AND status='pending'",
            params![serde_json::to_string(&context)?, event.id],
        )?;
    }
    Ok(())
}

pub(super) fn refresh_group_ingress_snapshots(
    db: &Connection,
    chat_id: i64,
    edited_message_id: i64,
) -> anyhow::Result<()> {
    let batches = {
        let mut statement = db.prepare(
            "SELECT id,first_message_id,last_message_id,status
             FROM telegram_group_ingress
             WHERE chat_id=?1 AND first_message_id<=?2 AND last_message_id>=?2
               AND status IN ('pending','processing')",
        )?;
        statement
            .query_map(params![chat_id, edited_message_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (batch_id, first_message_id, last_message_id, status) in batches {
        if status == "processing" {
            db.execute(
                "UPDATE telegram_group_ingress
                 SET status='complete',completed_at=?1,completion_reason='context_edited'
                 WHERE id=?2 AND status='processing'",
                params![Utc::now().to_rfc3339(), batch_id],
            )?;
            continue;
        }
        let messages = {
            let mut statement = db.prepare(&format!(
                "SELECT {GROUP_MESSAGE_JSON_COLUMNS}
                 FROM telegram_group_messages
                 WHERE chat_id=?1 AND message_id>=?2 AND message_id<=?3
                   AND sent_by_kennedy=0
                 ORDER BY message_id"
            ))?;
            statement
                .query_map(
                    params![chat_id, first_message_id, last_message_id],
                    group_message_json,
                )?
                .collect::<Result<Vec<_>, _>>()?
        };
        db.execute(
            "UPDATE telegram_group_ingress SET messages_json=?1 WHERE id=?2 AND status='pending'",
            params![serde_json::to_string(&messages)?, batch_id],
        )?;
    }
    Ok(())
}

fn repair_group_invocation_cursors(
    db: &Connection,
    event: &RelayEvent,
    completed_at: &str,
) -> anyhow::Result<()> {
    let Some(group_id) = event.group_id.as_deref() else {
        return Ok(());
    };
    let latest_group: i64 = db.query_row(
        "SELECT COALESCE(MAX(message_id),0) FROM telegram_events
         WHERE session_kind='group' AND group_id=?1
           AND COALESCE(completion_reason,'')<>'source_edited'",
        [group_id],
        |row| row.get(0),
    )?;
    db.execute(
        "UPDATE telegram_groups SET last_invocation_message_id=NULLIF(?1,0),updated_at=?2
         WHERE group_id=?3",
        params![latest_group, completed_at, group_id],
    )?;
    let latest_user: i64 = db.query_row(
        "SELECT COALESCE(MAX(message_id),0) FROM telegram_events
         WHERE session_kind='group' AND group_id=?1 AND telegram_user_id=?2
           AND COALESCE(completion_reason,'')<>'source_edited'",
        params![group_id, event.telegram_user_id],
        |row| row.get(0),
    )?;
    db.execute(
        "UPDATE telegram_group_sessions SET last_invocation_message_id=?1,updated_at=?2
         WHERE group_id=?3 AND telegram_user_id=?4",
        params![latest_user, completed_at, group_id, event.telegram_user_id],
    )?;
    db.execute(
        "UPDATE telegram_group_messages SET source_conversation_id=NULL
         WHERE chat_id=?1 AND message_id=?2
           AND source_conversation_id=COALESCE(?3,source_conversation_id)",
        params![event.chat_id, event.message_id, event.conversation_id],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn reconcile_group_message_edit(
    db: &Connection,
    update_id: i64,
    message: &Message,
    author: &GroupMessageAuthor,
    input: Option<&MessageInput>,
    invoked: bool,
    archive_text: &str,
    title: &str,
    group_id: &str,
) -> anyhow::Result<()> {
    let chat_id = message.chat.id.0;
    let message_id = i64::from(message.id.0);
    if let Some(event) = source_event_for_message(db, chat_id, message_id, "group")? {
        if !replace_event_revision(
            db,
            &event.id,
            update_id,
            author.username.as_deref(),
            &author.display_name,
            input,
            Some(archive_text),
        )? {
            return Ok(());
        }
        let cancel = event.status == "processing"
            || (event.status == "pending" && (!invoked || input.is_none()));
        if cancel {
            let now = Utc::now().to_rfc3339();
            complete_source_edited_event(db, &event, &now)?;
            repair_group_invocation_cursors(db, &event, &now)?;
        }
    }
    refresh_group_event_contexts(db, chat_id, message_id, group_id, title)?;
    refresh_group_ingress_snapshots(db, chat_id, message_id)?;
    Ok(())
}

#[cfg(test)]
mod edit_regression_tests {
    use super::*;

    #[derive(Clone, Default)]
    struct TelegramCapture(Arc<Mutex<Vec<Value>>>);

    async fn accept_send_message(
        State(capture): State<TelegramCapture>,
        Json(request): Json<Value>,
    ) -> Json<Value> {
        capture.0.lock().unwrap().push(request.clone());
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
                "chat":{
                    "id":request["chat_id"],
                    "title":"Friends",
                    "type":"supergroup"
                },
                "text":request["text"]
            }
        }))
    }

    #[tokio::test]
    async fn group_reply_allows_delivery_after_the_source_message_disappears() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let capture = TelegramCapture::default();
        let observed = capture.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .fallback(post(accept_send_message))
                    .with_state(capture),
            )
            .await
            .unwrap();
        });
        let bot = Bot::new("test-token").set_api_url(format!("http://{address}").parse().unwrap());

        let sent = send_telegram_text(&bot, -100, "answer", Some(77))
            .await
            .unwrap();
        assert_eq!(sent.len(), 1);
        let requests = observed.0.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["reply_parameters"]["message_id"], 77);
        assert_eq!(
            requests[0]["reply_parameters"]["allow_sending_without_reply"],
            true
        );
        server.abort();
    }

    #[test]
    fn processing_source_edit_replaces_sensitive_media_and_detaches_the_session() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        apply_migrations(&db).unwrap();
        let conversation_id = "019f5ca7-020f-7b63-be2f-82785fb68c03";
        let now = Utc::now().to_rfc3339();
        db.execute(
            "INSERT INTO telegram_private_sessions(
                 telegram_user_id,chat_id,current_conversation_id,created_at,updated_at
             ) VALUES(42,42,?1,?2,?2)",
            params![conversation_id, now],
        )
        .unwrap();
        db.execute(
            "INSERT INTO telegram_events(
                 id,update_id,message_id,telegram_user_id,chat_id,username,display_name,
                 kind,voice_bytes,mime_type,file_name,status,conversation_id,
                 processing_started_at,created_at,session_kind
             ) VALUES(
                 'event',1,7,42,42,'taek42','David','document',X'010203',
                 'application/pdf','backup-codes.pdf','processing',?1,?2,?2,'private'
             )",
            params![conversation_id, now],
        )
        .unwrap();
        let event = source_event_for_message(&db, 42, 7, "private")
            .unwrap()
            .unwrap();
        let revision = MessageInput {
            kind: "text",
            text: Some("replacement".into()),
            media_bytes: None,
            mime_type: None,
            file_name: None,
            duration_seconds: None,
        };
        replace_event_revision(
            &db,
            &event.id,
            2,
            Some("taek42"),
            "David",
            Some(&revision),
            None,
        )
        .unwrap();
        complete_source_edited_event(&db, &event, &Utc::now().to_rfc3339()).unwrap();

        let revised = fetch_event(&db, "event").unwrap();
        assert_eq!(revised.status, "complete");
        assert_eq!(revised.completion_reason.as_deref(), Some("source_edited"));
        assert_eq!(revised.kind, "text");
        assert_eq!(revised.text.as_deref(), Some("replacement"));
        assert_eq!(revised.file_name, None);
        assert!(
            db.query_row(
                "SELECT voice_bytes IS NULL FROM telegram_events WHERE id='event'",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
        );
        assert_eq!(
            db.query_row(
                "SELECT current_conversation_id FROM telegram_private_sessions
                 WHERE telegram_user_id=42",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn editing_captured_group_context_cancels_a_stale_in_flight_response() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        apply_migrations(&db).unwrap();
        let group = ensure_group(&db, -100, "Friends").unwrap();
        upsert_group_member(&db, &group.group_id, 42, Some("taek42"), "David", "member").unwrap();
        let conversation_id = "019f5ca7-020f-7b63-be2f-82785fb68c03";
        let now = Utc::now().to_rfc3339();
        db.execute(
            "INSERT INTO telegram_group_sessions(
                 group_id,telegram_user_id,current_conversation_id,updated_at,
                 last_context_message_id,last_invocation_message_id
             ) VALUES(?1,42,?2,?3,2,2)",
            params![group.group_id, conversation_id, now],
        )
        .unwrap();
        for (message_id, text) in [(1, "old context"), (2, "invoke Kennedy")] {
            db.execute(
                "INSERT INTO telegram_group_messages(
                     chat_id,message_id,update_id,telegram_user_id,display_name,text,
                     created_at,kind,group_id
                 ) VALUES(-100,?1,?1,42,'David',?2,?3,'text',?4)",
                params![message_id, text, now, group.group_id],
            )
            .unwrap();
        }
        db.execute(
            "INSERT INTO telegram_events(
                 id,update_id,message_id,telegram_user_id,chat_id,display_name,
                 kind,text,status,conversation_id,processing_started_at,created_at,
                 session_kind,group_id,group_context_json
             ) VALUES(
                 'later-event',2,2,42,-100,'David','text','invoke Kennedy',
                 'processing',?1,?2,?2,'group',?3,?4
             )",
            params![
                conversation_id,
                now,
                group.group_id,
                serde_json::to_string(&json!({
                    "messages":[
                        {"messageId":1,"text":"old context"},
                        {"messageId":2,"text":"invoke Kennedy"}
                    ]
                }))
                .unwrap()
            ],
        )
        .unwrap();

        refresh_group_event_contexts(&db, -100, 1, &group.group_id, "Friends").unwrap();

        let event = fetch_event(&db, "later-event").unwrap();
        assert_eq!(event.status, "complete");
        assert_eq!(event.completion_reason.as_deref(), Some("context_edited"));
        assert_eq!(
            db.query_row(
                "SELECT current_conversation_id FROM telegram_group_sessions
                 WHERE group_id=?1 AND telegram_user_id=42",
                [&group.group_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .unwrap(),
            None
        );
    }
}

#[cfg(test)]
mod revision_regression_tests {
    use super::*;

    #[derive(Default)]
    struct RevisionIdentitySink;

    impl IdentitySink for RevisionIdentitySink {
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

    #[test]
    fn edits_preserve_queue_identity_order_and_ignore_older_revisions() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        apply_migrations(&db).unwrap();
        let now = Utc::now().to_rfc3339();
        for (id, update_id, message_id, text) in [
            ("first", 10_i64, 1_i64, "original"),
            ("second", 20_i64, 2_i64, "later work"),
        ] {
            db.execute(
                "INSERT INTO telegram_events(
                     id,update_id,message_id,telegram_user_id,chat_id,display_name,
                     kind,text,status,created_at,session_kind,revision_update_id
                 ) VALUES(?1,?2,?3,42,42,'David','text',?4,'pending',?5,'private',?2)",
                params![id, update_id, message_id, text, now],
            )
            .unwrap();
        }
        let revision = MessageInput {
            kind: "text",
            text: Some("newest".into()),
            media_bytes: None,
            mime_type: None,
            file_name: None,
            duration_seconds: None,
        };
        assert!(
            replace_event_revision(
                &db,
                "first",
                30,
                Some("taek42"),
                "David",
                Some(&revision),
                None,
            )
            .unwrap()
        );
        let older = MessageInput {
            kind: "text",
            text: Some("stale".into()),
            media_bytes: None,
            mime_type: None,
            file_name: None,
            duration_seconds: None,
        };
        assert!(
            !replace_event_revision(
                &db,
                "first",
                25,
                Some("taek42"),
                "David",
                Some(&older),
                None,
            )
            .unwrap()
        );

        assert_eq!(
            db.query_row(
                "SELECT update_id,revision_update_id,text FROM telegram_events WHERE id='first'",
                [],
                |row| Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                )),
            )
            .unwrap(),
            (10, 30, "newest".into())
        );
        let order = db
            .prepare("SELECT id FROM telegram_events WHERE status='pending' ORDER BY update_id")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(order, vec!["first", "second"]);
    }

    #[test]
    fn replayed_original_message_cannot_resurrect_private_work() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        apply_migrations(&db).unwrap();
        let now = Utc::now().to_rfc3339();
        db.execute(
            "INSERT INTO telegram_private_sessions(
                 telegram_user_id,chat_id,created_at,updated_at
             ) VALUES(42,42,?1,?1)",
            [&now],
        )
        .unwrap();
        let message: Message = serde_json::from_str(
            r#"{
                "message_id": 7,
                "date": 1629404938,
                "from": {"id": 42, "is_bot": false, "first_name": "David"},
                "chat": {"id": 42, "first_name": "David", "type": "private"},
                "text": "original"
            }"#,
        )
        .unwrap();
        insert_event(
            &db,
            10,
            &message,
            42,
            Some("taek42"),
            "David",
            "text",
            Some("original"),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let event_id: String = db
            .query_row("SELECT id FROM telegram_events", [], |row| row.get(0))
            .unwrap();
        let revision = MessageInput {
            kind: "text",
            text: Some("edited".into()),
            media_bytes: None,
            mime_type: None,
            file_name: None,
            duration_seconds: None,
        };
        assert!(
            replace_event_revision(
                &db,
                &event_id,
                30,
                Some("taek42"),
                "David",
                Some(&revision),
                None,
            )
            .unwrap()
        );
        insert_event(
            &db,
            11,
            &message,
            42,
            Some("taek42"),
            "David",
            "text",
            Some("original replay"),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM telegram_events", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
            1
        );
        assert_eq!(
            db.query_row("SELECT update_id FROM telegram_events", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
            10
        );
    }

    #[test]
    fn processing_background_ingress_is_invalidated_by_an_edit() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        apply_migrations(&db).unwrap();
        db.execute(
            "INSERT INTO telegram_group_ingress(
                 id,chat_id,first_message_id,last_message_id,messages_json,
                 participants_json,status,created_at
             ) VALUES('batch',-100,1,80,'[]','[]','processing',?1)",
            [Utc::now().to_rfc3339()],
        )
        .unwrap();

        refresh_group_ingress_snapshots(&db, -100, 42).unwrap();

        assert_eq!(
            db.query_row(
                "SELECT status,completion_reason FROM telegram_group_ingress WHERE id='batch'",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
            ("complete".into(), "context_edited".into())
        );
    }

    #[tokio::test]
    async fn listed_background_ingress_is_claimed_and_stale_completion_is_rejected() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        apply_migrations(&db).unwrap();
        let identities = Arc::new(RevisionIdentitySink);
        let group = ensure_group(&db, -100, "Friends").unwrap();
        db.execute(
            "INSERT INTO telegram_group_ingress(
                 id,chat_id,first_message_id,last_message_id,messages_json,
                 participants_json,status,created_at,group_id
             ) VALUES('batch',-100,1,80,'[]','[]','pending',?1,?2)",
            params![Utc::now().to_rfc3339(), group.group_id],
        )
        .unwrap();
        let state = AppState {
            db: Arc::new(Mutex::new(db)),
            identity_sink: identities,
            bot: None,
            max_voice_bytes: 1024,
            bot_user_id: None,
            bot_username: None,
        };

        let listed = list_group_ingress(State(state.clone())).await.unwrap();
        assert_eq!(listed.0["batches"][0]["id"], "batch");
        assert_eq!(
            state
                .db
                .lock()
                .unwrap()
                .query_row(
                    "SELECT status FROM telegram_group_ingress WHERE id='batch'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "processing"
        );

        refresh_group_ingress_snapshots(&state.db.lock().unwrap(), -100, 42).unwrap();
        let error = complete_group_ingress(State(state), Path("batch".into()))
            .await
            .unwrap_err();
        assert_eq!(error.code, "state_conflict");
    }
}
