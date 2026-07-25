use std::cmp::Reverse;

use teloxide::{
    Bot, RequestError,
    payloads::{
        SendAnimationSetters, SendAudioSetters, SendPhotoSetters, SendStickerSetters,
        SendVideoNoteSetters, SendVideoSetters,
    },
    prelude::Requester,
    requests::Request,
    types::{FileId, InputFile, Message, ReplyParameters, StickerFormat},
};

use crate::telegram_requests;

pub(super) const NATIVE_MEDIA_KINDS: [NativeMediaKind; 6] = [
    NativeMediaKind::Photo,
    NativeMediaKind::Video,
    NativeMediaKind::Animation,
    NativeMediaKind::Audio,
    NativeMediaKind::VideoNote,
    NativeMediaKind::Sticker,
];

pub(super) const INBOUND_MEDIA_KINDS: [&str; 8] = [
    "voice",
    "document",
    "photo",
    "video",
    "animation",
    "audio",
    "video_note",
    "sticker",
];

pub(super) const OUTBOUND_MEDIA_KINDS: [&str; 7] = [
    "document",
    "photo",
    "video",
    "animation",
    "audio",
    "video_note",
    "sticker",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeMediaKind {
    Photo,
    Video,
    Animation,
    Audio,
    VideoNote,
    Sticker,
}

impl NativeMediaKind {
    pub(super) fn parse(value: &str) -> Option<Self> {
        match value {
            "photo" => Some(Self::Photo),
            "video" => Some(Self::Video),
            "animation" => Some(Self::Animation),
            "audio" => Some(Self::Audio),
            "video_note" => Some(Self::VideoNote),
            "sticker" => Some(Self::Sticker),
            _ => None,
        }
    }

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Photo => "photo",
            Self::Video => "video",
            Self::Animation => "animation",
            Self::Audio => "audio",
            Self::VideoNote => "video_note",
            Self::Sticker => "sticker",
        }
    }

    pub(super) const fn accepts_caption(self) -> bool {
        matches!(
            self,
            Self::Photo | Self::Video | Self::Animation | Self::Audio
        )
    }

    pub(super) fn fallback_mime(self, file_name: Option<&str>) -> &'static str {
        match self {
            Self::Photo => "image/jpeg",
            Self::Video | Self::VideoNote => "video/mp4",
            Self::Sticker => sticker_mime_from_file_name(file_name),
            Self::Animation => file_name
                .and_then(mime_from_animation_extension)
                .unwrap_or("application/octet-stream"),
            Self::Audio => file_name
                .and_then(mime_from_audio_extension)
                .unwrap_or("application/octet-stream"),
        }
    }

    pub(super) fn default_file_name(self, message_id: i64, mime_type: &str) -> String {
        let extension = extension_for_mime(mime_type).unwrap_or(match self {
            Self::Photo => "jpg",
            Self::Video | Self::VideoNote => "mp4",
            Self::Sticker => "webp",
            Self::Animation | Self::Audio => "bin",
        });
        format!(
            "telegram-{}-{message_id}.{extension}",
            self.as_str().replace('_', "-")
        )
    }
}

#[derive(Clone, Debug)]
pub(super) struct InboundMedia {
    pub(super) kind: &'static str,
    pub(super) file_id: FileId,
    pub(super) declared_size: Option<u32>,
    pub(super) text: Option<String>,
    pub(super) mime_type: Option<String>,
    pub(super) file_name: Option<String>,
    pub(super) duration_seconds: Option<i64>,
    pub(super) label: &'static str,
}

pub(super) fn classify_message(message: &Message) -> Option<InboundMedia> {
    if let Some(voice) = message.voice() {
        return Some(InboundMedia {
            kind: "voice",
            file_id: voice.file.id.clone(),
            declared_size: declared_file_size(voice.file.size),
            text: None,
            mime_type: Some(
                voice
                    .mime_type
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "audio/ogg".into()),
            ),
            file_name: None,
            duration_seconds: Some(i64::from(voice.duration.seconds())),
            label: "voice note",
        });
    }

    // Telegram may also expose an animation through its compatibility document
    // representation, so native animation must be tested before document.
    if let Some(animation) = message.animation() {
        let provider_file_name = animation.file_name.clone();
        let mime_type = animation
            .mime_type
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| {
                provider_file_name
                    .as_deref()
                    .and_then(mime_from_animation_extension)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "application/octet-stream".into());
        let kind = NativeMediaKind::Animation;
        return Some(InboundMedia {
            kind: kind.as_str(),
            file_id: animation.file.id.clone(),
            declared_size: declared_file_size(animation.file.size),
            text: message.caption().map(ToOwned::to_owned),
            file_name: Some(
                provider_file_name
                    .unwrap_or_else(|| kind.default_file_name(i64::from(message.id.0), &mime_type)),
            ),
            mime_type: Some(mime_type),
            duration_seconds: Some(i64::from(animation.duration.seconds())),
            label: "animation",
        });
    }

    if let Some(photo) = message.photo().and_then(select_photo) {
        let kind = NativeMediaKind::Photo;
        let mime_type = kind.fallback_mime(None);
        return Some(InboundMedia {
            kind: kind.as_str(),
            file_id: photo.file.id.clone(),
            declared_size: declared_file_size(photo.file.size),
            text: message.caption().map(ToOwned::to_owned),
            mime_type: Some(mime_type.into()),
            file_name: Some(kind.default_file_name(i64::from(message.id.0), mime_type)),
            duration_seconds: None,
            label: "photo",
        });
    }

    if let Some(video) = message.video() {
        let kind = NativeMediaKind::Video;
        let mime_type = video
            .mime_type
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| kind.fallback_mime(video.file_name.as_deref()).into());
        return Some(InboundMedia {
            kind: kind.as_str(),
            file_id: video.file.id.clone(),
            declared_size: declared_file_size(video.file.size),
            text: message.caption().map(ToOwned::to_owned),
            file_name: Some(
                video
                    .file_name
                    .clone()
                    .unwrap_or_else(|| kind.default_file_name(i64::from(message.id.0), &mime_type)),
            ),
            mime_type: Some(mime_type),
            duration_seconds: Some(i64::from(video.duration.seconds())),
            label: "video",
        });
    }

    if let Some(audio) = message.audio() {
        let kind = NativeMediaKind::Audio;
        let provider_file_name = audio.file_name.clone();
        let mime_type = audio
            .mime_type
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| {
                provider_file_name
                    .as_deref()
                    .and_then(mime_from_audio_extension)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| "application/octet-stream".into());
        return Some(InboundMedia {
            kind: kind.as_str(),
            file_id: audio.file.id.clone(),
            declared_size: declared_file_size(audio.file.size),
            text: message.caption().map(ToOwned::to_owned),
            file_name: Some(
                provider_file_name
                    .unwrap_or_else(|| kind.default_file_name(i64::from(message.id.0), &mime_type)),
            ),
            mime_type: Some(mime_type),
            duration_seconds: Some(i64::from(audio.duration.seconds())),
            label: "audio",
        });
    }

    if let Some(video_note) = message.video_note() {
        let kind = NativeMediaKind::VideoNote;
        let mime_type = kind.fallback_mime(None);
        return Some(InboundMedia {
            kind: kind.as_str(),
            file_id: video_note.file.id.clone(),
            declared_size: declared_file_size(video_note.file.size),
            text: None,
            mime_type: Some(mime_type.into()),
            file_name: Some(kind.default_file_name(i64::from(message.id.0), mime_type)),
            duration_seconds: Some(i64::from(video_note.duration.seconds())),
            label: "video note",
        });
    }

    if let Some(sticker) = message.sticker() {
        let kind = NativeMediaKind::Sticker;
        let mime_type = match sticker.format() {
            StickerFormat::Static => "image/webp",
            StickerFormat::Animated => "application/x-tgsticker",
            StickerFormat::Video => "video/webm",
        };
        return Some(InboundMedia {
            kind: kind.as_str(),
            file_id: sticker.file.id.clone(),
            declared_size: declared_file_size(sticker.file.size),
            text: sticker.emoji.clone(),
            mime_type: Some(mime_type.into()),
            file_name: Some(kind.default_file_name(i64::from(message.id.0), mime_type)),
            duration_seconds: None,
            label: "sticker",
        });
    }

    if let Some(document) = message.document() {
        return Some(InboundMedia {
            kind: "document",
            file_id: document.file.id.clone(),
            declared_size: declared_file_size(document.file.size),
            text: message.caption().map(ToOwned::to_owned),
            mime_type: document.mime_type.as_ref().map(ToString::to_string),
            file_name: Some(
                document
                    .file_name
                    .clone()
                    .unwrap_or_else(|| "telegram-file".into()),
            ),
            duration_seconds: None,
            label: "file",
        });
    }

    None
}

fn select_photo(photos: &[teloxide::types::PhotoSize]) -> Option<&teloxide::types::PhotoSize> {
    photos
        .iter()
        .enumerate()
        .max_by_key(|(index, photo)| {
            let area = u64::from(photo.width).saturating_mul(u64::from(photo.height));
            (
                area,
                declared_file_size(photo.file.size).unwrap_or(0),
                Reverse(*index),
            )
        })
        .map(|(_, photo)| photo)
}

fn declared_file_size(size: u32) -> Option<u32> {
    (size != u32::MAX).then_some(size)
}

pub(super) fn is_retained_media_kind(kind: &str) -> bool {
    matches!(kind, "voice" | "document") || NativeMediaKind::parse(kind).is_some()
}

pub(super) fn is_audio_oriented(kind: &str) -> bool {
    matches!(kind, "voice" | "audio" | "video_note")
}

pub(super) fn message_duration(message: &Message, kind: NativeMediaKind) -> Option<i64> {
    match kind {
        NativeMediaKind::Video => message
            .video()
            .map(|media| i64::from(media.duration.seconds())),
        NativeMediaKind::Animation => message
            .animation()
            .map(|media| i64::from(media.duration.seconds())),
        NativeMediaKind::Audio => message
            .audio()
            .map(|media| i64::from(media.duration.seconds())),
        NativeMediaKind::VideoNote => message
            .video_note()
            .map(|media| i64::from(media.duration.seconds())),
        NativeMediaKind::Photo | NativeMediaKind::Sticker => None,
    }
}

pub(super) async fn send_native_media(
    bot: &Bot,
    chat_id: i64,
    kind: NativeMediaKind,
    bytes: &[u8],
    file_name: &str,
    caption: Option<&str>,
    reply_parameters: Option<ReplyParameters>,
) -> Result<Message, RequestError> {
    let input_file = InputFile::memory(bytes.to_vec()).file_name(file_name.to_owned());
    match kind {
        NativeMediaKind::Photo => {
            let mut request = bot.send_photo(teloxide::types::ChatId(chat_id), input_file);
            if let Some(caption) = caption {
                request = request.caption(caption.to_owned());
            }
            if let Some(reply_parameters) = reply_parameters {
                request = request.reply_parameters(reply_parameters);
            }
            telegram_requests::retry_request("send_photo", || request.clone().send()).await
        }
        NativeMediaKind::Video => {
            let mut request = bot.send_video(teloxide::types::ChatId(chat_id), input_file);
            if let Some(caption) = caption {
                request = request.caption(caption.to_owned());
            }
            if let Some(reply_parameters) = reply_parameters {
                request = request.reply_parameters(reply_parameters);
            }
            telegram_requests::retry_request("send_video", || request.clone().send()).await
        }
        NativeMediaKind::Animation => {
            let mut request = bot.send_animation(teloxide::types::ChatId(chat_id), input_file);
            if let Some(caption) = caption {
                request = request.caption(caption.to_owned());
            }
            if let Some(reply_parameters) = reply_parameters {
                request = request.reply_parameters(reply_parameters);
            }
            telegram_requests::retry_request("send_animation", || request.clone().send()).await
        }
        NativeMediaKind::Audio => {
            let mut request = bot.send_audio(teloxide::types::ChatId(chat_id), input_file);
            if let Some(caption) = caption {
                request = request.caption(caption.to_owned());
            }
            if let Some(reply_parameters) = reply_parameters {
                request = request.reply_parameters(reply_parameters);
            }
            telegram_requests::retry_request("send_audio", || request.clone().send()).await
        }
        NativeMediaKind::VideoNote => {
            let mut request = bot.send_video_note(teloxide::types::ChatId(chat_id), input_file);
            if let Some(reply_parameters) = reply_parameters {
                request = request.reply_parameters(reply_parameters);
            }
            telegram_requests::retry_request("send_video_note", || request.clone().send()).await
        }
        NativeMediaKind::Sticker => {
            let mut request = bot.send_sticker(teloxide::types::ChatId(chat_id), input_file);
            if let Some(reply_parameters) = reply_parameters {
                request = request.reply_parameters(reply_parameters);
            }
            telegram_requests::retry_request("send_sticker", || request.clone().send()).await
        }
    }
}

fn lower_extension(file_name: &str) -> Option<String> {
    let (_, extension) = file_name.rsplit_once('.')?;
    (!extension.is_empty()).then(|| extension.to_ascii_lowercase())
}

fn mime_from_animation_extension(file_name: &str) -> Option<&'static str> {
    match lower_extension(file_name)?.as_str() {
        "gif" => Some("image/gif"),
        "mp4" => Some("video/mp4"),
        _ => None,
    }
}

fn mime_from_audio_extension(file_name: &str) -> Option<&'static str> {
    match lower_extension(file_name)?.as_str() {
        "mp3" => Some("audio/mpeg"),
        "m4a" | "mp4" => Some("audio/mp4"),
        "ogg" | "oga" | "opus" => Some("audio/ogg"),
        "wav" => Some("audio/wav"),
        "flac" => Some("audio/flac"),
        _ => None,
    }
}

fn sticker_mime_from_file_name(file_name: Option<&str>) -> &'static str {
    match file_name.and_then(lower_extension).as_deref() {
        Some("tgs") => "application/x-tgsticker",
        Some("webm") => "video/webm",
        _ => "image/webp",
    }
}

fn extension_for_mime(mime_type: &str) -> Option<&'static str> {
    match mime_type.to_ascii_lowercase().as_str() {
        "image/jpeg" => Some("jpg"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        "video/mp4" => Some("mp4"),
        "video/webm" => Some("webm"),
        "audio/mpeg" => Some("mp3"),
        "audio/mp4" => Some("m4a"),
        "audio/ogg" => Some("ogg"),
        "audio/wav" => Some("wav"),
        "audio/flac" => Some("flac"),
        "application/x-tgsticker" => Some("tgs"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{Json, Router, extract::State, routing::post};
    use serde_json::Value;

    use super::*;

    fn message(value: serde_json::Value) -> Message {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn native_kind_contract_is_cohesive() {
        assert_eq!(
            NATIVE_MEDIA_KINDS.map(NativeMediaKind::as_str),
            [
                "photo",
                "video",
                "animation",
                "audio",
                "video_note",
                "sticker"
            ]
        );
        assert!(NativeMediaKind::Photo.accepts_caption());
        assert!(!NativeMediaKind::Sticker.accepts_caption());
        assert!(is_audio_oriented("audio"));
        assert!(!is_audio_oriented("video"));
    }

    #[test]
    fn largest_photo_uses_area_then_size_then_stable_order() {
        let message = message(serde_json::json!({
            "message_id":123,
            "date":1629404938,
            "chat":{"id":42,"type":"private"},
            "photo":[
                {"file_id":"first","file_unique_id":"a","width":100,"height":100,"file_size":20},
                {"file_id":"large-small","file_unique_id":"b","width":200,"height":100,"file_size":10},
                {"file_id":"large-big","file_unique_id":"c","width":100,"height":200,"file_size":30},
                {"file_id":"large-big-later","file_unique_id":"d","width":100,"height":200,"file_size":30},
                {"file_id":"large-missing-size","file_unique_id":"e","width":100,"height":200}
            ],
            "caption":"exact caption"
        }));
        let media = classify_message(&message).unwrap();
        assert_eq!(media.kind, "photo");
        assert_eq!(media.file_id.0, "large-big");
        assert_eq!(media.text.as_deref(), Some("exact caption"));
        assert_eq!(media.file_name.as_deref(), Some("telegram-photo-123.jpg"));
    }

    #[test]
    fn animation_precedes_compatibility_document_and_uncertain_mime_is_honest() {
        let animation = serde_json::json!({
            "file_id":"animation",
            "file_unique_id":"a",
            "width":320,
            "height":240,
            "duration":3,
            "file_size":50,
            "mime_type":null
        });
        let compatibility_document = serde_json::json!({
            "file_id":"document",
            "file_unique_id":"d",
            "file_size":50
        });
        let direct_animation =
            serde_json::from_value::<teloxide::types::MediaAnimation>(serde_json::json!({
                "animation":animation.clone(),
                "document":compatibility_document.clone()
            }));
        assert!(
            direct_animation.is_ok(),
            "animation fixture must be valid: {direct_animation:?}"
        );
        let provider_kind: teloxide::types::MediaKind = serde_json::from_value(serde_json::json!({
            "animation":animation.clone(),
            "document":compatibility_document
        }))
        .unwrap();
        assert!(matches!(
            provider_kind,
            teloxide::types::MediaKind::Animation(_)
        ));

        let message = message(serde_json::json!({
            "message_id":5,
            "date":1629404938,
            "chat":{"id":42,"type":"private"},
            "animation":animation
        }));
        let media = classify_message(&message).unwrap();
        assert_eq!(media.kind, "animation");
        assert_eq!(media.mime_type.as_deref(), Some("application/octet-stream"));
        assert_eq!(media.file_name.as_deref(), Some("telegram-animation-5.bin"));
    }

    #[test]
    fn documents_keep_document_semantics_regardless_of_mime() {
        let message = message(serde_json::json!({
            "message_id":6,
            "date":1629404938,
            "chat":{"id":42,"type":"private"},
            "document":{
                "file_id":"document",
                "file_unique_id":"d",
                "file_size":50,
                "file_name":"picture.png",
                "mime_type":"image/png"
            }
        }));
        let media = classify_message(&message).unwrap();
        assert_eq!(media.kind, "document");
        assert_eq!(media.mime_type.as_deref(), Some("image/png"));
    }

    #[test]
    fn video_audio_and_video_note_keep_essential_metadata() {
        let cases = [
            (
                serde_json::json!({
                    "message_id":8,
                    "date":1629404938,
                    "chat":{"id":42,"type":"private"},
                    "video":{
                        "file_id":"video",
                        "file_unique_id":"v",
                        "file_size":51,
                        "width":640,
                        "height":480,
                        "duration":9,
                        "file_name":"clip.mp4",
                        "mime_type":"video/mp4"
                    },
                    "caption":"video caption"
                }),
                "video",
                Some("video caption"),
                Some("clip.mp4"),
                Some("video/mp4"),
                Some(9),
            ),
            (
                serde_json::json!({
                    "message_id":9,
                    "date":1629404938,
                    "chat":{"id":42,"type":"private"},
                    "audio":{
                        "file_id":"audio",
                        "file_unique_id":"a",
                        "file_size":52,
                        "duration":10,
                        "file_name":"track.ogg",
                        "mime_type":"audio/ogg"
                    },
                    "caption":"audio caption"
                }),
                "audio",
                Some("audio caption"),
                Some("track.ogg"),
                Some("audio/ogg"),
                Some(10),
            ),
            (
                serde_json::json!({
                    "message_id":10,
                    "date":1629404938,
                    "chat":{"id":42,"type":"private"},
                    "video_note":{
                        "file_id":"note",
                        "file_unique_id":"n",
                        "file_size":53,
                        "length":240,
                        "duration":11
                    }
                }),
                "video_note",
                None,
                Some("telegram-video-note-10.mp4"),
                Some("video/mp4"),
                Some(11),
            ),
        ];
        for (message_value, kind, text, file_name, mime_type, duration) in cases {
            let media = classify_message(&message(message_value)).unwrap();
            assert_eq!(media.kind, kind);
            assert_eq!(media.text.as_deref(), text);
            assert_eq!(media.file_name.as_deref(), file_name);
            assert_eq!(media.mime_type.as_deref(), mime_type);
            assert_eq!(media.duration_seconds, duration);
        }
    }

    #[test]
    fn sticker_fallbacks_follow_provider_format() {
        for (flags, expected_mime, expected_suffix) in [
            (
                serde_json::json!({"is_animated":false,"is_video":false}),
                "image/webp",
                ".webp",
            ),
            (
                serde_json::json!({"is_animated":true,"is_video":false}),
                "application/x-tgsticker",
                ".tgs",
            ),
            (
                serde_json::json!({"is_animated":false,"is_video":true}),
                "video/webm",
                ".webm",
            ),
        ] {
            let mut sticker = serde_json::json!({
                "file_id":"sticker",
                "file_unique_id":"s",
                "file_size":25,
                "width":512,
                "height":512,
                "type":"regular",
                "emoji":"🙂"
            });
            sticker
                .as_object_mut()
                .unwrap()
                .extend(flags.as_object().unwrap().clone());
            let message = message(serde_json::json!({
                "message_id":7,
                "date":1629404938,
                "chat":{"id":42,"type":"private"},
                "sticker":sticker
            }));
            let media = classify_message(&message).unwrap();
            assert_eq!(media.mime_type.as_deref(), Some(expected_mime));
            assert!(media.file_name.unwrap().ends_with(expected_suffix));
            assert_eq!(media.text.as_deref(), Some("🙂"));
        }
    }

    #[tokio::test]
    async fn every_native_kind_uses_its_native_telegram_method() {
        async fn accept(
            State(paths): State<Arc<Mutex<Vec<String>>>>,
            uri: axum::http::Uri,
        ) -> Json<Value> {
            paths.lock().unwrap().push(uri.path().to_ascii_lowercase());
            Json(serde_json::json!({
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
                    "chat":{"id":42,"first_name":"David","type":"private"},
                    "text":"accepted"
                }
            }))
        }

        let paths = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .fallback(post(accept))
            .with_state(paths.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let bot = Bot::new("test-token").set_api_url(format!("http://{address}").parse().unwrap());

        for kind in NATIVE_MEDIA_KINDS {
            let caption = kind.accepts_caption().then_some("caption");
            send_native_media(
                &bot,
                42,
                kind,
                b"media",
                &kind.default_file_name(1, kind.fallback_mime(None)),
                caption,
                None,
            )
            .await
            .unwrap();
        }

        assert_eq!(
            *paths.lock().unwrap(),
            [
                "/bottest-token/sendphoto",
                "/bottest-token/sendvideo",
                "/bottest-token/sendanimation",
                "/bottest-token/sendaudio",
                "/bottest-token/sendvideonote",
                "/bottest-token/sendsticker",
            ]
        );
        server.abort();
    }
}
