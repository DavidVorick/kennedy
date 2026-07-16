# Audio Ingress Specification

The audio-ingress service is Kennedy's durable loopback pipeline for vnote WAV
recordings. It does not access the Kmap, conversation database, or intelligence
backend. Its persistent state is `kennedy-audio.sqlite3` plus the private
`kennedy-audio-ingress/` media tree.

## Upload and identity

`POST /api/v1/audio-ingress` accepts multipart fields `recorded_at` and `file`.
`recorded_at` is required RFC 3339 with an offset and means the instant recording
began. The service streams the file to private temporary storage, enforces the
configured byte limit, computes SHA-256 while writing, syncs it, publishes a
content-addressed original, inserts the job, and only then returns. New uploads
return `202`; an already-known hash returns its existing record with
`deduplicated: true`. Files are never identified by their mutable path or name.

`GET /api/v1/audio-ingress/by-sha256/{sha256}` provides the historical-import
membership test. The list and individual-record endpoints expose processing,
piece, retry, and completion status without returning the large final transcript
or audio bytes.

## Durable preparation

The worker processes the oldest eligible record through these stored states:

```text
uploaded -> chunking -> transcribing -> reconciling -> ready_for_ingress
                                                        |
                                                        v
                                                    ingressing -> complete
                                                        |
                                                        v
                                                   ingress_failed
```

Provider or transport failure retains the current stage, concise error, attempt
count, and next-attempt time. Delay grows exponentially to one hour. A killed
process can repeat the current idempotent stage but cannot lose an accepted
original, completed chunk transcript, final transcript, or ingress checkpoint.

Only WAV input is processed. Windows are equalized for the whole recording,
are at most 240 seconds, and overlap their neighbors by 15 seconds. Each window
is a durable WAV under `chunks/{recording_id}/` and has an ordered database row
with recording-relative bounds. Gemini Files API media is deleted after each
interaction; the local original and results remain.

`gemini-3.1-pro-preview` receives up to four independent windows concurrently
and returns structured utterances with relative timestamps, chunk-local
speakers, language, original text, English translation, annotations, and
confidence. Each successful result is committed immediately; a retry sends
only unfinished windows. `gpt-5.6-sol` with `xhigh` reasoning receives all
resulting JSON in database order. It produces the canonical complete Markdown
transcript, removes the known overlap, and reconciles speaker labels without
inventing unsupported real identities.

The estimate is `ceil(Unicode characters / 4)`. Sol inserts
`<!-- KENNEDY_INGRESS_BREAK -->` only at sensible boundaries when a transcript
needs more than one piece. The service strips markers and refuses to publish
any empty or greater-than-50,000-token piece. It asks Sol for a second
copy-only boundary pass if the initial result exceeds that contract.

## Kennedy ingress queue

Every piece stores its text, index, total-piece relationship, estimated tokens,
optimistic version, provenance ID, complete history-ingress archive, and its
concise failure history. The queue returns an in-progress piece first, then the
oldest recording and lowest piece index. At most one audio piece can be in
progress in this database. A failed Kennedy turn is retried after durable
delays of one, five, fifteen, and sixty minutes instead of consuming all five
attempts during a short provider outage. The fifth consecutive failure remains
terminal, but the UI can explicitly requeue the preserved piece; doing so keeps
the old diagnostics while resetting the consecutive-failure counter.

The frontend creates provenance with source `audio-vnote`, the piece-specific
idempotency key `audio:{sha256}:piece:{index}`, and `source_created_at` equal to
recording start. It runs the normal mutation tool loop with the additional
audio-ingress prompt policy. The frontend uses one Web Lock for conversation
and audio ingress, providing global browser-side Kmap mutation serialization.
Completing the final piece atomically marks the recording complete.

## API summary

- `GET /health`
- `POST /api/v1/audio-ingress`
- `GET /api/v1/audio-ingress?limit=N`
- `GET /api/v1/audio-ingress/{recording_id}`
- `GET /api/v1/audio-ingress/by-sha256/{sha256}`
- `GET /api/v1/audio-ingress/ingress/next`
- `GET /api/v1/audio-ingress/pieces/{piece_id}`
- `POST /api/v1/audio-ingress/pieces/{piece_id}/ingress-started`
- `PUT /api/v1/audio-ingress/pieces/{piece_id}/ingress-checkpoint`
- `POST /api/v1/audio-ingress/pieces/{piece_id}/ingress-completed`
- `POST /api/v1/audio-ingress/pieces/{piece_id}/ingress-failure`
- `POST /api/v1/audio-ingress/pieces/{piece_id}/retry-ingress`

Terminal retry preserves the transcript, provenance, and diagnostic log. The
caller may replace the opaque frontend state so an exhausted model checkpoint
can be discarded before the piece returns to the durable ingress queue.
