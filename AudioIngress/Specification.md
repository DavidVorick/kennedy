# Audio Ingress Specification

The audio-ingress router is Kennedy's durable pipeline for vnote WAV recordings.
It does not access the Kmap, conversation database, or intelligence backend.
Its default persistent state is `data/kennedy-audio.sqlite3` plus the private
`data/audio-ingress-media/` tree. The native orchestrator calls its cloned
service handle directly; the HTTP routes remain for browser and upload clients.

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
or audio bytes. The list orders recordings by recording-start time from newest
to oldest; upload time and identifier are deterministic tie-breakers only.

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
process cannot lose an accepted original, completed final transcript, prepared
piece, or ingress checkpoint. It does lose the active external transcription
job and starts a new one from the original bytes, potentially repeating provider
work.
Structurally invalid or truncated WAV input is terminal instead of being
retried forever; its accepted original and diagnostic remain available.

Only WAV input is processed. The accepted original is archived byte-for-byte.
The service gives those bytes to an in-memory `kcode-audio-transcribe` job. The
external library owns validation, working audio, chunk planning, provider
calls, retries within that job, and canonical transcript reconciliation.
AudioIngress polls the job, persists its serialized status snapshot for browser
progress and diagnostics, and maps the reported steps to its durable
`chunking`, `transcribing`, and `reconciling` stages. It creates no working WAV
files or `audio_chunks` rows for new jobs. Those rows and the
`chunks/{recording_id}/` directory are retained only for older data; startup and
final completion remove old shard directories when present.

On library completion, AudioIngress stores the canonical transcript and splits
it at paragraph or line boundaries when necessary. The estimate is
`ceil(Unicode characters / 4)`, and no nonempty prepared piece may exceed
50,000 estimated tokens. It records the library's transcription and
reconciliation model attribution in the transcript header.

## Kennedy ingress queue

Every prepared piece is inserted into `audio_ingress_pieces` in the same
transaction that stores the final transcript. That table owns the immutable
payload, optimistic version, provenance ID, complete history-ingress
checkpoint, concise failure history, retry schedule, and the single audio
claim. It resumes an in-progress piece first, otherwise choosing the oldest
recording and lowest piece index. A failed Kennedy turn is eligible for
retry after a durable 15-second delay. Every nonterminal failure releases the
claim so other recording work can proceed. The fifth consecutive
failure remains terminal, but the UI can explicitly requeue the preserved
piece; doing so keeps the old diagnostics while resetting the
consecutive-failure counter. A provider input-size rejection is known to be
non-retryable for an unchanged checkpoint, so it becomes terminal immediately
instead of repeating the same oversized request five times.

The backend creates provenance with source `audio-vnote`, a stable
piece-specific idempotency key, and `source_created_at` equal to recording
start. It supplies the `end-session-v2` completion-protocol
identifier when claiming work, and the backend rejects claims from older
clients. It runs the normal mutation tool loop with the additional
audio-ingress prompt policy under the backend's serialized Kmap-writer gate.
Completing the final piece atomically marks the recording complete and removes
any legacy local WAV shards.
The completion endpoint independently requires the persisted Chatend snapshot
to show both a completed session and its final Kweb session-object ID.
Legacy checkpoints with a successful `EndSession` tool-log entry remain
accepted for migration compatibility. Historical
pieces identified as prematurely completed remain terminal with
`historyIngressRepairRequired: true` until the corrected backend worker invokes
repair release through the in-process adapter; release removes the old ingress
checkpoint, resets the
consecutive-attempt count, consumes a separate one-time release marker, and
returns their parent recordings to `ready_for_ingress`. If the repaired ingress
exhausts its new attempts, later frontend loads leave it terminal for explicit
retry.

## Browser-facing HTTP API

- `GET /api/v1/audio-ingress/health`
- `POST /api/v1/audio-ingress`
- `GET /api/v1/audio-ingress?limit=N`
- `GET /api/v1/audio-ingress/by-sha256/{sha256}`
- `GET /api/v1/audio-ingress/{recording_id}/history`
- `POST /api/v1/audio-ingress/pieces/{piece_id}/retry-ingress`

Piece reads, ingress transitions, and repair release use the same path-shaped
contract through the backend's in-process service adapter; they are not public
HTTP routes.

Terminal retry preserves the transcript, provenance, and diagnostic log. The
caller may replace the opaque frontend state so an exhausted model checkpoint
can be discarded before the piece returns to the durable ingress queue.

The completed version-5 queue adoption is no longer part of AudioIngress.
Retired queue files are offline archive material and are never read by this
service.
