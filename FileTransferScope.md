# Bidirectional File Transfer Scope

## Goal

Kennedy and a user can exchange files through browser conversations and
Telegram. User-supplied bytes remain in restart-safe session custody until the
session's existing history-ingress transaction commits them to Kweb. Kennedy
can later recover an eight-character object ID from ordinary node text and
emit that object as a response.

This proposal deliberately does not add a formal file-to-node relationship.
Kennedy may write the canonical object ID into a node's long description.

## Existing foundation

Most of the storage path already exists:

- `kcode-session-log` 0.2.1 durably stages a pending object with its filename,
  media type, checksum, and bytes before appending its `pending-object` event.
- `SessionJournal` gives that event a shared `pending:N` identity and allows a
  user message box to reference it through `BoxContent.objects`.
- browser attachments are already uploaded as multipart data before their
  message command is queued;
- Telegram voice notes and documents are already fetched from the relay and
  staged in the session;
- one history-ingress commit already creates every staged Kweb object, creates
  the session archive, updates nodes, finalizes one transaction, and returns
  pending-to-canonical mappings;
- `kcode-kweb-db` 1.0.3 already provides `create_object` and `get_object`;
- local `kcode-tg-kennedy-bot` 0.3.0 provides bounded inbound native media,
  existing-route byte retrieval, generic document delivery, and explicit
  native-media delivery.

The missing pieces are finalized file metadata, canonical-ID projection,
object response semantics, raw object HTTP delivery, Telegram delivery
orchestration, and browser rendering.

## Minimal product contract

### Input

- The browser accepts any nonempty file, not only extractable documents.
- Telegram accepts voice notes, documents, photos, videos, animations, audio
  tracks, video notes, and stickers.
- Original bytes are always staged first. Text extraction or transcription is
  supplementary and may fail without discarding an otherwise valid file.
- A user message owns its object references. In active sessions they are
  `pending:N`; in completed session views they are canonical object IDs.
- Every staged object remains part of the session transaction even if Kennedy
  elects not to mention it in a node.

### Storage

New user-file objects use a small Kennedy-owned, versioned binary envelope
inside ordinary Kweb object bytes:

```text
8-byte file magic/version
u32 filename byte length
u32 media-type byte length
u32 transport-kind byte length (zero means absent)
u64 content byte length
UTF-8 filename
UTF-8 media type
UTF-8 source transport kind
exact original content bytes
```

Kweb already authenticates and checksums the complete object payload, so the
file envelope needs no second content hash. The decoder rejects invalid
lengths, invalid UTF-8 metadata, trailing bytes, unsafe filenames, and invalid
media types. Filenames are reduced to a safe basename and bounded before
storage.

The envelope solves an otherwise important gap: Kweb intentionally stores only
opaque bytes, while `EmitObject` receives only an object ID. Without the
envelope, a later session cannot reliably recover the original filename or
media type. No Kennedy metadata table or Kweb schema change is needed.

Objects without the file magic remain valid legacy/arbitrary Kweb objects.
They are emitted as generic binary downloads, with conservative signature
sniffing permitted for common image, audio, video, and PDF payloads.

### Pending-ID finalization

No post-commit node update is needed.

`Transaction::create_object` returns each canonical object ID before
`Transaction::finalize`. The existing Kennedy Kmap transaction adapter should:

1. encode and create every staged file object;
2. build the `pending:N -> object ID` map;
3. replace recognized pending-object tokens in staged node descriptive text;
4. replace the same tokens in the session archive bytes;
5. create the finalized session archive object;
6. create/update nodes and finalize the one transaction.

Replacement is limited to exact known pending IDs and observes token
boundaries, so `pending:47` cannot alter `pending:470`. It applies to short
name, short description, and long description for consistency, although long
description is the intended use. Structured node object arrays continue using
their existing typed resolver.

The completed Kweb archive therefore contains canonical IDs in message object
references, attachment metadata, tool arguments, and Kennedy-authored node
text. The on-disk in-progress session log remains immutable and is deleted
through the existing completion path. The completion receipt retains its
pending-to-canonical mapping for audit, recovery, and old archives.

This transformation belongs in Kennedy's Kmap adapter, where the transaction
has allocated the IDs. It does not require object-ID reservation in
`kcode-kweb-db`.

### Output and `EmitObject`

Use the existing tool naming convention:

```json
{"name":"EmitObject","arguments":{"objectId":"gAECAwQF"}}
```

`EmitObject` is available only in user-facing browser, Telegram-private, and
Telegram-group conversation sessions. It:

1. validates the canonical object ID and confirms the object is readable;
2. confirms that the active transport can deliver its byte length;
3. creates one ordinary Kennedy message box whose `BoxContent.objects`
   contains the ID and whose metadata carries the current external event ID;
4. returns a short native success result without creating a duplicate generic
   result box.

The emitted object box is the durable response. The tool does not attach the
object to a node and does not copy it to a new Kweb object.

A successful `EmitObject` may terminate a conversational turn without a final
prose assistant message. Kennedy may emit several objects and may also provide
a final text response. Chatend event order is authoritative, while adapters
may group delivery as required by their transport.

The tool must not directly perform Telegram I/O. It records the response first;
the owning adapter delivers durable response parts afterward. This preserves
the existing restart behavior in which a saved Kennedy response can be
delivered without rerunning inference.

## Browser behavior

### Routes

Add:

```text
GET /api/v1/objects/{object_id}
GET /api/v1/conversations/{session_id}/objects/{pending_id}
```

The canonical route reads and decodes the Kweb file envelope. The pending route
reads the existing session sidecar. Both return exact original bytes with:

- the recorded or conservatively inferred `Content-Type`;
- a sanitized `Content-Disposition` filename;
- `Content-Length`;
- `Cache-Control: no-store`;
- `X-Content-Type-Options: nosniff`.

The canonical route is also the internal source used for Telegram delivery.
V1 may use the existing owned-buffer Kweb API; streaming and range-aware reads
are separate optimization work.

### Rendering

Transcript projection must retain `BoxContent.objects` and attachment
metadata. Render each referenced object as:

- `image/*`: bounded image preview plus download action;
- `video/*`: controlled video element plus download action;
- `audio/*`: controlled audio element plus download action;
- `application/pdf`: inline PDF object where supported plus download action;
- known office/text documents: named document/download card;
- everything else: named generic download card.

The active-session URL uses the session ID and pending ID. The completed view
uses the canonical object route. Rendering continues to use DOM properties and
text nodes rather than injected HTML.

The composer changes from "Upload PDF" and a document allowlist to "Upload
file" with arbitrary file selection. Supported text extraction remains
best-effort enrichment. An unsupported format is still sent to Kennedy as an
object; it is not rejected merely because readable text cannot be extracted.

## Telegram behavior

### Inbound

The relay contract:

- accepts authorized private and allowed-group voice, document, photo, video,
  animation, audio, video-note, and sticker messages;
- preserves original bytes, filename, MIME type, and caption;
- exposes those bytes through media routes;
- applies the configured transport byte limit.

KennedyServer should stage those bytes regardless of extraction support.
Searchable document extraction and voice transcription remain useful Chatend
enrichment. An extraction failure becomes bounded attachment metadata/text
such as "text extraction unavailable" rather than an early error reply that
throws away the file.

### Outbound

The pinned relay already exposes:

```text
POST /api/v1/events/{event_id}/file
```

The 0.3.0 relay additionally exposes `POST
/api/v1/events/{event_id}/media`. KennedyServer selects a native kind using
the stored transport kind (or MIME type for browser uploads) and posts the
exact bytes and essential metadata there. It does not silently fall back:
`/file` is chosen only when Kennedy explicitly wants generic document
semantics.

For one response:

1. deliver every emitted object as a separate Telegram native-media or
   document message according to its retained transport metadata;
2. use `complete=false` while another object or prose response remains;
3. use the existing text reply endpoint for final prose, or `complete=true` on
   the last file for an object-only response.

Telegram's existing send-then-local-completion ambiguity remains: a network or
compare-and-swap failure after Telegram accepts a file can make a retry
duplicate it. This is the same class of side-effect boundary already documented
by the relay. A durable outbox is not introduced for this feature.

The configured Telegram media limit is copied into the Telegram session's
channel capabilities. `EmitObject` fails before recording a successful
response when the object exceeds that limit.

## API and data-shape changes

### `SessionObject`

Extend the internal commit DTO from only:

```text
pending_id, bytes
```

to:

```text
pending_id, file_name, media_type, transport_kind, bytes
```

This is an in-process Kennedy type, not an upstream or public wire contract.

### Transcript response parts

Conversation transcript entries should preserve:

```json
{
  "role": "kennedy",
  "content": "",
  "objects": ["gAECAwQF"],
  "externalEventId": "..."
}
```

User entries likewise retain their object list and attachment metadata.
`answer_for_external_event` becomes response-part aware so an object-only
Kennedy response is complete and recoverable.

### Completion receipts

Completed Session History summary/detail responses should expose their existing
commit receipt instead of discarding it. New archives should already contain
canonical object IDs; the receipt mapping lets the frontend resolve objects in
older archives created before canonical archive finalization.

## Code impact

### Kennedy-owned Rust

- `KennedyServer/src/kmap_http.rs`
  - file-envelope codec;
  - canonical object response route/internal read method;
  - pending-token substitution in node text and archive;
  - commit DTO metadata.
- `KennedyServer/src/orchestration/chatend.rs`
  - preserve object descriptors needed by replay/projection; no new durable
    event kind is required.
- `KennedyServer/src/orchestration/session.rs`
  - `EmitObject`;
  - object-response boxes and transcript parts;
  - object-only terminal-turn handling;
  - pass filename/media type into commit.
- `KennedyServer/src/orchestration/worker.rs`
  - best-effort Telegram extraction;
  - response-part collection;
  - multipart Telegram file delivery and completion ordering.
- `KennedyServer/src/orchestration/http.rs`
  - non-test multipart request helper;
  - internal Kweb file lookup.
- `KennedyServer/src/orchestration/prompts.rs` and
  `Frontend/SystemPrompts/ReadTools.txt`
  - surface capability and exact `EmitObject` contract.
- `ConversationHistory/src/lib.rs`
  - pending-object byte response;
  - completion receipt in completed records;
  - response projection that retains object references.

### Browser

- `Frontend/public/index.html`
  - arbitrary-file picker and label.
- `Frontend/public/js/api.js`
  - canonical and pending object URLs.
- `Frontend/public/js/session_log_view.js`
  - project object references and canonicalize old pending IDs from receipts.
- `Frontend/public/js/render.js`
  - safe media/document/download components.
- `Frontend/public/js/app.js`
  - best-effort extraction and object URL selection.
- `Frontend/public/css/styles.css`
  - bounded media and attachment-card layout.

### Specifications

Update `UserSpecification.md`, `TechnicalDesign.md`,
`ConversationHistory/Specification.md`, and `Frontend/Specification.md` when
implementation begins. The implementation follows this document plus the
native Telegram addendum in `telegram-native-expansion.txt`.

## Upstream-library assessment

Only the native Telegram specialization needs an upstream expansion:

| Library | Existing capability used | Change |
| --- | --- | --- |
| `kcode-kweb-db` 1.0.3 | opaque immutable byte objects, ID returned before finalization, read by ID | none |
| `kcode-session-log` 0.2.1 | durable pending bytes, filename, media type, replay, cleanup | none |
| `kcode-tg-kennedy-bot` 0.3.0 | native and generic media reads/sends, bounded retries, group snapshot fidelity | implemented locally; publication remains separate |
| `kcode-codex-runtime-v2` 0.1.1 | dynamic `call_ktool` bridge | none |
| `kcode-doc-extraction` 0.1.0 | optional readable-text enrichment | none |

KennedyServer targets that native API through the local workspace dependency.
A native send failure is surfaced rather than reinterpreted as document
delivery. The pinned Kweb, session-log, Codex runtime, and extraction crates
need no changes.

## Failure and safety rules

- Reject malformed, missing, or non-object IDs as ordinary tool failures.
- A missing Kweb object never creates a successful response box.
- A transport-size failure is reported to Kennedy before response completion.
- A browser extraction failure never loses an already accepted upload.
- Filenames cannot inject paths, control characters, or response headers.
- Unknown media is downloadable and is never embedded as active HTML.
- Pending IDs never enter durable node text or new completed archive object
  references.
- Object delivery does not mutate Kweb and does not enter the writer lane.
- Session commit remains all-or-nothing for file objects, archive, and node
  text.

The existing 32 GiB Kweb object/transaction ceiling still applies to the
encoded file objects plus the session archive. Envelope overhead and the
archive must be included in final aggregate validation. The current
owned-buffer upload/read path is retained for V1; true large-object streaming
is not part of this feature.

## Verification plan

### Unit

- file envelope round-trip, malformed lengths, unsafe filenames, and legacy
  raw-object fallback;
- pending-token replacement, including prefix/boundary cases;
- `EmitObject` validation, transport limits, replay, and object-only terminal
  turns;
- transcript reconstruction with user and Kennedy object parts.

### Transaction integration

- one staged upload creates one canonical file object and one archive object in
  the same transaction;
- the archive and a created/updated node long description contain the
  canonical ID, never `pending:N`;
- receipt mappings remain correct;
- prepared-receipt recovery returns the same mappings and archive;
- a failed transaction exposes neither the object nor rewritten node state.

### HTTP and browser

- arbitrary upload and pending download;
- canonical image, video, audio, PDF, document, and unknown-object responses;
- active and completed transcript rendering;
- old archive resolution through the completion receipt;
- filenames and captions render only as text.

### Telegram host integration

- inbound arbitrary document is staged even when extraction is unsupported;
- one object-only response posts one file with `complete=true`;
- multiple files and a final text response use correct completion ordering;
- restart with a recorded object response skips a second model call;
- oversized and missing objects fail without completing the relay event as a
  successful response.

The relay crate's own existing tests remain the authority for Telegram update
admission, media retention, file-field validation, and send semantics.

## Suggested implementation order

1. Add file-envelope codec, internal/canonical reads, and pending-ID
   finalization tests.
2. Preserve object response parts through Chatend, replay, transcripts, and
   completion receipts.
3. Add `EmitObject` and object-only turn completion.
4. Add canonical and pending HTTP delivery plus browser rendering.
5. Make browser and Telegram extraction best-effort.
6. Connect Telegram multipart delivery and recovery tests.
7. Update canonical specifications and run the full Rust/frontend suites.
