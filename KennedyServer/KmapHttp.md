# Kmap HTTP Adapter

`kennedy-server` owns the HTTP representation of the storage-only
`kweb` library. The library itself contains no Axum or filesystem asset
serving code.

The adapter serializes access to one `Kmap`, serves the frontend and prompt
assets on the same listener, and exposes:

- `GET /api/v1/kmap/health`
- `GET /api/v1/kmap/roots`
- `GET /api/v1/kmap/stats`
- `POST /api/v1/kmap/provenance`
- `POST /api/v1/kmap/provenance-with-artifacts`
- `GET /api/v1/kmap/provenance/{id}`
- `GET /api/v1/kmap/provenance-artifacts/{shard}/{filename}`
- `POST /api/v1/kmap/nodes`
- `GET /api/v1/kmap/nodes/{id}`
- `PUT /api/v1/kmap/nodes/{id}`
- `GET /api/v1/kmap/nodes/{id}/history`

The same listener carries the frontend's internal Rust-library tool bridge at
`POST /api/v1/rust-libs/execute` and
`POST /api/v1/rust-libs/release`. Those routes are not Kmap storage methods.
They adapt the browser-owned text-tool loop to the in-process published
`kcode-rust-libs` crate; Kennedy never calls the HTTP representation directly.

The roots endpoint is application policy rather than a Kmap method. System
root mappings live in `kmap_system_roots` in the separate identity database.
On the first upgraded startup, the server copies the legacy role mappings out
of the Kmap database and removes that obsolete table.

An HTTP create may omit `node_id`, in which case the adapter deterministically
derives one from the request's random idempotency identifier so an exact replay
uses the same effective node request. Externally reserved identity roots
provide their exact ID. `owner_node_id` is either a node identifier, `self`, or
`unowned`.

The frontend implements recent-connection activation, graph operations, and
multi-node workflows through ordinary node reads and sequential updates. Those
sequences are intentionally non-atomic and are not one composite idempotent
transaction. Every individual mutation is idempotent under its own required
identifier.

## Representations

A node response is:

```json
{
  "id": "40-lowercase-hex",
  "short_name": "Name",
  "short_description": "Summary",
  "long_description": "Full text",
  "last_modified_by": "opaque caller attribution",
  "last_modified_at": "2026-07-18T00:00:00Z",
  "owner_node_id": "40-lowercase-hex or null",
  "fixed_connections": ["40-lowercase-hex"],
  "recent_connections": ["40-lowercase-hex"]
}
```

Create and update requests contain every field above except the two generated
modification fields. They use `provenance_id`, `model_attribution`, and
`owner_node_id`; the owner input is a durable ID, `self`, or `unowned`.
They also require `idempotency_id`, represented by exactly 32 lowercase
hexadecimal characters (16 random bytes). Creation may additionally supply
`node_id`. Create/update responses wrap the node as `{ "node": ... }`; a
direct get returns the node itself.

Provenance creation accepts required `idempotency_id` plus `data`, `source`,
and `source_created_at`, and returns `{ "id": ... }`. A provenance read
returns those fields plus `id` and ordered `artifacts` metadata.
`provenance-with-artifacts` accepts the same logical fields as multipart form
parts, required `data_filename`, and repeated file parts named `artifact`.
The adapter preserves each multipart filename and content type, assigns the
role `media`, and passes all bytes to the library under the same idempotency
receipt. Artifact metadata exposes a two-component relative path; the
namespaced artifact GET streams that immutable file. Conversation archives
replace each embedded media `dataUrl` with `provenanceArtifactIndex`, an index
into the returned ordered artifact metadata.
History returns `{ "node_id": ..., "provenance_ids": [...] }`, newest first.

The caller generates a new idempotency identifier once per logical mutation
and reuses that exact request for retries. The adapter's frontend client retries
one ambiguous network failure automatically without changing the identifier.
An exact replay succeeds without another provenance or history row. Reusing an
identifier with a different operation or normalized request returns 409. Node
replays return current node state; provenance replays return the originally
created identifier.

Roots returns `user_root_node_id` and `kennedy_root_node_id`. Stats returns the
library's typed stats as JSON. Stats fields are additive: clients must ignore
unknown fields so future statistics do not break existing consumers.

Validated-library input errors return 400, missing objects return 404,
constraint conflicts return 409, and unexpected storage errors return 500,
using the common `{ "error": { "code": ..., "message": ... } }` envelope.
Axum may reject malformed JSON or an oversized body before a handler runs, in
which case its transport-level rejection format applies.
