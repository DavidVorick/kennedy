# Kmap HTTP Adapter

`kennedy-server` owns the application representation of the storage-only
`kcode-kweb-db` 1.0 library. The library itself contains no Axum or filesystem asset
serving code.

The adapter owns one transactional `KwebDb`, serves the frontend on the same
listener, and exposes these browser-facing reads:

- `GET /api/v1/kmap/health`
- `GET /api/v1/kmap/roots`
- `GET /api/v1/kmap/provenance/{id}`
- `GET /api/v1/kmap/nodes/{id}`
- `GET /api/v1/kmap/nodes/{id}/history`

The same listener also merges Session History and audio-ingress routes before
serving frontend assets.
Those routes remain separate application domains and are not Kmap storage
methods.

The roots endpoint is application policy rather than a Kmap method. System
root mappings live in `kmap_system_roots` in the separate identity database.
The Kweb root must already satisfy the strict 1.0 binary format. Startup performs
WAL recovery supplied by the core but does not replay the transaction log or run
legacy-format migration.

Kmap mutations and provenance artifact persistence are backend-only operations
through the in-process service adapter. They are not exposed as HTTP routes.

## Representations

A node response is:

```json
{
  "id": "AAECAwQF",
  "short_name": "Name",
  "short_description": "Summary",
  "long_description": "Full text",
  "last_modified_by": "opaque caller attribution",
  "last_modified_at": "2026-07-18T00:00:00Z",
  "owner_node_id": "eight-character node ID or null",
  "fixed_connections": ["eight-character node ID"],
  "recent_connections": ["eight-character node ID"],
  "objects": ["eight-character object ID"],
  "connection_summaries": [
    {
      "id": "eight-character node ID",
      "short_name": "Connected node name",
      "short_description": "Connected node summary"
    }
  ]
}
```

`fixed_connections` and `recent_connections` remain the canonical ordered ID
arrays. `connection_summaries` is an additive read projection containing each
unique referenced node once, in first-reference order, so clients can render
fixed, active, and fanout links without fetching every full connected node.

Backend create and update requests contain every stored field above except the two
generated modification fields, and omit the read-only `connection_summaries`
projection. They use `provenance_id`, `model_attribution`, and `owner_node_id`;
the owner input is a durable ID, `self`, or `unowned`.
The adapter currently accepts the existing `idempotency_id` field for caller
compatibility and stores its application receipt in the identity database. It
writes a pending receipt before entering the Kweb mutation, then records the
eight-character result ID after commit. An exact completed replay returns the
same result without another Kweb write; mismatched reuse conflicts. If the
process fails in the narrow cross-database window, the pending receipt blocks
automatic replay for offline recovery instead of risking a duplicate. Native
Kweb transaction IDs independently suppress receipt of the same signed
transaction package more than once. Create/update responses wrap the same
enriched node representation as `{ "node": ... }`; a direct get returns that
representation itself.

Backend provenance creation accepts required `idempotency_id` plus `data`, `source`,
and `source_created_at`, and returns `{ "id": ... }`. A provenance read
returns those fields plus `id` and ordered `artifacts` metadata.
The in-process archive operation preserves each artifact filename and content
type and moves all bytes into immutable Kweb objects without cloning them.
The provenance envelope is a canonical binary Kennedy record stored as another
Kweb object; JSON exists only at the application API boundary.
History returns `{ "node_id": ..., "visible_transaction": ..., "entries": [...] }`. Each native history entry includes its transaction ID, writer, commit time, embedded provenance, active status, and whether the node was created or updated.

Roots returns `user_root_node_id` and `kennedy_root_node_id`.

Validated-library input errors return 400, missing objects return 404,
constraint conflicts return 409, and unexpected storage errors return 500,
using the common `{ "error": { "code": ..., "message": ... } }` envelope.
Axum may reject malformed JSON or an oversized body before a handler runs, in
which case its transport-level rejection format applies.
