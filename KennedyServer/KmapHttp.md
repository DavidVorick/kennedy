# Kmap HTTP Adapter

`kennedy-server` owns the HTTP representation of the storage-only
`kweb-db-core` library. The library itself contains no Axum or filesystem asset
serving code.

The adapter serializes access to one `Kmap`, serves the frontend on the same
listener, and exposes these browser-facing reads:

- `GET /api/v1/kmap/health`
- `GET /api/v1/kmap/roots`
- `GET /api/v1/kmap/provenance/{id}`
- `GET /api/v1/kmap/nodes/{id}`
- `GET /api/v1/kmap/nodes/{id}/history`

The same listener also merges intelligence, conversation-history,
and audio-ingress routes before serving frontend assets.
Those routes remain separate application domains and are not Kmap storage
methods.

The roots endpoint is application policy rather than a Kmap method. System
root mappings live in `kmap_system_roots` in the separate identity database.
The Kmap database must already satisfy the strict core schema; neither the core
nor the adapter performs compatibility migration during startup.

Kmap mutations and provenance artifact persistence are backend-only operations
through the in-process service adapter. They are not exposed as HTTP routes.

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
  "recent_connections": ["40-lowercase-hex"],
  "connection_summaries": [
    {
      "id": "40-lowercase-hex",
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
They also require `idempotency_id`, represented by exactly 32 lowercase
hexadecimal characters (16 random bytes). Creation may additionally supply
`node_id`. Create/update responses wrap the same enriched node representation as
`{ "node": ... }`; a direct get returns that representation itself.

Backend provenance creation accepts required `idempotency_id` plus `data`, `source`,
and `source_created_at`, and returns `{ "id": ... }`. A provenance read
returns those fields plus `id` and ordered `artifacts` metadata.
The in-process archive operation preserves each artifact filename and content
type and passes all bytes to the library under the same idempotency receipt.
History returns `{ "node_id": ..., "provenance_ids": [...] }`, newest first.

The backend generates a new idempotency identifier once per logical mutation
and reuses that exact request for retries.
An exact replay succeeds without another provenance or history row. Reusing an
identifier with a different operation or normalized request returns 409. Node
replays return current node state; provenance replays return the originally
created identifier.

Roots returns `user_root_node_id` and `kennedy_root_node_id`.

Validated-library input errors return 400, missing objects return 404,
constraint conflicts return 409, and unexpected storage errors return 500,
using the common `{ "error": { "code": ..., "message": ... } }` envelope.
Axum may reject malformed JSON or an oversized body before a handler runs, in
which case its transport-level rejection format applies.
