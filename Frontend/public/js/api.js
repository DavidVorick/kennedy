// Browser-only HTTP client helpers. Backend orchestration is implemented in Rust.
export class ApiError extends Error {
  constructor(message, status = 0, code = "network_error") { super(message); this.name = "ApiError"; this.status = status; this.code = code; }
}

export function newIdempotencyId() {
  const bytes = new Uint8Array(16);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("");
}

export async function requestJSON(base, path, options = {}) {
  let response;
  try {
    response = await fetch(`${base}${path}`, {
      ...options,
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    });
  } catch (error) {
    if (error?.name === "AbortError") throw error;
    throw new ApiError(`Could not reach ${base}.`, 0, "network_error");
  }
  const isJSON = (response.headers.get("content-type") || "").includes("application/json");
  const payload = isJSON ? await response.json().catch(() => null) : await response.text().catch(() => "");
  if (!response.ok) {
    const remote = payload?.error;
    const requestId = remote?.request_id ? ` (request ID: ${remote.request_id})` : "";
    throw new ApiError(`${remote?.message || `Request failed (${response.status}).`}${requestId}`, response.status, remote?.code || "request_failed");
  }
  return payload;
}

export async function requestFormJSON(base, path, form) {
  let response;
  try {
    response = await fetch(`${base}${path}`, { method: "POST", body: form });
  } catch {
    throw new ApiError(`Could not reach ${base}.`, 0, "network_error");
  }
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const remote = payload?.error;
    const requestId = remote?.request_id ? ` (request ID: ${remote.request_id})` : "";
    throw new ApiError(`${remote?.message || `Request failed (${response.status}).`}${requestId}`, response.status, remote?.code || "request_failed");
  }
  return payload;
}

export function hydrateStoredNodeConnections(node) {
  const summariesById = new Map((node?.connection_summaries || []).map(summary => [summary.id, summary]));
  const hydrate = connection => {
    const stored = typeof connection === "string" ? { id: connection } : connection;
    return { ...(summariesById.get(stored.id) || {}), ...stored };
  };
  return {
    fixedConnections: (node?.fixed_connections || []).map((connection, index) => ({ ...hydrate(connection), slot: connection.slot || index + 1 })),
    recentConnections: (node?.recent_connections || []).map(hydrate),
  };
}

export const KwebAPI = (base) => ({
  health: () => requestJSON(base, "/api/v1/kmap/health"),
  roots: () => requestJSON(base, "/api/v1/kmap/roots"),
  node: (id) => requestJSON(base, `/api/v1/kmap/nodes/${id}`),
  history: (id) => requestJSON(base, `/api/v1/kmap/nodes/${id}/history`),
  provenance: (id) => requestJSON(base, `/api/v1/kmap/provenance/${id}`),
  sessionArchive: (id) => requestJSON(base, `/api/v1/session-history/${id}`),
});

export const IntelligenceAPI = (base) => ({
  health: () => requestJSON(base, "/health"),
  providers: () => requestJSON(base, "/api/v1/providers"),
  extractDocument: ({ file, fileName = "document" }) => {
    const form = new FormData();
    form.append("file", file, fileName);
    return requestFormJSON(base, "/api/v1/documents/extract", form);
  },
  transcribe: ({ provider, model, file, fileName = "voice-note.webm" }) => {
    const form = new FormData();
    if (provider) form.append("provider", provider);
    if (model) form.append("model", model);
    form.append("file", file, fileName);
    return requestFormJSON(base, "/api/v1/audio/transcriptions", form);
  },
});

export const SessionHistoryAPI = (base) => ({
  health: () => requestJSON(base, "/api/v1/conversations/health"),
  list: () => requestJSON(base, "/api/v1/conversations/summaries"),
  start: (body) => requestJSON(base, "/api/v1/conversations/start", { method: "POST", body: JSON.stringify(body) }),
  commandHeads: () => requestJSON(base, "/api/v1/conversation-commands"),
  queueCommand: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/commands`, { method: "POST", body: JSON.stringify(body) }),
  stageObject: (id, file, fileName = "object") => {
    const form = new FormData();
    form.append("file", file, fileName);
    return requestFormJSON(base, `/api/v1/conversations/${id}/objects`, form);
  },
  stop: (id) => requestJSON(base, `/api/v1/conversations/${id}/stop`, { method: "POST", body: "{}" }),
  get: (id) => requestJSON(base, `/api/v1/conversations/${id}`),
  retryIngress: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/retry-ingress`, { method: "POST", body: JSON.stringify(body) }),
});

export const AudioIngressAPI = (base) => ({
  health: () => requestJSON(base, "/api/v1/audio-ingress/health"),
  list: (limit = 100) => requestJSON(base, `/api/v1/audio-ingress?limit=${encodeURIComponent(limit)}`),
  history: (id) => requestJSON(base, `/api/v1/audio-ingress/${id}/history`),
  retryIngress: (id, body) => requestJSON(base, `/api/v1/audio-ingress/pieces/${id}/retry-ingress`, { method: "POST", body: JSON.stringify(body) }),
});

export const TelegramRelayAPI = (base) => ({
  health: () => requestJSON(base, "/health"),
});
