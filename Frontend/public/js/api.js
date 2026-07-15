export class ApiError extends Error {
  constructor(message, status = 0, code = "network_error") { super(message); this.name = "ApiError"; this.status = status; this.code = code; }
}

export async function requestJSON(base, path, options = {}) {
  let response;
  try {
    response = await fetch(`${base}${path}`, {
      ...options,
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    });
  } catch (error) {
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

export const KwebAPI = (base) => ({
  health: () => requestJSON(base, "/health"),
  user: () => requestJSON(base, "/api/v1/user"),
  node: (id) => requestJSON(base, `/api/v1/nodes/${id}`),
  context: (id) => requestJSON(base, `/api/v1/nodes/${id}/context`),
  history: (id) => requestJSON(base, `/api/v1/nodes/${id}/history`),
  provenance: (id) => requestJSON(base, `/api/v1/provenance/${id}`),
  createProvenance: (body) => requestJSON(base, "/api/v1/provenance", { method: "POST", body: JSON.stringify(body) }),
  createNode: (body) => requestJSON(base, "/api/v1/nodes", { method: "POST", body: JSON.stringify(body) }),
  updateNode: (id, body) => requestJSON(base, `/api/v1/nodes/${id}`, { method: "PUT", body: JSON.stringify(body) }),
  connect: (nodeIds, modelAttribution) => requestJSON(base, "/api/v1/connections", { method: "POST", body: JSON.stringify({ node_ids: nodeIds, model_attribution: modelAttribution }) }),
  consolidateFanout: (body) => requestJSON(base, "/api/v1/connections/consolidate-fanout", { method: "POST", body: JSON.stringify(body) }),
  assignTask: (body) => requestJSON(base, "/api/v1/tasks", { method: "POST", body: JSON.stringify(body) }),
});

export const IntelligenceAPI = (base) => ({
  health: () => requestJSON(base, "/health"),
  providers: () => requestJSON(base, "/api/v1/providers"),
  generate: (body) => requestJSON(base, "/api/v1/generate", { method: "POST", body: JSON.stringify(body) }),
  webSearch: (body) => requestJSON(base, "/api/v1/web/search", { method: "POST", body: JSON.stringify(body) }),
  webFetch: (body) => requestJSON(base, "/api/v1/web/fetch", { method: "POST", body: JSON.stringify(body) }),
  recordTiming: (body) => requestJSON(base, "/api/v1/timings", { method: "POST", body: JSON.stringify(body) }),
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

export const ConversationHistoryAPI = (base) => ({
  health: () => requestJSON(base, "/health"),
  list: () => requestJSON(base, "/api/v1/conversations"),
  current: () => requestJSON(base, "/api/v1/conversations/current"),
  nextIngress: () => requestJSON(base, "/api/v1/conversations/ingress/next"),
  discardUnstarted: () => requestJSON(base, "/api/v1/conversations/unstarted", { method: "DELETE" }),
  get: (id) => requestJSON(base, `/api/v1/conversations/${id}`),
  create: (body) => requestJSON(base, "/api/v1/conversations", { method: "POST", body: JSON.stringify(body) }),
  checkpoint: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/checkpoint`, { method: "PUT", body: JSON.stringify(body) }),
  requestIngress: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/request-ingress`, { method: "POST", body: JSON.stringify(body) }),
  ingressStarted: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/ingress-started`, { method: "POST", body: JSON.stringify(body) }),
  ingressCheckpoint: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/ingress-checkpoint`, { method: "PUT", body: JSON.stringify(body) }),
  ingressCompleted: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/ingress-completed`, { method: "POST", body: JSON.stringify(body) }),
});

export const TelegramRelayAPI = (base) => ({
  health: () => requestJSON(base, "/health"),
  events: () => requestJSON(base, "/api/v1/events"),
  media: async (id) => {
    let response;
    try { response = await fetch(`${base}/api/v1/events/${id}/media`, { cache: "no-store" }); }
    catch { throw new ApiError(`Could not reach ${base}.`, 0, "network_error"); }
    if (!response.ok) throw new ApiError(`Telegram media fetch failed (${response.status}).`, response.status, "request_failed");
    return response.blob();
  },
  bind: (id, conversationId) => requestJSON(base, `/api/v1/events/${id}/bind`, { method: "POST", body: JSON.stringify({ conversationId }) }),
  saveTranscription: (id, text, transcriptionModel) => requestJSON(base, `/api/v1/events/${id}/transcription`, { method: "POST", body: JSON.stringify({ text, transcriptionModel }) }),
  reply: (id, conversationId, text, contextWarning = null) => requestJSON(base, `/api/v1/events/${id}/reply`, { method: "POST", body: JSON.stringify({ conversationId, text, contextWarning }) }),
  resetCompleted: (id, message) => requestJSON(base, `/api/v1/events/${id}/reset-completed`, { method: "POST", body: JSON.stringify({ message }) }),
});
