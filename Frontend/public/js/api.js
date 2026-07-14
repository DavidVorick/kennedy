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
});

export const ConversationHistoryAPI = (base) => ({
  health: () => requestJSON(base, "/health"),
  list: () => requestJSON(base, "/api/v1/conversations"),
  current: () => requestJSON(base, "/api/v1/conversations/current"),
  nextIngress: () => requestJSON(base, "/api/v1/conversations/ingress/next"),
  get: (id) => requestJSON(base, `/api/v1/conversations/${id}`),
  create: (body) => requestJSON(base, "/api/v1/conversations", { method: "POST", body: JSON.stringify(body) }),
  checkpoint: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/checkpoint`, { method: "PUT", body: JSON.stringify(body) }),
  requestIngress: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/request-ingress`, { method: "POST", body: JSON.stringify(body) }),
  ingressStarted: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/ingress-started`, { method: "POST", body: JSON.stringify(body) }),
  ingressCheckpoint: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/ingress-checkpoint`, { method: "PUT", body: JSON.stringify(body) }),
  ingressCompleted: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/ingress-completed`, { method: "POST", body: JSON.stringify(body) }),
});
