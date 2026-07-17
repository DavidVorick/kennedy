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

export const KwebAPI = (base) => ({
  health: () => requestJSON(base, "/health"),
  user: () => requestJSON(base, "/api/v1/user"),
  node: (id) => requestJSON(base, `/api/v1/nodes/${id}`),
  context: (id) => requestJSON(base, `/api/v1/nodes/${id}/context`),
  history: (id) => requestJSON(base, `/api/v1/nodes/${id}/history`),
  provenance: (id) => requestJSON(base, `/api/v1/provenance/${id}`),
  createProvenance: (body) => requestJSON(base, "/api/v1/provenance", { method: "POST", body: JSON.stringify(body) }),
  createNode: (body) => requestJSON(base, "/api/v1/nodes", { method: "POST", body: JSON.stringify(body) }),
  bootstrapNode: (nodeId, shortName = null) => requestJSON(base, "/api/v1/nodes/bootstrap", {
    method: "POST",
    body: JSON.stringify({ node_id: nodeId, ...(shortName ? { short_name: shortName } : {}) }),
  }),
  updateNode: (id, body) => requestJSON(base, `/api/v1/nodes/${id}`, { method: "PUT", body: JSON.stringify(body) }),
  connect: (nodeIds, modelAttribution) => requestJSON(base, "/api/v1/connections", { method: "POST", body: JSON.stringify({ node_ids: nodeIds, model_attribution: modelAttribution }) }),
  consolidateFanout: (body) => requestJSON(base, "/api/v1/connections/consolidate-fanout", { method: "POST", body: JSON.stringify(body) }),
  setFixedConnection: (body) => requestJSON(base, "/api/v1/fixed-connections", { method: "POST", body: JSON.stringify(body) }),
});

export const IntelligenceAPI = (base) => ({
  health: () => requestJSON(base, "/health"),
  providers: () => requestJSON(base, "/api/v1/providers"),
  generate: (body, { signal = null, operationId = null } = {}) => requestJSON(base, "/api/v1/generate", {
    method: "POST",
    body: JSON.stringify(operationId ? { ...body, operation_id: operationId } : body),
    signal,
  }),
  webSearch: (body, { signal = null, operationId = null } = {}) => requestJSON(base, "/api/v1/web/search", {
    method: "POST",
    body: JSON.stringify(operationId ? { ...body, operation_id: operationId } : body),
    signal,
  }),
  webFetch: (body, { signal = null, operationId = null } = {}) => requestJSON(base, "/api/v1/web/fetch", {
    method: "POST",
    body: JSON.stringify(operationId ? { ...body, operation_id: operationId } : body),
    signal,
  }),
  cancelOperation: (operationId) => requestJSON(base, `/api/v1/operations/${encodeURIComponent(operationId)}/cancel`, { method: "POST" }),
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
  purge: (id, body) => requestJSON(base, `/api/v1/conversations/${id}`, { method: "DELETE", body: JSON.stringify(body) }),
  create: (body) => requestJSON(base, "/api/v1/conversations", { method: "POST", body: JSON.stringify(body) }),
  checkpoint: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/checkpoint`, { method: "PUT", body: JSON.stringify(body) }),
  requestIngress: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/request-ingress`, { method: "POST", body: JSON.stringify(body) }),
  completeWithoutIngress: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/complete`, { method: "POST", body: JSON.stringify(body) }),
  ingressStarted: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/ingress-started`, { method: "POST", body: JSON.stringify(body) }),
  ingressCheckpoint: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/ingress-checkpoint`, { method: "PUT", body: JSON.stringify(body) }),
  ingressCompleted: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/ingress-completed`, { method: "POST", body: JSON.stringify(body) }),
  ingressFailure: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/ingress-failure`, { method: "POST", body: JSON.stringify(body) }),
  retryIngress: (id, body) => requestJSON(base, `/api/v1/conversations/${id}/retry-ingress`, { method: "POST", body: JSON.stringify(body) }),
});

export const AudioIngressAPI = (base) => ({
  health: () => requestJSON(base, "/health"),
  list: (limit = 100) => requestJSON(base, `/api/v1/audio-ingress?limit=${encodeURIComponent(limit)}`),
  get: (id) => requestJSON(base, `/api/v1/audio-ingress/${id}`),
  history: (id) => requestJSON(base, `/api/v1/audio-ingress/${id}/history`),
  bySha256: (sha256) => requestJSON(base, `/api/v1/audio-ingress/by-sha256/${sha256}`),
  nextIngress: () => requestJSON(base, "/api/v1/audio-ingress/ingress/next"),
  getPiece: (id) => requestJSON(base, `/api/v1/audio-ingress/pieces/${id}`),
  ingressStarted: (id, body) => requestJSON(base, `/api/v1/audio-ingress/pieces/${id}/ingress-started`, { method: "POST", body: JSON.stringify(body) }),
  ingressCheckpoint: (id, body) => requestJSON(base, `/api/v1/audio-ingress/pieces/${id}/ingress-checkpoint`, { method: "PUT", body: JSON.stringify(body) }),
  ingressCompleted: (id, body) => requestJSON(base, `/api/v1/audio-ingress/pieces/${id}/ingress-completed`, { method: "POST", body: JSON.stringify(body) }),
  ingressFailure: (id, body) => requestJSON(base, `/api/v1/audio-ingress/pieces/${id}/ingress-failure`, { method: "POST", body: JSON.stringify(body) }),
  retryIngress: (id, body) => requestJSON(base, `/api/v1/audio-ingress/pieces/${id}/retry-ingress`, { method: "POST", body: JSON.stringify(body) }),
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
  groupMessageMedia: async (chatId, messageId) => {
    let response;
    try { response = await fetch(`${base}/api/v1/group-messages/${encodeURIComponent(chatId)}/${encodeURIComponent(messageId)}/media`, { cache: "no-store" }); }
    catch { throw new ApiError(`Could not reach ${base}.`, 0, "network_error"); }
    if (!response.ok) throw new ApiError(`Telegram group media fetch failed (${response.status}).`, response.status, "request_failed");
    return response.blob();
  },
  bind: (id, conversationId, expectedConversationId = null) => requestJSON(base, `/api/v1/events/${id}/bind`, {
    method: "POST",
    body: JSON.stringify({ conversationId, ...(expectedConversationId ? { expectedConversationId } : {}) }),
  }),
  saveTranscription: (id, text, transcriptionModel) => requestJSON(base, `/api/v1/events/${id}/transcription`, { method: "POST", body: JSON.stringify({ text, transcriptionModel }) }),
  reply: (id, conversationId, text, contextWarning = null) => requestJSON(base, `/api/v1/events/${id}/reply`, { method: "POST", body: JSON.stringify({ conversationId, text, contextWarning }) }),
  abort: (id, conversationId, message) => requestJSON(base, `/api/v1/events/${id}/abort`, { method: "POST", body: JSON.stringify({ conversationId, message }) }),
  resetCompleted: (id, message) => requestJSON(base, `/api/v1/events/${id}/reset-completed`, { method: "POST", body: JSON.stringify({ message }) }),
  provisioningUsers: () => requestJSON(base, "/api/v1/users/provisioning"),
  userByHandle: (handle) => requestJSON(base, `/api/v1/users/by-handle/${encodeURIComponent(handle)}`),
  userById: (id) => requestJSON(base, `/api/v1/users/${encodeURIComponent(id)}`),
  completeUserRoot: (id, rootNodeId) => requestJSON(base, `/api/v1/users/${encodeURIComponent(id)}/root-ready`, { method: "POST", body: JSON.stringify({ rootNodeId }) }),
  completeHandleRoot: (handle, rootNodeId) => requestJSON(base, `/api/v1/users/by-handle/${encodeURIComponent(handle)}/root-ready`, { method: "POST", body: JSON.stringify({ rootNodeId }) }),
  provisioningGroups: () => requestJSON(base, "/api/v1/groups/provisioning"),
  groupById: (chatId) => requestJSON(base, `/api/v1/groups/${encodeURIComponent(chatId)}`),
  completeGroupRoot: (chatId, rootNodeId) => requestJSON(base, `/api/v1/groups/${encodeURIComponent(chatId)}/root-ready`, { method: "POST", body: JSON.stringify({ rootNodeId }) }),
  groupIngress: () => requestJSON(base, "/api/v1/group-ingress"),
  completeGroupIngress: (id) => requestJSON(base, `/api/v1/group-ingress/${id}/complete`, { method: "POST", body: "{}" }),
  groupSessionUpdates: () => requestJSON(base, "/api/v1/group-sessions/updates"),
  acknowledgeGroupContext: (conversationId, throughMessageId) => requestJSON(base, `/api/v1/group-sessions/${encodeURIComponent(conversationId)}/context-ack`, { method: "POST", body: JSON.stringify({ throughMessageId }) }),
  completeSilentGroupReset: (conversationId) => requestJSON(base, `/api/v1/group-sessions/${encodeURIComponent(conversationId)}/silent-reset-completed`, { method: "POST", body: "{}" }),
  saveGroupMessagePreparation: (chatId, messageId, body) => requestJSON(base, `/api/v1/group-messages/${encodeURIComponent(chatId)}/${encodeURIComponent(messageId)}/preparation`, { method: "POST", body: JSON.stringify(body) }),
});
