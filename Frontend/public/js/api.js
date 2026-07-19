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

async function requestKmapMutation(base, path, options) {
  try {
    return await requestJSON(base, path, options);
  } catch (error) {
    if (error?.code !== "network_error") throw error;
    return requestJSON(base, path, options);
  }
}

async function requestKmapFormMutation(base, path, form) {
  try {
    return await requestFormJSON(base, path, form);
  } catch (error) {
    if (error?.code !== "network_error") throw error;
    return requestFormJSON(base, path, form);
  }
}

async function dataUrlBlob(dataUrl) {
  const response = await fetch(dataUrl);
  if (!response.ok) throw new ApiError("An archived media data URL could not be decoded.", response.status, "invalid_media");
  return response.blob();
}

function archiveFilename(source) {
  const safe = String(source || "provenance").replace(/[^A-Za-z0-9_-]+/g, "-");
  return `${safe || "provenance"}-archive.json`;
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

function contextNode(node) {
  const {
    fixed_connections: _fixedConnections = [],
    recent_connections: _recentConnections = [],
    connection_summaries: _connectionSummaries = [],
    ...stored
  } = node;
  const { fixedConnections, recentConnections } = hydrateStoredNodeConnections(node);
  return {
    ...stored,
    owner_root_node_id: node.owner_node_id,
    fixed_connections: fixedConnections,
    active_connections: recentConnections.slice(0, 8),
    fanout_connections: recentConnections.slice(8),
  };
}

export const KwebAPI = (base) => ({
  health: () => requestJSON(base, "/api/v1/kmap/health"),
  roots: () => requestJSON(base, "/api/v1/kmap/roots"),
  stats: () => requestJSON(base, "/api/v1/kmap/stats"),
  node: (id) => requestJSON(base, `/api/v1/kmap/nodes/${id}`),
  context: async (id) => {
    const requested = await requestJSON(base, `/api/v1/kmap/nodes/${id}`);
    const activeIds = hydrateStoredNodeConnections(requested).recentConnections.slice(0, 8).map(connection => connection.id);
    const active = await Promise.all(activeIds.map(activeId => requestJSON(base, `/api/v1/kmap/nodes/${activeId}`)));
    return { requested_node: contextNode(requested), active_connection_nodes: active.map(contextNode) };
  },
  history: (id) => requestJSON(base, `/api/v1/kmap/nodes/${id}/history`),
  provenance: (id) => requestJSON(base, `/api/v1/kmap/provenance/${id}`),
  provenanceArtifact: (relativePath) => {
    const encodedPath = String(relativePath).split("/").map(encodeURIComponent).join("/");
    return fetch(`${base}/api/v1/kmap/provenance-artifacts/${encodedPath}`).then(async response => {
      if (!response.ok) throw new Error(`Kweb artifact request failed (${response.status}).`);
      return response.blob();
    });
  },
  createProvenance: ({ idempotency_id, data, source, source_created_at }) => requestKmapMutation(base, "/api/v1/kmap/provenance", { method: "POST", body: JSON.stringify({ idempotency_id, data, source, source_created_at }) }),
  createProvenanceArchive: async ({ idempotency_id, archive, source, source_created_at }) => {
    const storedArchive = JSON.parse(JSON.stringify(archive));
    const form = new FormData();
    form.append("idempotency_id", idempotency_id);
    form.append("source", source);
    form.append("source_created_at", source_created_at);
    form.append("data_filename", archiveFilename(source));
    for (const media of storedArchive.media || []) {
      if (typeof media?.dataUrl !== "string" || !media.dataUrl) continue;
      const artifactIndex = form.getAll("artifact").length;
      const blob = await dataUrlBlob(media.dataUrl);
      const originalFilename = media.fileName || `provenance-media-${artifactIndex + 1}`;
      delete media.dataUrl;
      media.provenanceArtifactIndex = artifactIndex;
      form.append("artifact", blob, originalFilename);
    }
    form.append("data", JSON.stringify(storedArchive, null, 2));
    return requestKmapFormMutation(base, "/api/v1/kmap/provenance-with-artifacts", form);
  },
  createNode: (body) => requestKmapMutation(base, "/api/v1/kmap/nodes", { method: "POST", body: JSON.stringify(body) }),
  bootstrapNode: async (nodeId, shortName = null) => {
    try { return await requestJSON(base, `/api/v1/kmap/nodes/${nodeId}`); }
    catch (error) { if (error.status !== 404) throw error; }
    const provenance = await requestKmapMutation(base, "/api/v1/kmap/provenance", {
      method: "POST",
      body: JSON.stringify({ idempotency_id: newIdempotencyId(), data: "Automatically provisioned blank Kmap root node.", source: "system-bootstrap", source_created_at: new Date().toISOString() }),
    });
    const created = await requestKmapMutation(base, "/api/v1/kmap/nodes", {
      method: "POST",
      body: JSON.stringify({ idempotency_id: newIdempotencyId(), node_id: nodeId, provenance_id: provenance.id, owner_node_id: "self", model_attribution: "system-bootstrap", short_name: shortName || "User Root", short_description: "", long_description: "", fixed_connections: [], recent_connections: [] }),
    });
    return created.node;
  },
  updateNode: (id, body) => requestKmapMutation(base, `/api/v1/kmap/nodes/${id}`, { method: "PUT", body: JSON.stringify(body) }),
});

export const RustLibsAPI = (base) => ({
  execute: (sessionId, name, args) => requestJSON(base, "/api/v1/rust-libs/execute", {
    method: "POST",
    body: JSON.stringify({ session_id: sessionId, name, arguments: args }),
  }).then(payload => payload.result),
  release: (sessionId) => requestJSON(base, "/api/v1/rust-libs/release", {
    method: "POST",
    body: JSON.stringify({ session_id: sessionId }),
  }),
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
  list: () => requestJSON(base, "/api/v1/conversations/summaries"),
  current: () => requestJSON(base, "/api/v1/conversations/current"),
  nextIngress: () => requestJSON(base, "/api/v1/conversations/ingress/next"),
  releaseIngressRepairs: () => requestJSON(base, "/api/v1/conversations/ingress/repairs/release", { method: "POST" }),
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
  releaseIngressRepairs: () => requestJSON(base, "/api/v1/audio-ingress/ingress/repairs/release", { method: "POST" }),
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
  groupIngress: () => requestJSON(base, "/api/v1/group-ingress"),
  completeGroupIngress: (id) => requestJSON(base, `/api/v1/group-ingress/${id}/complete`, { method: "POST", body: "{}" }),
  groupSessionUpdates: () => requestJSON(base, "/api/v1/group-sessions/updates"),
  acknowledgeGroupContext: (conversationId, throughMessageId) => requestJSON(base, `/api/v1/group-sessions/${encodeURIComponent(conversationId)}/context-ack`, { method: "POST", body: JSON.stringify({ throughMessageId }) }),
  completeSilentGroupReset: (conversationId) => requestJSON(base, `/api/v1/group-sessions/${encodeURIComponent(conversationId)}/silent-reset-completed`, { method: "POST", body: "{}" }),
  saveGroupMessagePreparation: (chatId, messageId, body) => requestJSON(base, `/api/v1/group-messages/${encodeURIComponent(chatId)}/${encodeURIComponent(messageId)}/preparation`, { method: "POST", body: JSON.stringify(body) }),
});

export const TelegramDirectoryAPI = (base) => ({
  provisioningUsers: () => requestJSON(base, "/api/v1/telegram-directory/users/provisioning"),
  userByHandle: (handle) => requestJSON(base, `/api/v1/telegram-directory/users/by-handle/${encodeURIComponent(handle)}`),
  userById: (id) => requestJSON(base, `/api/v1/telegram-directory/users/${encodeURIComponent(id)}`),
  completeUserRoot: (id, rootNodeId) => requestJSON(base, `/api/v1/telegram-directory/users/${encodeURIComponent(id)}/root-ready`, { method: "POST", body: JSON.stringify({ rootNodeId }) }),
  completeHandleRoot: (handle, rootNodeId) => requestJSON(base, `/api/v1/telegram-directory/users/by-handle/${encodeURIComponent(handle)}/root-ready`, { method: "POST", body: JSON.stringify({ rootNodeId }) }),
  provisioningGroups: () => requestJSON(base, "/api/v1/telegram-directory/groups/provisioning"),
  groupById: (groupId) => requestJSON(base, `/api/v1/telegram-directory/groups/${encodeURIComponent(groupId)}`),
  completeGroupRoot: (groupId, rootNodeId) => requestJSON(base, `/api/v1/telegram-directory/groups/${encodeURIComponent(groupId)}/root-ready`, { method: "POST", body: JSON.stringify({ rootNodeId }) }),
});
