import { formatKmapContext } from "./human_format.js?v=20260715.7";

const RESET_HISTORY_KIND = "reset-history";

function jsonCopy(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function compactNodeName(value) {
  return String(value || "Unnamed memory").replace(/\s+/g, " ").trim() || "Unnamed memory";
}

export class Chatend {
  constructor(systemPrompt, context, retained = []) { this.systemPrompt = systemPrompt; this.context = context; this.retained = retained; this.messages = []; this.historySegments = []; this.restoreResetHistory(); this.rebuild(); }

  contextMessage() { return { role: "system", display_role: "Kmap context", context_kind: "memory", content: formatKmapContext(this.context.snapshot()) }; }

  rebuild() { this.messages = [{ role: "system", display_role: "Agent manuals", context_kind: "instructions", content: this.systemPrompt }, ...this.retained.map(item => ({ ...item })), this.contextMessage()]; }

  restoreMessages(messages, retained = this.retained) {
    this.retained = retained;
    this.restoreResetHistory();
    this.messages = messages.map(message => ({ ...message }));
    const instructionIndex = this.messages.findIndex(message => message.context_kind === "instructions");
    if (instructionIndex >= 0) this.messages[instructionIndex] = { ...this.messages[instructionIndex], content: this.systemPrompt };
    else this.messages.unshift({ role: "system", display_role: "Agent manuals", context_kind: "instructions", content: this.systemPrompt });
    const memoryIndex = this.messages.findIndex(message => message.context_kind === "memory");
    if (memoryIndex >= 0) this.messages[memoryIndex] = this.contextMessage();
    else this.messages.push(this.contextMessage());
  }

  restoreResetHistory() {
    const saved = this.retained.find(item => item.context_kind === RESET_HISTORY_KIND)?.reset_history_entries;
    this.resetHistory = Array.isArray(saved)
      ? saved.map(entry => ({
        retainedNodeNames: Array.isArray(entry.retainedNodeNames) ? entry.retainedNodeNames.map(compactNodeName) : [],
        budgetUsed: Number.isInteger(entry.budgetUsed) ? entry.budgetUsed : 0,
        budgetLimit: Number.isInteger(entry.budgetLimit) ? entry.budgetLimit : 0,
      }))
      : [];
  }

  restoreFullHistory(segments) {
    this.historySegments = Array.isArray(segments)
      ? segments.filter(segment => Array.isArray(segment?.messages)).map(segment => jsonCopy(segment))
      : [];
  }

  fullHistorySnapshot() { return { segments: jsonCopy(this.historySegments) }; }

  resetHistoryMessage() {
    const groups = new Map();
    for (const entry of this.resetHistory) {
      const names = [...entry.retainedNodeNames].sort((left, right) => left.localeCompare(right));
      const key = JSON.stringify(names);
      if (!groups.has(key)) groups.set(key, { names, count: 0 });
      groups.get(key).count += 1;
    }
    const latest = this.resetHistory.at(-1);
    return {
      role: "system",
      display_role: "ResetContext history",
      context_kind: RESET_HISTORY_KIND,
      reset_history_entries: this.resetHistory.map(entry => ({ ...entry, retainedNodeNames: [...entry.retainedNodeNames] })),
      content: [
        `ResetContext history · ${this.resetHistory.length} successful call${this.resetHistory.length === 1 ? "" : "s"} · shared context-load budget at latest reset: ${latest.budgetUsed}/${latest.budgetLimit}`,
        ...[...groups.values()].map(group => `${group.count}× ${group.names.length ? group.names.join(" | ") : "roots only"}`),
      ].join("\n"),
    };
  }

  rebuildAfterReset(selfMessage, resetHistoryEntry, assistantMessage, ...followingMessages) {
    const boundaryIndex = followingMessages.findIndex(message => message?.full_history_boundary === true);
    const boundary = boundaryIndex >= 0 ? followingMessages.splice(boundaryIndex, 1)[0] : {};
    this.historySegments.push({
      reason: "ResetContext",
      messages: jsonCopy(this.messages),
      memory: jsonCopy(boundary.memory || null),
      usage: jsonCopy(boundary.usage || null),
    });
    this.resetHistory.push({
      retainedNodeNames: (resetHistoryEntry?.retainedNodeNames || []).map(compactNodeName),
      budgetUsed: resetHistoryEntry?.budgetUsed || 0,
      budgetLimit: resetHistoryEntry?.budgetLimit || 0,
    });
    this.retained = this.retained.filter(item => item.context_kind !== RESET_HISTORY_KIND);
    this.retained.push(this.resetHistoryMessage());
    if (typeof selfMessage === "string") {
      this.retained.push({ role: "assistant", display_role: "Kennedy note to self", context_kind: "reset-note", content: selfMessage });
    }
    this.rebuild();
    this.messages.push(assistantMessage, ...followingMessages);
  }
  append(message) { this.messages.push(message); }
  replaceRetained(retained) { this.retained = retained; this.restoreResetHistory(); this.rebuild(); }
}
