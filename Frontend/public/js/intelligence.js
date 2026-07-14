import { parseToolCalls, TOOL_CALL_PREFIX } from "./tools.js?v=20260713.6";

export function createCacheKey(mode) {
  return `kennedy-${mode}-prompt-v2`;
}

export class ContinuationState {
  constructor(cacheKey) { this.cacheKey = cacheKey; this.reset(); }

  requestMessages(chatend) {
    return this.previousResponseId ? chatend.messages.slice(this.sentThrough) : chatend.messages;
  }

  accept(responseId, sentThrough) {
    if (typeof responseId !== "string" || !responseId) throw new Error("The intelligence service returned no continuation ID.");
    this.previousResponseId = responseId;
    this.sentThrough = sentThrough;
  }

  reset() { this.previousResponseId = null; this.sentThrough = 0; this.resets = (this.resets || 0) + 1; }
}

export class UsageTracker {
  constructor({ contextWindowTokens = 0, maxInputTokens = 0 } = {}) {
    this.contextWindowTokens = contextWindowTokens;
    this.maxInputTokens = maxInputTokens;
    this.requests = 0;
    this.totalInputTokens = 0;
    this.totalOutputTokens = 0;
    this.totalCachedTokens = 0;
    this.totalCacheWriteTokens = 0;
    this.totalReasoningTokens = 0;
    this.last = null;
  }

  record(usage) {
    this.requests += 1;
    if (!usage) return;
    const normalized = {
      inputTokens: Number(usage.input_tokens) || 0,
      outputTokens: Number(usage.output_tokens) || 0,
      cachedTokens: Number(usage.cached_tokens) || 0,
      cacheWriteTokens: Number(usage.cache_write_tokens) || 0,
      reasoningTokens: Number(usage.reasoning_tokens) || 0,
    };
    this.totalInputTokens += normalized.inputTokens;
    this.totalOutputTokens += normalized.outputTokens;
    this.totalCachedTokens += normalized.cachedTokens;
    this.totalCacheWriteTokens += normalized.cacheWriteTokens;
    this.totalReasoningTokens += normalized.reasoningTokens;
    this.last = normalized;
  }

  restore(snapshot) {
    if (!snapshot || typeof snapshot !== "object") return;
    this.requests = Number(snapshot.requests) || 0;
    this.totalInputTokens = Number(snapshot.totalInputTokens) || 0;
    this.totalOutputTokens = Number(snapshot.totalOutputTokens) || 0;
    this.totalCachedTokens = Number(snapshot.totalCachedTokens) || 0;
    this.totalCacheWriteTokens = Number(snapshot.totalCacheWriteTokens) || 0;
    this.totalReasoningTokens = Number(snapshot.totalReasoningTokens) || 0;
    this.last = snapshot.last && typeof snapshot.last === "object" ? { ...snapshot.last } : null;
  }

  snapshot() {
    const contextTokens = this.last ? this.last.inputTokens + this.last.outputTokens : 0;
    const contextRemaining = this.contextWindowTokens ? Math.max(0, this.contextWindowTokens - contextTokens) : null;
    return {
      requests: this.requests,
      contextWindowTokens: this.contextWindowTokens,
      maxInputTokens: this.maxInputTokens,
      contextTokens,
      contextRemaining,
      totalInputTokens: this.totalInputTokens,
      totalOutputTokens: this.totalOutputTokens,
      totalCachedTokens: this.totalCachedTokens,
      totalCacheWriteTokens: this.totalCacheWriteTokens,
      totalReasoningTokens: this.totalReasoningTokens,
      cacheReadPercent: this.totalInputTokens ? (100 * this.totalCachedTokens / this.totalInputTokens) : 0,
      last: this.last,
    };
  }
}

function protocolFailureMessage(error) {
  return {
    role: "user",
    display_role: "Tool protocol error",
    content: [
      "Kennedy tool protocol error",
      "",
      error.message,
      `Return either a normal final answer with no ${TOOL_CALL_PREFIX} marker, or a tool request containing only ${TOOL_CALL_PREFIX}, one newline, and one valid JSON envelope whose closing brace is the final non-whitespace character.`,
    ].join("\n"),
  };
}

export async function runAgentLoop({ intelligence, provider, model, chatend, executor, continuation, usage, onUpdate = () => {}, checkpoint = async () => {} }) {
  for (let round = 0; round < 100; round++) {
    const messages = continuation.requestMessages(chatend);
    if (messages.length === 0) throw new Error("Kennedy has no new context to continue from.");
    const response = await intelligence.generate({
      provider,
      model,
      messages,
      previous_response_id: continuation.previousResponseId,
      prompt_cache_key: continuation.cacheKey,
    });
    usage.record(response.usage);
    const content = response.message?.content;
    if (response.status !== "complete" || typeof content !== "string") throw new Error("The intelligence service returned an invalid text generation.");
    chatend.append(response.message);
    continuation.accept(response.response_id, chatend.messages.length);
    onUpdate();

    let calls;
    try { calls = parseToolCalls(content); }
    catch (error) {
      chatend.append(protocolFailureMessage(error));
      onUpdate();
      await checkpoint();
      continue;
    }
    if (!calls) return content;

    calls = calls.map((call, index) => ({ ...call, id: `text_call_${round + 1}_${index + 1}` }));
    const resetIsMixed = calls.length > 1 && calls.some(call => call.name === "ResetContext");
    for (const call of calls) {
      const execution = resetIsMixed && call.name === "ResetContext"
        ? executor.failure(call, "mixed_reset_call", "ResetContext must be requested by itself so the chatend can be rebuilt safely.")
        : await executor.execute(call);
      if (execution.reset) {
        chatend.rebuildAfterReset(response.message, execution.message);
        continuation.reset();
      } else {
        chatend.append(execution.message);
      }
      onUpdate();
    }
    await checkpoint();
  }
  throw new Error("Kennedy exceeded the 100-round tool-loop safety limit.");
}
