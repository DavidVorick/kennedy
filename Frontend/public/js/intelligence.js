import { parseToolCalls, TOOL_CALL_PREFIX } from "./tools.js?v=20260718.1";
import { addTimingStep, createTurnTiming, elapsedMs, timingMessage, updateTimingSummary } from "./timing.js?v=20260715.2";
import { formatChatend } from "./chatend_format.js?v=20260715.9";

export const AGENT_LOOP_ROUND_LIMIT = 100;
export const AGENT_LOOP_SESSION_ENDED = Symbol("agent-loop-session-ended");

function throwIfCancelled(signal) {
  if (!signal?.aborted) return;
  throw Object.assign(new Error("Kennedy's response was stopped."), { code: "turn_stopped" });
}

export function createCacheKey(mode) {
  return `kennedy-${mode}-prompt-v4`;
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
    this.providerThreadTotals = null;
  }

  record(usage, { continued = false } = {}) {
    this.requests += 1;
    if (!usage) return;
    const reported = {
      inputTokens: Number(usage.input_tokens) || 0,
      outputTokens: Number(usage.output_tokens) || 0,
      cachedTokens: Number(usage.cached_tokens) || 0,
      cacheWriteTokens: Number(usage.cache_write_tokens) || 0,
      reasoningTokens: Number(usage.reasoning_tokens) || 0,
    };
    const previous = usage.cumulative === true && continued ? this.providerThreadTotals : null;
    const normalized = previous
      ? Object.fromEntries(Object.entries(reported).map(([key, value]) => [key, Math.max(0, value - (previous[key] || 0))]))
      : reported;
    this.providerThreadTotals = usage.cumulative === true ? reported : null;
    this.totalInputTokens += normalized.inputTokens;
    this.totalOutputTokens += normalized.outputTokens;
    this.totalCachedTokens += normalized.cachedTokens;
    this.totalCacheWriteTokens += normalized.cacheWriteTokens;
    this.totalReasoningTokens += normalized.reasoningTokens;
    const reportedContextTokens = normalized.inputTokens + normalized.outputTokens;
    // Codex reports cumulative usage for every internal model pass in a turn.
    // A turn that invokes a native tool can therefore exceed the model's context
    // window even though no individual request did. Do not present that aggregate
    // as the size of the current context.
    this.last = this.contextWindowTokens > 0 && reportedContextTokens > this.contextWindowTokens
      ? null
      : normalized;
  }

  resetThread() {
    this.last = null;
    this.providerThreadTotals = null;
  }

  restore(snapshot) {
    if (!snapshot || typeof snapshot !== "object") return;
    this.requests = Number(snapshot.requests) || 0;
    this.totalInputTokens = Number(snapshot.totalInputTokens) || 0;
    this.totalOutputTokens = Number(snapshot.totalOutputTokens) || 0;
    this.totalCachedTokens = Number(snapshot.totalCachedTokens) || 0;
    this.totalCacheWriteTokens = Number(snapshot.totalCacheWriteTokens) || 0;
    this.totalReasoningTokens = Number(snapshot.totalReasoningTokens) || 0;
    const restoredLast = snapshot.last && typeof snapshot.last === "object" ? { ...snapshot.last } : null;
    const restoredContextTokens = restoredLast
      ? (Number(restoredLast.inputTokens) || 0) + (Number(restoredLast.outputTokens) || 0)
      : 0;
    this.last = this.contextWindowTokens > 0 && restoredContextTokens > this.contextWindowTokens
      ? null
      : restoredLast;
    this.providerThreadTotals = snapshot.providerThreadTotals && typeof snapshot.providerThreadTotals === "object" ? { ...snapshot.providerThreadTotals } : null;
  }

  snapshot() {
    const contextKnown = Boolean(this.last);
    const contextTokens = contextKnown ? this.last.inputTokens + this.last.outputTokens : 0;
    const contextRemaining = contextKnown && this.contextWindowTokens ? Math.max(0, this.contextWindowTokens - contextTokens) : null;
    return {
      requests: this.requests,
      contextWindowTokens: this.contextWindowTokens,
      maxInputTokens: this.maxInputTokens,
      contextKnown,
      contextTokens,
      contextRemaining,
      totalInputTokens: this.totalInputTokens,
      totalOutputTokens: this.totalOutputTokens,
      totalCachedTokens: this.totalCachedTokens,
      totalCacheWriteTokens: this.totalCacheWriteTokens,
      totalReasoningTokens: this.totalReasoningTokens,
      cacheReadPercent: this.totalInputTokens ? (100 * this.totalCachedTokens / this.totalInputTokens) : 0,
      last: this.last,
      providerThreadTotals: this.providerThreadTotals,
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

async function checkpointContinuation(chatend, directive, timing, onUpdate, checkpoint) {
  if (!directive?.continueWith) return false;
  chatend.append(directive.continueWith);
  onUpdate();
  const checkpointStarted = performance.now();
  await checkpoint();
  addTimingStep(timing, "checkpoint", "Self-time continuation save", elapsedMs(checkpointStarted));
  return true;
}

export async function runAgentLoop({ intelligence, provider, model, chatend, executor, continuation, usage, timing = createTurnTiming(), onUpdate = () => {}, checkpoint = async () => {}, roundOffset = 0, onRoundStart = async () => {}, onResponse = async () => null, onFinal = async () => null, onNoAnswer = async () => null, signal = null, operationId = null, requestTimeoutSeconds = null }) {
  for (let round = roundOffset; round < AGENT_LOOP_ROUND_LIMIT; round++) {
    throwIfCancelled(signal);
    const roundDirective = await onRoundStart(round + 1);
    throwIfCancelled(signal);
    const messages = continuation.requestMessages(chatend);
    if (messages.length === 0) throw new Error("Kennedy has no new context to continue from.");
    const llmStarted = performance.now();
    let response;
    const continued = Boolean(continuation.previousResponseId);
    try {
      const timeoutSeconds = requestTimeoutSeconds?.();
      response = await intelligence.generate(
        {
          provider,
          model,
          chatend: formatChatend(messages, usage.snapshot()),
          previous_response_id: continuation.previousResponseId,
          prompt_cache_key: continuation.cacheKey,
          ...(timeoutSeconds ? { timeout_seconds: timeoutSeconds } : {}),
        },
        { signal, operationId },
      );
    } catch (error) {
      if (error?.code === "empty_assistant_message") {
        const directive = await onNoAnswer(round + 1);
        if (await checkpointContinuation(chatend, directive, timing, onUpdate, checkpoint)) continue;
      }
      if (error?.code === "stale_codex_thread" && continuation.previousResponseId) {
        continuation.reset();
        usage.resetThread();
        onUpdate();
        continue;
      }
      throw error;
    } finally {
      addTimingStep(timing, "llm", "LLM call", elapsedMs(llmStarted));
    }
    throwIfCancelled(signal);
    usage.record(response.usage, { continued });
    const content = response.message?.content;
    if (response.status !== "complete" || typeof content !== "string") throw new Error("The intelligence service returned an invalid text generation.");
    const emptyAnswer = !content.trim();
    if (!emptyAnswer) chatend.append(response.message);
    continuation.accept(response.response_id, chatend.messages.length);
    const llmTimingMessage = timingMessage("LLM call", timing.steps.at(-1).durationMs);
    chatend.append(llmTimingMessage);
    onUpdate();
    const responseDirective = await onResponse(round + 1);

    if (roundDirective?.endAfterResponse || responseDirective?.endAfterResponse) {
      executor.sessionEndContent = content;
      const summary = updateTimingSummary(timing);
      chatend.append(summary);
      onUpdate();
      const checkpointStarted = performance.now();
      await checkpoint();
      addTimingStep(timing, "checkpoint", "Final self-time save", elapsedMs(checkpointStarted));
      return AGENT_LOOP_SESSION_ENDED;
    }

    if (emptyAnswer) {
      const directive = await onNoAnswer(round + 1);
      if (await checkpointContinuation(chatend, directive, timing, onUpdate, checkpoint)) continue;
      throw new Error("The intelligence service returned no assistant answer.");
    }

    let calls;
    try { calls = parseToolCalls(content); }
    catch (error) {
      chatend.append(protocolFailureMessage(error));
      onUpdate();
      const checkpointStarted = performance.now();
      await checkpoint();
      addTimingStep(timing, "checkpoint", "Tool-round save", elapsedMs(checkpointStarted));
      continue;
    }
    if (!calls) {
      const directive = await onFinal(content, round + 1);
      if (await checkpointContinuation(chatend, directive, timing, onUpdate, checkpoint)) continue;
      const summary = updateTimingSummary(timing);
      chatend.append(summary);
      onUpdate();
      return content;
    }

    calls = calls.map((call, index) => ({ ...call, id: `text_call_${round + 1}_${index + 1}` }));
    const resetIsMixed = calls.length > 1 && calls.some(call => call.name === "ResetContext");
    const sessionEndIsMixed = calls.length > 1 && calls.some(call => ["EndSelfTimeSession", "EndFreeTimeSession"].includes(call.name));
    let sessionEnded = false;
    for (const call of calls) {
      throwIfCancelled(signal);
      const toolStarted = performance.now();
      const execution = resetIsMixed && call.name === "ResetContext"
        ? executor.failure(call, "mixed_reset_call", "ResetContext must be requested by itself so the chatend can be rebuilt safely.")
        : sessionEndIsMixed && ["EndSelfTimeSession", "EndFreeTimeSession"].includes(call.name)
          ? executor.failure(call, "mixed_session_end_call", `${call.name} must be requested by itself so the session can close safely.`)
        : await executor.execute(call, { signal, operationId });
      throwIfCancelled(signal);
      const durationMs = addTimingStep(
        timing,
        "tool",
        call.name,
        Number.isInteger(execution.durationMs) ? execution.durationMs : elapsedMs(toolStarted),
      );
      if (execution.reset) {
        chatend.rebuildAfterReset(
          execution.selfMessage,
          execution.resetHistoryEntry,
          response.message,
          llmTimingMessage,
          execution.message,
          { full_history_boundary: true, memory: execution.previousContext, usage: usage.snapshot() },
        );
        continuation.reset();
        usage.resetThread();
      } else {
        chatend.append(execution.message);
      }
      sessionEnded ||= execution.endSession;
      onUpdate();
    }
    const checkpointStarted = performance.now();
    await checkpoint();
    addTimingStep(timing, "checkpoint", "Tool-round save", elapsedMs(checkpointStarted));
    if (sessionEnded) return AGENT_LOOP_SESSION_ENDED;
  }
  throw new Error(`Kennedy exceeded the ${AGENT_LOOP_ROUND_LIMIT}-round tool-loop safety limit.`);
}
