// Historical JavaScript behavior retained only as a migration-parity test oracle.
export function elapsedMs(started) {
  return Math.max(0, Math.round(performance.now() - started));
}

export function formatDuration(durationMs) {
  const milliseconds = Math.max(0, Math.round(Number(durationMs) || 0));
  if (milliseconds < 1_000) return `${milliseconds} ms`;
  if (milliseconds < 60_000) return `${(milliseconds / 1_000).toFixed(3)} s`;
  const minutes = Math.floor(milliseconds / 60_000);
  return `${minutes}m ${((milliseconds % 60_000) / 1_000).toFixed(3)}s`;
}

export function createTurnTiming(sessionType = "conversation") {
  return {
    startedAt: performance.now(),
    sessionType,
    steps: [],
    llmDurationMs: 0,
    toolDurationMs: 0,
    summaryMessage: null,
    reported: false,
  };
}

export function addTimingStep(timing, type, name, durationMs) {
  if (!timing) return 0;
  const measured = Math.max(0, Math.round(Number(durationMs) || 0));
  timing.steps.push({ type, name, durationMs: measured });
  if (type === "llm") timing.llmDurationMs += measured;
  if (type === "tool") timing.toolDurationMs += measured;
  return measured;
}

export function timingMessage(name, durationMs) {
  return {
    role: "system",
    display_role: "Latency",
    context_kind: "timing",
    content: `Latency: ${name} ${formatDuration(durationMs)}`,
  };
}

function summaryContent(timing, totalDurationMs) {
  const callDurationMs = timing.llmDurationMs + timing.toolDurationMs;
  return `Turn latency: ${formatDuration(totalDurationMs)} total · ${formatDuration(callDurationMs)} in LLM/tools`;
}

export function updateTimingSummary(timing, totalDurationMs = elapsedMs(timing.startedAt)) {
  if (!timing.summaryMessage) {
    timing.summaryMessage = {
      role: "system",
      display_role: "Latency summary",
      context_kind: "timing",
      content: "",
    };
  }
  timing.totalDurationMs = Math.max(
    0,
    timing.llmDurationMs + timing.toolDurationMs,
    Math.round(Number(totalDurationMs) || 0),
  );
  timing.summaryMessage.content = summaryContent(timing, timing.totalDurationMs);
  return timing.summaryMessage;
}
