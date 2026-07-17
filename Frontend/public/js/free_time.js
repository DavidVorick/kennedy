export const DEFAULT_FREE_TIME_MINUTES = 30;
export const FREE_TIME_WARNING_MS = 3 * 60 * 1000;
export const FREE_TIME_HARD_STOP_GRACE_MS = 2 * 60 * 1000;
export const MAX_FREE_TIME_MINUTES = 7 * 24 * 60;

export function parseFreeTimeMinutes(value) {
  const minutes = Number(value);
  if (!Number.isFinite(minutes) || minutes < 0.1 || minutes > MAX_FREE_TIME_MINUTES) {
    throw new Error(`Free time must be between 0.1 and ${MAX_FREE_TIME_MINUTES.toLocaleString("en-US")} minutes.`);
  }
  return minutes;
}

export function freeTimeTiming(freeTime, now = Date.now()) {
  const deadlineMs = Date.parse(freeTime?.deadlineAt);
  if (!Number.isFinite(deadlineMs)) throw new Error("The free-time session has no valid deadline.");
  const remainingMs = deadlineMs - now;
  return {
    deadlineMs,
    hardStopMs: deadlineMs + FREE_TIME_HARD_STOP_GRACE_MS,
    remainingMs,
    warningDue: remainingMs > 0 && remainingMs <= FREE_TIME_WARNING_MS,
    expired: remainingMs <= 0,
    hardExpired: now >= deadlineMs + FREE_TIME_HARD_STOP_GRACE_MS,
  };
}

export function freeTimeRequestTimeoutSeconds(freeTime, now = Date.now()) {
  const { hardStopMs } = freeTimeTiming(freeTime, now);
  return Math.max(1, Math.ceil((hardStopMs - now) / 1000));
}

export function formatFreeTimeRemaining(milliseconds) {
  const seconds = Math.max(0, Math.ceil(milliseconds / 1000));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainder = seconds % 60;
  if (hours) return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  return `${minutes}:${String(remainder).padStart(2, "0")}`;
}

export function freeTimeScheduleText(freeTime, now = Date.now()) {
  const timing = freeTimeTiming(freeTime, now);
  return [
    `Run ID: ${freeTime.runId}`,
    `Clean-slate session: ${freeTime.sliceIndex}`,
    `Free time began: ${freeTime.runStartedAt}`,
    `The shared free-time deadline is: ${freeTime.deadlineAt}`,
    `Time remaining when this session opened: ${formatFreeTimeRemaining(timing.remainingMs)}`,
    "Every clean-slate session in this run shares that same absolute deadline.",
  ].join("\n");
}

export function freeTimeOpeningMessage(freeTime, now = Date.now()) {
  const timing = freeTimeTiming(freeTime, now);
  return [
    `Free time session ${freeTime.sliceIndex} is open.`,
    `You currently have ${formatFreeTimeRemaining(timing.remainingMs)} left in the shared free-time run.`,
    "Begin now. This time is yours—follow your interests, use your tools, make or explore whatever sounds fun, and do not wait for another user message.",
  ].join("\n");
}

export function freeTimeWarningMessage(freeTime, now = Date.now()) {
  const timing = freeTimeTiming(freeTime, now);
  return `Free-time timer notification: ${formatFreeTimeRemaining(timing.remainingMs)} remains. You are inside the final three minutes; start bringing anything important to a natural stopping point.`;
}

export function freeTimeExpiredMessage() {
  return "Free-time timer notification: the scheduled time has ended. This is your final wrap-up round. Do not request or execute more tools; briefly finish your current thought so the session can shut down cleanly.";
}
