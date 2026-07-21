// Browser-only validation and display helpers for backend-owned self time.
export const DEFAULT_FREE_TIME_MINUTES = 30;
export const FREE_TIME_WARNING_MS = 3 * 60 * 1000;
export const FREE_TIME_HARD_STOP_GRACE_MS = 2 * 60 * 1000;
export const MAX_FREE_TIME_MINUTES = 7 * 24 * 60;
export const MAX_SELF_TIME_PROMPT_CHARACTERS = 20_000;

export function parseFreeTimeMinutes(value) {
  const minutes = Number(value);
  if (!Number.isFinite(minutes) || minutes < 0.1 || minutes > MAX_FREE_TIME_MINUTES) {
    throw new Error(`Self time must be between 0.1 and ${MAX_FREE_TIME_MINUTES.toLocaleString("en-US")} minutes.`);
  }
  return minutes;
}

export function parseSelfTimePrompt(value) {
  const prompt = String(value ?? "").trim();
  if ([...prompt].length > MAX_SELF_TIME_PROMPT_CHARACTERS) {
    throw new Error(`The self-time prompt must be at most ${MAX_SELF_TIME_PROMPT_CHARACTERS.toLocaleString("en-US")} characters.`);
  }
  return prompt;
}

export function freeTimeTiming(freeTime, now = Date.now()) {
  const deadlineMs = Date.parse(freeTime?.deadlineAt);
  if (!Number.isFinite(deadlineMs)) throw new Error("The self-time session has no valid deadline.");
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

export function formatFreeTimeRemaining(milliseconds) {
  const seconds = Math.max(0, Math.ceil(milliseconds / 1000));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainder = seconds % 60;
  if (hours) return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  return `${minutes}:${String(remainder).padStart(2, "0")}`;
}
