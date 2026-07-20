// Historical JavaScript behavior retained only as a migration-parity test oracle.
export const TELEGRAM_RESPONSE_TIMEOUT_MS = 30 * 60 * 1000;

export function telegramEventDeadlineMs(event, now = Date.now()) {
  const persistedStart = Date.parse(event?.processingStartedAt);
  const startedAt = Number.isFinite(persistedStart) ? persistedStart : now;
  return startedAt + TELEGRAM_RESPONSE_TIMEOUT_MS;
}

export function telegramEventTimeoutMs(event, now = Date.now()) {
  return Math.max(0, telegramEventDeadlineMs(event, now) - now);
}
