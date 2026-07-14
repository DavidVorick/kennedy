DROP INDEX IF EXISTS telegram_events_work_queue;
DROP INDEX IF EXISTS telegram_events_user_queue;

CREATE INDEX telegram_events_work_queue
ON telegram_events(status, update_id);

CREATE INDEX telegram_events_user_queue
ON telegram_events(telegram_user_id, status, update_id);
