BEGIN IMMEDIATE;

UPDATE conversations
SET phase = 'complete',
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    ended_at = COALESCE(ended_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ingress_next_attempt_at = NULL,
    version = version + 1
WHERE phase IN ('ingress_pending', 'ingress_in_progress', 'ingress_failed')
  AND (
    json_extract(state_json, '$.sessionType') = 'free-time'
    OR json_extract(state_json, '$.archive.sessionType') = 'free-time'
  );

PRAGMA user_version = 5;

COMMIT;
