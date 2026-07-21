use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Context;
use chrono::{Duration, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const INITIAL_MIGRATION: &str = include_str!("../migrations/001_initial.sql");
pub const COMPLETION_PROTOCOL: &str = "end-turn-v1";
const FAILURE_LIMIT: i64 = 5;
const RETRY_DELAY_SECONDS: i64 = 15;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceKind {
    Conversation,
    Audio,
}

impl SourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conversation => "conversation",
            Self::Audio => "audio",
        }
    }
}

impl std::str::FromStr for SourceKind {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "conversation" => Ok(Self::Conversation),
            "audio" => Ok(Self::Audio),
            _ => Err(Error::invalid("Unknown memory-ingress source kind.")),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Job {
    pub id: String,
    pub source_kind: SourceKind,
    pub source_id: String,
    pub source_created_at: String,
    pub source_position: i64,
    pub phase: String,
    pub provenance_id: Option<String>,
    pub state: Value,
    pub version: i64,
    pub failure_count: i64,
    pub failures: Value,
    pub next_attempt_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug)]
pub struct Submission {
    pub source_kind: SourceKind,
    pub source_id: String,
    pub source_created_at: String,
    pub source_position: i64,
    pub state: Value,
    pub version: i64,
}

#[derive(Clone, Debug)]
pub struct LegacySubmission {
    pub source_kind: SourceKind,
    pub source_id: String,
    pub source_created_at: String,
    pub source_position: i64,
    pub phase: String,
    pub provenance_id: Option<String>,
    pub state: Value,
    pub version: i64,
    pub failure_count: i64,
    pub failures: Value,
    pub next_attempt_at: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Failure {
    pub stage: String,
    pub code: Option<String>,
    pub message: String,
    pub rounds_used: Option<u64>,
    pub context_tokens: Option<u64>,
    pub context_window_tokens: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Invalid,
    NotFound,
    Conflict,
    Internal,
}

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}

impl Error {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Invalid,
            message: message.into(),
        }
    }

    fn not_found() -> Self {
        Self {
            kind: ErrorKind::NotFound,
            message: "Memory-ingress job not found.".into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Conflict,
            message: message.into(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(%error, "memory ingress queue operation failed");
        Self {
            kind: ErrorKind::Internal,
            message: "An unexpected memory-ingress queue error occurred.".into(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

#[derive(Clone)]
pub struct Queue {
    db: Arc<Mutex<Connection>>,
}

impl Queue {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let connection =
            Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        Self::configure(connection).context("initializing memory-ingress queue")
    }

    #[cfg(test)]
    pub fn in_memory() -> anyhow::Result<Self> {
        Self::configure(Connection::open_in_memory()?)
    }

    fn configure(connection: Connection) -> anyhow::Result<Self> {
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;",
        )?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version < 1 {
            connection.execute_batch(INITIAL_MIGRATION)?;
        }
        Ok(Self {
            db: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn submit(&self, submission: Submission) -> Result<Job, Error> {
        validate_submission(
            submission.source_id.as_str(),
            submission.source_created_at.as_str(),
            submission.source_position,
            submission.version,
        )?;
        let state_json = serde_json::to_string(&submission.state).map_err(Error::internal)?;
        let id = job_id(submission.source_kind, &submission.source_id);
        let now = Utc::now().to_rfc3339();
        let db = self.lock()?;
        db.execute(
            "INSERT INTO memory_ingress_jobs(id,source_kind,source_id,source_created_at,source_position,phase,state_json,version,created_at,updated_at)
             VALUES(?1,?2,?3,?4,?5,'ingress_pending',?6,?7,?8,?8)
             ON CONFLICT(source_kind,source_id) DO NOTHING",
            params![id, submission.source_kind.as_str(), submission.source_id, submission.source_created_at, submission.source_position, state_json, submission.version, now],
        ).map_err(Error::internal)?;
        fetch_by_source(&db, submission.source_kind, &submission.source_id)
    }

    /// Imports a record from either retired source-owned queue. Claimed work is
    /// deliberately released because no worker can survive the process restart
    /// during which this migration runs.
    pub fn import_legacy(&self, mut submission: LegacySubmission) -> Result<Job, Error> {
        validate_submission(
            submission.source_id.as_str(),
            submission.source_created_at.as_str(),
            submission.source_position,
            submission.version,
        )?;
        if !matches!(
            submission.phase.as_str(),
            "ingress_pending" | "ingress_in_progress" | "ingress_failed"
        ) {
            return Err(Error::invalid(
                "Legacy memory-ingress phase is not importable.",
            ));
        }
        if submission.phase == "ingress_in_progress" {
            submission.phase = "ingress_pending".into();
            submission.version += 1;
            submission.next_attempt_at =
                Some((Utc::now() + Duration::seconds(RETRY_DELAY_SECONDS)).to_rfc3339());
        }
        let state_json = serde_json::to_string(&submission.state).map_err(Error::internal)?;
        let failures_json = serde_json::to_string(&submission.failures).map_err(Error::internal)?;
        let id = job_id(submission.source_kind, &submission.source_id);
        let now = Utc::now().to_rfc3339();
        let db = self.lock()?;
        db.execute(
            "INSERT INTO memory_ingress_jobs(id,source_kind,source_id,source_created_at,source_position,phase,provenance_id,state_json,version,failure_count,failures_json,next_attempt_at,created_at,updated_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?13)
             ON CONFLICT(source_kind,source_id) DO NOTHING",
            params![id, submission.source_kind.as_str(), submission.source_id, submission.source_created_at, submission.source_position, submission.phase, submission.provenance_id, state_json, submission.version, submission.failure_count, failures_json, submission.next_attempt_at, now],
        ).map_err(Error::internal)?;
        fetch_by_source(&db, submission.source_kind, &submission.source_id)
    }

    pub fn get(&self, source_kind: SourceKind, source_id: &str) -> Result<Option<Job>, Error> {
        let db = self.lock()?;
        optional_by_source(&db, source_kind, source_id)
    }

    pub fn next(&self) -> Result<Option<Job>, Error> {
        let db = self.lock()?;
        db.query_row(
            &format!("{} WHERE phase IN ('ingress_in_progress','ingress_pending') AND (next_attempt_at IS NULL OR datetime(next_attempt_at)<=datetime('now')) ORDER BY CASE phase WHEN 'ingress_in_progress' THEN 0 ELSE 1 END,datetime(source_created_at),source_position,id LIMIT 1", job_select()),
            [],
            row_job,
        )
        .optional()
        .map_err(Error::internal)
    }

    pub fn start(
        &self,
        source_kind: SourceKind,
        source_id: &str,
        expected_version: i64,
        provenance_id: &str,
        completion_protocol: Option<&str>,
    ) -> Result<Job, Error> {
        validate_version(expected_version)?;
        if provenance_id.trim().is_empty() {
            return Err(Error::invalid("provenance_id must not be empty."));
        }
        if completion_protocol != Some(COMPLETION_PROTOCOL) {
            return Err(Error::conflict(
                "This client does not support the required explicit history-ingress completion protocol.",
            ));
        }
        let db = self.lock()?;
        let existing = fetch_by_source(&db, source_kind, source_id)?;
        if existing.phase == "ingress_in_progress"
            && existing.provenance_id.as_deref() == Some(provenance_id)
        {
            return Ok(existing);
        }
        let changed = db.execute(
            "UPDATE memory_ingress_jobs SET phase='ingress_in_progress',provenance_id=?1,next_attempt_at=NULL,updated_at=?2,version=version+1
             WHERE source_kind=?3 AND source_id=?4 AND phase='ingress_pending' AND version=?5
               AND NOT EXISTS(SELECT 1 FROM memory_ingress_jobs WHERE phase='ingress_in_progress')",
            params![provenance_id, Utc::now().to_rfc3339(), source_kind.as_str(), source_id, expected_version],
        ).map_err(Error::internal)?;
        if changed == 0 {
            return Err(Error::conflict(
                "Another job is active or this job is not ready to start memory ingress.",
            ));
        }
        fetch_by_source(&db, source_kind, source_id)
    }

    pub fn checkpoint(
        &self,
        source_kind: SourceKind,
        source_id: &str,
        expected_version: i64,
        state: &Value,
    ) -> Result<Job, Error> {
        validate_version(expected_version)?;
        let state_json = serde_json::to_string(state).map_err(Error::internal)?;
        let db = self.lock()?;
        let changed = db
            .execute(
                "UPDATE memory_ingress_jobs SET state_json=?1,updated_at=?2,version=version+1
             WHERE source_kind=?3 AND source_id=?4 AND phase='ingress_in_progress' AND version=?5",
                params![
                    state_json,
                    Utc::now().to_rfc3339(),
                    source_kind.as_str(),
                    source_id,
                    expected_version
                ],
            )
            .map_err(Error::internal)?;
        if changed == 0 {
            return Err(Error::conflict(
                "Memory ingress changed in another session or is no longer in progress.",
            ));
        }
        fetch_by_source(&db, source_kind, source_id)
    }

    pub fn complete(
        &self,
        source_kind: SourceKind,
        source_id: &str,
        expected_version: i64,
    ) -> Result<Job, Error> {
        validate_version(expected_version)?;
        let db = self.lock()?;
        let existing = fetch_by_source(&db, source_kind, source_id)?;
        if existing.phase == "complete" {
            return Ok(existing);
        }
        if !was_explicitly_ended(&existing.state) {
            return Err(Error::conflict(
                "Memory ingress cannot complete without a successful EndTurn tool call.",
            ));
        }
        let changed = db.execute(
            "UPDATE memory_ingress_jobs SET phase='complete',state_json=json_remove(state_json,'$.historyIngressRepairRequired'),next_attempt_at=NULL,updated_at=?1,version=version+1
             WHERE source_kind=?2 AND source_id=?3 AND phase='ingress_in_progress' AND version=?4",
            params![Utc::now().to_rfc3339(), source_kind.as_str(), source_id, expected_version],
        ).map_err(Error::internal)?;
        if changed == 0 {
            return Err(Error::conflict(
                "Memory-ingress job is not in the expected state.",
            ));
        }
        fetch_by_source(&db, source_kind, source_id)
    }

    pub fn fail(
        &self,
        source_kind: SourceKind,
        source_id: &str,
        expected_version: i64,
        failure: &Failure,
    ) -> Result<Job, Error> {
        validate_version(expected_version)?;
        let mut db = self.lock()?;
        let tx = db.transaction().map_err(Error::internal)?;
        let existing = fetch_by_source(&tx, source_kind, source_id)?;
        if !matches!(
            existing.phase.as_str(),
            "ingress_pending" | "ingress_in_progress"
        ) || existing.version != expected_version
        {
            return Err(Error::conflict(
                "Memory-ingress job is no longer in the expected attempt.",
            ));
        }
        let attempt = existing.failure_count + 1;
        let terminal =
            failure.code.as_deref() == Some("input_too_large") || attempt >= FAILURE_LIMIT;
        let mut failures = existing.failures.as_array().cloned().unwrap_or_default();
        failures.push(json!({
            "attempt":attempt,
            "occurred_at":Utc::now().to_rfc3339(),
            "stage":concise(&failure.stage,80,"unknown"),
            "code":failure.code.as_deref().map(|value|concise(value,80,"unknown_error")),
            "message":concise(&failure.message,2000,"Memory ingress failed without an error message."),
            "rounds_used":failure.rounds_used,
            "context_tokens":failure.context_tokens,
            "context_window_tokens":failure.context_window_tokens,
        }));
        if failures.len() > FAILURE_LIMIT as usize {
            failures.drain(..failures.len() - FAILURE_LIMIT as usize);
        }
        let now_time = Utc::now();
        let phase = if terminal {
            "ingress_failed"
        } else {
            "ingress_pending"
        };
        let next_attempt_at =
            (!terminal).then(|| (now_time + Duration::seconds(RETRY_DELAY_SECONDS)).to_rfc3339());
        let changed = tx.execute(
            "UPDATE memory_ingress_jobs SET phase=?1,failure_count=?2,failures_json=?3,next_attempt_at=?4,updated_at=?5,version=version+1
             WHERE source_kind=?6 AND source_id=?7 AND phase IN ('ingress_pending','ingress_in_progress') AND version=?8",
            params![phase, attempt, serde_json::to_string(&failures).map_err(Error::internal)?, next_attempt_at, now_time.to_rfc3339(), source_kind.as_str(), source_id, expected_version],
        ).map_err(Error::internal)?;
        if changed == 0 {
            return Err(Error::conflict(
                "Memory ingress changed while recording a failure.",
            ));
        }
        tx.commit().map_err(Error::internal)?;
        fetch_by_source(&db, source_kind, source_id)
    }

    pub fn retry(
        &self,
        source_kind: SourceKind,
        source_id: &str,
        expected_version: i64,
        state: &Value,
    ) -> Result<Job, Error> {
        validate_version(expected_version)?;
        let state_json = serde_json::to_string(state).map_err(Error::internal)?;
        let db = self.lock()?;
        let changed = db.execute(
            "UPDATE memory_ingress_jobs SET phase='ingress_pending',state_json=?1,failure_count=0,next_attempt_at=NULL,updated_at=?2,version=version+1
             WHERE source_kind=?3 AND source_id=?4 AND phase='ingress_failed' AND version=?5",
            params![state_json, Utc::now().to_rfc3339(), source_kind.as_str(), source_id, expected_version],
        ).map_err(Error::internal)?;
        if changed == 0 {
            return Err(Error::conflict(
                "Memory-ingress job is not in the expected failed state.",
            ));
        }
        fetch_by_source(&db, source_kind, source_id)
    }

    pub fn release_repairs_for(&self, source_kind: SourceKind) -> Result<usize, Error> {
        let db = self.lock()?;
        db.execute(
            "UPDATE memory_ingress_jobs SET phase='ingress_pending',state_json=json_remove(state_json,'$.historyIngress','$.historyIngressRepairReleasePending'),next_attempt_at=NULL,failure_count=0,updated_at=?1,version=version+1
             WHERE source_kind=?2 AND phase='ingress_failed' AND json_extract(state_json,'$.historyIngressRepairRequired')=1 AND json_extract(state_json,'$.historyIngressRepairReleasePending')=1",
            params![Utc::now().to_rfc3339(), source_kind.as_str()],
        ).map_err(Error::internal)
    }

    pub fn remove(&self, source_kind: SourceKind, source_id: &str) -> Result<bool, Error> {
        let db = self.lock()?;
        db.execute(
            "DELETE FROM memory_ingress_jobs WHERE source_kind=?1 AND source_id=?2",
            params![source_kind.as_str(), source_id],
        )
        .map(|changed| changed > 0)
        .map_err(Error::internal)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, Error> {
        self.db.lock().map_err(Error::internal)
    }
}

fn validate_submission(
    source_id: &str,
    created_at: &str,
    position: i64,
    version: i64,
) -> Result<(), Error> {
    if source_id.trim().is_empty() {
        return Err(Error::invalid(
            "Memory-ingress source ID must not be empty.",
        ));
    }
    if chrono::DateTime::parse_from_rfc3339(created_at).is_err() {
        return Err(Error::invalid(
            "Memory-ingress source timestamp must be RFC 3339.",
        ));
    }
    if position < 0 {
        return Err(Error::invalid(
            "Memory-ingress source position must not be negative.",
        ));
    }
    validate_version(version)
}

fn validate_version(version: i64) -> Result<(), Error> {
    if version < 1 {
        Err(Error::invalid("expected_version must be positive."))
    } else {
        Ok(())
    }
}

fn job_id(kind: SourceKind, source_id: &str) -> String {
    format!("{}:{source_id}", kind.as_str())
}

fn job_select() -> &'static str {
    "SELECT id,source_kind,source_id,source_created_at,source_position,phase,provenance_id,state_json,version,failure_count,failures_json,next_attempt_at,created_at,updated_at FROM memory_ingress_jobs"
}

fn row_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let kind: String = row.get(1)?;
    let state: String = row.get(7)?;
    let failures: String = row.get(10)?;
    Ok(Job {
        id: row.get(0)?,
        source_kind: kind.parse().map_err(|error: Error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        source_id: row.get(2)?,
        source_created_at: row.get(3)?,
        source_position: row.get(4)?,
        phase: row.get(5)?,
        provenance_id: row.get(6)?,
        state: serde_json::from_str(&state).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                7,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        version: row.get(8)?,
        failure_count: row.get(9)?,
        failures: serde_json::from_str(&failures).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                10,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        next_attempt_at: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

fn optional_by_source(
    db: &Connection,
    kind: SourceKind,
    source_id: &str,
) -> Result<Option<Job>, Error> {
    db.query_row(
        &format!("{} WHERE source_kind=?1 AND source_id=?2", job_select()),
        params![kind.as_str(), source_id],
        row_job,
    )
    .optional()
    .map_err(Error::internal)
}

fn fetch_by_source(db: &Connection, kind: SourceKind, source_id: &str) -> Result<Job, Error> {
    optional_by_source(db, kind, source_id)?.ok_or_else(Error::not_found)
}

fn was_explicitly_ended(state: &Value) -> bool {
    state
        .pointer("/historyIngress/tools/log")
        .and_then(Value::as_array)
        .is_some_and(|entries| {
            entries.iter().any(|entry| {
                entry.get("name").and_then(Value::as_str) == Some("EndTurn")
                    && entry.get("ok").and_then(Value::as_bool) == Some(true)
            })
        })
}

fn concise(value: &str, limit: usize, fallback: &str) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let bounded = normalized.chars().take(limit).collect::<String>();
    if bounded.is_empty() {
        fallback.to_owned()
    } else {
        bounded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn submission(kind: SourceKind, id: &str, created: &str, position: i64) -> Submission {
        Submission {
            source_kind: kind,
            source_id: id.into(),
            source_created_at: created.into(),
            source_position: position,
            state: json!({"archive":{"id":id}}),
            version: 2,
        }
    }

    #[test]
    fn conversation_and_audio_share_one_ordered_claim_lane() {
        let queue = Queue::in_memory().unwrap();
        queue
            .submit(submission(
                SourceKind::Audio,
                "piece-2",
                "2026-01-01T00:00:00Z",
                2,
            ))
            .unwrap();
        queue
            .submit(submission(
                SourceKind::Conversation,
                "conversation",
                "2026-01-02T00:00:00Z",
                0,
            ))
            .unwrap();
        queue
            .submit(submission(
                SourceKind::Audio,
                "piece-1",
                "2026-01-01T00:00:00Z",
                1,
            ))
            .unwrap();

        let next = queue.next().unwrap().unwrap();
        assert_eq!(next.source_id, "piece-1");
        let active = queue
            .start(
                SourceKind::Audio,
                "piece-1",
                2,
                "provenance",
                Some(COMPLETION_PROTOCOL),
            )
            .unwrap();
        assert_eq!(active.phase, "ingress_in_progress");
        let conflict = queue
            .start(
                SourceKind::Conversation,
                "conversation",
                2,
                "other",
                Some(COMPLETION_PROTOCOL),
            )
            .unwrap_err();
        assert_eq!(conflict.kind, ErrorKind::Conflict);
        assert_eq!(queue.next().unwrap().unwrap().source_id, "piece-1");
    }

    #[test]
    fn checkpoint_failure_retry_and_completion_are_one_state_machine() {
        let queue = Queue::in_memory().unwrap();
        queue
            .submit(submission(
                SourceKind::Conversation,
                "c",
                "2026-01-01T00:00:00Z",
                0,
            ))
            .unwrap();
        let active = queue
            .start(
                SourceKind::Conversation,
                "c",
                2,
                "p",
                Some(COMPLETION_PROTOCOL),
            )
            .unwrap();
        let checkpoint = queue
            .checkpoint(
                SourceKind::Conversation,
                "c",
                active.version,
                &json!({"historyIngress":{"tools":{"log":[]}}}),
            )
            .unwrap();
        let pending = queue
            .fail(
                SourceKind::Conversation,
                "c",
                checkpoint.version,
                &Failure {
                    stage: "generation".into(),
                    code: None,
                    message: "temporary".into(),
                    rounds_used: None,
                    context_tokens: None,
                    context_window_tokens: None,
                },
            )
            .unwrap();
        assert_eq!(pending.phase, "ingress_pending");
        assert_eq!(pending.failure_count, 1);

        // Remove the retry delay only for this deterministic unit test.
        {
            let db = queue.db.lock().unwrap();
            db.execute("UPDATE memory_ingress_jobs SET next_attempt_at=NULL", [])
                .unwrap();
        }
        let active = queue
            .start(
                SourceKind::Conversation,
                "c",
                pending.version,
                "p",
                Some(COMPLETION_PROTOCOL),
            )
            .unwrap();
        let ended = queue
            .checkpoint(
                SourceKind::Conversation,
                "c",
                active.version,
                &json!({"historyIngress":{"tools":{"log":[{"name":"EndTurn","ok":true}]}}}),
            )
            .unwrap();
        assert_eq!(
            queue
                .complete(SourceKind::Conversation, "c", ended.version)
                .unwrap()
                .phase,
            "complete"
        );
        assert!(queue.next().unwrap().is_none());
    }

    #[test]
    fn legacy_claims_are_released_and_import_is_idempotent() {
        let queue = Queue::in_memory().unwrap();
        let legacy = LegacySubmission {
            source_kind: SourceKind::Audio,
            source_id: "piece".into(),
            source_created_at: "2026-01-01T00:00:00Z".into(),
            source_position: 0,
            phase: "ingress_in_progress".into(),
            provenance_id: Some("p".into()),
            state: json!({}),
            version: 7,
            failure_count: 1,
            failures: json!([]),
            next_attempt_at: None,
        };
        let imported = queue.import_legacy(legacy.clone()).unwrap();
        assert_eq!(imported.phase, "ingress_pending");
        assert_eq!(imported.version, 8);
        assert_eq!(queue.import_legacy(legacy).unwrap().version, 8);
    }
}
