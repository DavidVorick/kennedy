use std::{collections::BTreeMap, fmt, sync::Mutex};

use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};

use crate::{CostBreakdown, Error, Money, Result, ServiceTier, TokenUsage};

type WindowBounds = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

/// UTC period used by one spending limit.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LimitPeriod {
    /// Current UTC clock hour.
    Hourly,
    /// Current UTC calendar day.
    Daily,
    /// Current UTC calendar month.
    Monthly,
}

impl LimitPeriod {
    /// Returns the stable name for the period.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hourly => "hourly",
            Self::Daily => "daily",
            Self::Monthly => "monthly",
        }
    }
}

impl fmt::Display for LimitPeriod {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Optional spending limits for the current session's UTC hour, day, and month.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpendingLimits {
    /// Current UTC clock-hour limit, or `None` when disabled.
    pub hourly: Option<Money>,
    /// Current UTC calendar-day limit, or `None` when disabled.
    pub daily: Option<Money>,
    /// Current UTC calendar-month limit, or `None` when disabled.
    pub monthly: Option<Money>,
}

impl SpendingLimits {
    pub(crate) const fn any(self) -> bool {
        self.hourly.is_some() || self.daily.is_some() || self.monthly.is_some()
    }
}

/// Current-session spend and configured limits for all three UTC periods.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LimitStatus {
    /// Active limit configuration.
    pub limits: SpendingLimits,
    /// Session spend since the start of the current UTC hour.
    pub hourly_spent: Money,
    /// Session spend since the start of the current UTC day.
    pub daily_spent: Money,
    /// Session spend since the start of the current UTC month.
    pub monthly_spent: Money,
}

/// Time selection for a current-session usage breakdown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UsageWindow {
    /// Every request recorded by the live client.
    AllTime,
    /// Current UTC clock hour.
    CurrentHour,
    /// Current UTC calendar day.
    CurrentDay,
    /// Current UTC calendar month.
    CurrentMonth,
    /// Explicit half-open UTC range `[start, end)`.
    Range {
        /// Inclusive range start.
        start: DateTime<Utc>,
        /// Exclusive range end.
        end: DateTime<Utc>,
    },
}

/// One in-memory usage record. Prompts, media, responses, and credentials are not recorded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsageRecord {
    /// Monotonic identifier within the current client session.
    pub id: u64,
    /// Time at which the provider attempt finished.
    pub occurred_at: DateTime<Utc>,
    /// Stable operation name such as `infer_flash_lite` or `nano_banana_pro`.
    pub operation: String,
    /// Exact requested Gemini model identifier.
    pub model: String,
    /// Requested service tier.
    pub service_tier: ServiceTier,
    /// Whether the provider interaction produced a usable response.
    pub succeeded: bool,
    /// Gemini interaction identifier when the provider supplied one.
    pub provider_request_id: Option<String>,
    /// Local failure class without raw provider or request content.
    pub failure_kind: Option<String>,
    /// Provider-reported token and grounding usage.
    pub usage: TokenUsage,
    /// Locally calculated cost at the crate's compiled pricing version.
    pub cost: CostBreakdown,
}

/// Aggregated request, token, and cost values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UsageTotals {
    /// Total provider attempts.
    pub requests: u64,
    /// Attempts that returned usable model output.
    pub successful_requests: u64,
    /// Attempts that failed locally or at the provider after admission.
    pub failed_requests: u64,
    /// Summed provider token and grounding usage.
    pub usage: TokenUsage,
    /// Summed locally calculated costs.
    pub cost: CostBreakdown,
}

impl UsageTotals {
    fn add_record(&mut self, record: &UsageRecord) {
        self.requests = self.requests.saturating_add(1);
        if record.succeeded {
            self.successful_requests = self.successful_requests.saturating_add(1);
        } else {
            self.failed_requests = self.failed_requests.saturating_add(1);
        }
        self.usage.saturating_add(&record.usage);
        self.cost.saturating_add(&record.cost);
    }
}

/// Usage totals grouped by one model or operation name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupedUsage {
    /// Exact grouping key.
    pub key: String,
    /// Aggregated usage for the key.
    pub totals: UsageTotals,
}

/// Complete current-session aggregate report for one selected window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UsageBreakdown {
    /// Requested time window.
    pub window: UsageWindow,
    /// Report generation time.
    pub generated_at: DateTime<Utc>,
    /// Aggregate across every matching record.
    pub totals: UsageTotals,
    /// Matching usage grouped by exact model identifier.
    pub by_model: Vec<GroupedUsage>,
    /// Matching usage grouped by stable operation name.
    pub by_operation: Vec<GroupedUsage>,
    /// Current UTC hour/day/month limit state, independent of the report window.
    pub limit_status: LimitStatus,
    /// Clarifies the scope and authority of the cost calculation.
    pub accounting_note: String,
}

#[derive(Default)]
struct SessionState {
    next_id: u64,
    records: Vec<UsageRecord>,
    limits: SpendingLimits,
}

pub(crate) struct Accounting {
    state: Mutex<SessionState>,
}

impl Accounting {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(SessionState {
                next_id: 1,
                ..SessionState::default()
            }),
        }
    }

    pub(crate) fn limits(&self) -> Result<SpendingLimits> {
        Ok(self.lock()?.limits)
    }

    pub(crate) fn set_limits(&self, limits: SpendingLimits) -> Result<()> {
        self.lock()?.limits = limits;
        Ok(())
    }

    pub(crate) fn enforce_limits(&self, now: DateTime<Utc>) -> Result<()> {
        let status = self.limit_status_at(now)?;
        for (period, limit, spent) in [
            (
                LimitPeriod::Hourly,
                status.limits.hourly,
                status.hourly_spent,
            ),
            (LimitPeriod::Daily, status.limits.daily, status.daily_spent),
            (
                LimitPeriod::Monthly,
                status.limits.monthly,
                status.monthly_spent,
            ),
        ] {
            if let Some(limit) = limit
                && spent >= limit
            {
                return Err(Error::SpendingLimitReached {
                    period,
                    limit,
                    spent,
                });
            }
        }
        Ok(())
    }

    pub(crate) fn limit_status(&self) -> Result<LimitStatus> {
        self.limit_status_at(Utc::now())
    }

    fn limit_status_at(&self, now: DateTime<Utc>) -> Result<LimitStatus> {
        let state = self.lock()?;
        let (hour, day, month) = period_starts(now);
        Ok(LimitStatus {
            limits: state.limits,
            hourly_spent: sum_cost_since(&state.records, hour),
            daily_spent: sum_cost_since(&state.records, day),
            monthly_spent: sum_cost_since(&state.records, month),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record(
        &self,
        operation: &str,
        model: &str,
        service_tier: ServiceTier,
        succeeded: bool,
        provider_request_id: Option<&str>,
        failure_kind: Option<&str>,
        usage: &TokenUsage,
        cost: &CostBreakdown,
    ) -> Result<u64> {
        let mut state = self.lock()?;
        let id = state.next_id;
        state.next_id = state.next_id.checked_add(1).ok_or_else(|| {
            Error::Accounting("session usage identifier space is exhausted".into())
        })?;
        state.records.push(UsageRecord {
            id,
            occurred_at: Utc::now(),
            operation: operation.into(),
            model: model.into(),
            service_tier,
            succeeded,
            provider_request_id: provider_request_id.map(str::to_owned),
            failure_kind: failure_kind.map(str::to_owned),
            usage: usage.clone(),
            cost: cost.clone(),
        });
        Ok(id)
    }

    pub(crate) fn breakdown(&self, window: UsageWindow) -> Result<UsageBreakdown> {
        let generated_at = Utc::now();
        let records = self.records(&window, None)?;
        let mut totals = UsageTotals::default();
        let mut models = BTreeMap::<String, UsageTotals>::new();
        let mut operations = BTreeMap::<String, UsageTotals>::new();
        for record in &records {
            totals.add_record(record);
            models
                .entry(record.model.clone())
                .or_default()
                .add_record(record);
            operations
                .entry(record.operation.clone())
                .or_default()
                .add_record(record);
        }
        Ok(UsageBreakdown {
            window,
            generated_at,
            totals,
            by_model: grouped(models),
            by_operation: grouped(operations),
            limit_status: self.limit_status_at(generated_at)?,
            accounting_note: concat!(
                "This report covers only requests made through this live Gemini client session. ",
                "Costs apply compiled Gemini Developer API rates to provider-reported usage and ",
                "are local estimates; Google AI Studio and Cloud Billing remain authoritative. ",
                "All records and limits disappear when the session is dropped."
            )
            .into(),
        })
    }

    pub(crate) fn usage_records(
        &self,
        window: UsageWindow,
        maximum: usize,
    ) -> Result<Vec<UsageRecord>> {
        if maximum == 0 || maximum > 10_000 {
            return Err(Error::InvalidInput(
                "maximum usage records must be between 1 and 10000".into(),
            ));
        }
        self.records(&window, Some(maximum))
    }

    fn records(&self, window: &UsageWindow, maximum: Option<usize>) -> Result<Vec<UsageRecord>> {
        let (start, end) = window_bounds(window, Utc::now())?;
        let state = self.lock()?;
        let mut records = state
            .records
            .iter()
            .rev()
            .filter(|record| {
                start.is_none_or(|start| record.occurred_at >= start)
                    && end.is_none_or(|end| record.occurred_at < end)
            })
            .take(maximum.unwrap_or(usize::MAX))
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            right
                .occurred_at
                .cmp(&left.occurred_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        Ok(records)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, SessionState>> {
        self.state
            .lock()
            .map_err(|_| Error::Accounting("in-memory session accounting is unavailable".into()))
    }
}

fn grouped(values: BTreeMap<String, UsageTotals>) -> Vec<GroupedUsage> {
    values
        .into_iter()
        .map(|(key, totals)| GroupedUsage { key, totals })
        .collect()
}

fn sum_cost_since(records: &[UsageRecord], start: DateTime<Utc>) -> Money {
    records
        .iter()
        .filter(|record| record.occurred_at >= start)
        .fold(Money::ZERO, |total, record| {
            total.saturating_add(record.cost.total)
        })
}

fn period_starts(now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>, DateTime<Utc>) {
    let hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("valid UTC clock-hour boundary");
    let day = Utc
        .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
        .single()
        .expect("valid UTC calendar-day boundary");
    let month = Utc
        .with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
        .single()
        .expect("valid UTC calendar-month boundary");
    (hour, day, month)
}

fn window_bounds(window: &UsageWindow, now: DateTime<Utc>) -> Result<WindowBounds> {
    let (hour, day, month) = period_starts(now);
    match window {
        UsageWindow::AllTime => Ok((None, None)),
        UsageWindow::CurrentHour => Ok((Some(hour), None)),
        UsageWindow::CurrentDay => Ok((Some(day), None)),
        UsageWindow::CurrentMonth => Ok((Some(month), None)),
        UsageWindow::Range { start, end } if start < end => Ok((Some(*start), Some(*end))),
        UsageWindow::Range { .. } => Err(Error::InvalidInput(
            "usage range end must be later than its start".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CostAccuracy, Modality, ModalityTokens};

    fn sample_usage() -> TokenUsage {
        TokenUsage {
            input_tokens: 10,
            output_tokens: 4,
            total_tokens: 14,
            input_by_modality: vec![ModalityTokens {
                modality: Modality::Text,
                tokens: 10,
            }],
            ..TokenUsage::default()
        }
    }

    fn sample_cost(nanos: u64) -> CostBreakdown {
        CostBreakdown {
            input: Money::from_usd_nanos(nanos),
            total: Money::from_usd_nanos(nanos),
            accuracy: CostAccuracy::Exact,
            ..CostBreakdown::default()
        }
    }

    #[test]
    fn limits_block_at_the_boundary() {
        let accounting = Accounting::new();
        accounting
            .set_limits(SpendingLimits {
                hourly: Some(Money::from_usd_nanos(100)),
                daily: None,
                monthly: None,
            })
            .unwrap();
        accounting
            .record(
                "infer_flash_lite",
                "gemini-3.1-flash-lite",
                ServiceTier::Standard,
                true,
                Some("interaction"),
                None,
                &sample_usage(),
                &sample_cost(100),
            )
            .unwrap();
        let error = accounting.enforce_limits(Utc::now()).unwrap_err();
        assert!(matches!(
            error,
            Error::SpendingLimitReached {
                period: LimitPeriod::Hourly,
                ..
            }
        ));
    }

    #[test]
    fn breakdown_groups_without_storing_content() {
        let accounting = Accounting::new();
        accounting
            .record(
                "infer_pro",
                "gemini-3.1-pro-preview",
                ServiceTier::Standard,
                true,
                Some("interaction"),
                None,
                &sample_usage(),
                &sample_cost(5_000),
            )
            .unwrap();
        let report = accounting.breakdown(UsageWindow::AllTime).unwrap();
        assert_eq!(report.totals.requests, 1);
        assert_eq!(report.totals.usage.input_tokens, 10);
        assert_eq!(report.totals.cost.total.usd_nanos(), 5_000);
        assert_eq!(report.by_model[0].key, "gemini-3.1-pro-preview");
        let records = accounting.usage_records(UsageWindow::AllTime, 10).unwrap();
        assert_eq!(
            records[0].provider_request_id.as_deref(),
            Some("interaction")
        );
    }

    #[test]
    fn a_new_session_has_no_records_or_limits() {
        let previous = Accounting::new();
        previous
            .set_limits(SpendingLimits {
                hourly: Some(Money::from_usd_nanos(100)),
                ..SpendingLimits::default()
            })
            .unwrap();
        previous
            .record(
                "infer_pro",
                "gemini-3.1-pro-preview",
                ServiceTier::Standard,
                true,
                None,
                None,
                &sample_usage(),
                &sample_cost(50),
            )
            .unwrap();

        let next = Accounting::new();
        assert_eq!(next.limits().unwrap(), SpendingLimits::default());
        assert!(
            next.usage_records(UsageWindow::AllTime, 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn invalid_range_is_rejected() {
        let now = Utc::now();
        assert!(
            Accounting::new()
                .breakdown(UsageWindow::Range {
                    start: now,
                    end: now,
                })
                .is_err()
        );
    }
}
