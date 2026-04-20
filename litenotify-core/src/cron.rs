//! 5-field crontab parser + next-fire calculator.
//!
//! Exposed as `jl_cron_next_after(expr, from_unix) -> next_unix` via
//! `attach_joblite_functions`. One implementation for every binding —
//! the Python `CronSchedule` class collapses to a marker holding the
//! expression string.
//!
//! Semantics match standard Unix cron (and the previous Python
//! implementation):
//!
//!   * Fields: minute (0-59), hour (0-23), day-of-month (1-31),
//!     month (1-12), day-of-week (0-6, Sunday=0).
//!   * Each field: `*`, `N`, `N-M`, `*/K`, `N-M/K`, or a comma list
//!     of those.
//!   * `next_after(dt)` returns the first boundary STRICTLY AFTER `dt`
//!     at minute precision.
//!   * Calendar arithmetic runs in the SYSTEM LOCAL TIME ZONE — same
//!     as standard cron. Set `TZ=UTC` in the scheduler's environment
//!     if you want UTC boundaries.
//!
//! Implementation strategy: naive minute-by-minute scan, capped at
//! ~5 years of iterations. Simple and good enough — a scheduler tick
//! calls this once per registered task per boundary, not in a hot
//! loop.

use chrono::{Datelike, Duration, Local, NaiveDateTime, Timelike, Weekday};

use std::collections::BTreeSet;

/// Parsed cron expression. Each field is the set of integer values
/// that satisfy that field.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    minutes: BTreeSet<u32>,
    hours: BTreeSet<u32>,
    days: BTreeSet<u32>,
    months: BTreeSet<u32>,
    dows: BTreeSet<u32>,
}

impl CronSchedule {
    pub fn parse(expr: &str) -> Result<Self, String> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(format!(
                "crontab requires 5 fields (minute hour dom month dow); got {}: {:?}",
                parts.len(),
                expr
            ));
        }
        Ok(Self {
            minutes: parse_field(parts[0], 0, 59)?,
            hours: parse_field(parts[1], 0, 23)?,
            days: parse_field(parts[2], 1, 31)?,
            months: parse_field(parts[3], 1, 12)?,
            dows: parse_field(parts[4], 0, 6)?,
        })
    }

    fn matches(&self, dt: &NaiveDateTime) -> bool {
        // chrono Weekday: Mon=0..Sun=6. Cron dow: Sun=0..Sat=6.
        let cron_dow = match dt.weekday() {
            Weekday::Sun => 0,
            Weekday::Mon => 1,
            Weekday::Tue => 2,
            Weekday::Wed => 3,
            Weekday::Thu => 4,
            Weekday::Fri => 5,
            Weekday::Sat => 6,
        };
        self.minutes.contains(&dt.minute())
            && self.hours.contains(&dt.hour())
            && self.days.contains(&(dt.day() as u32))
            && self.months.contains(&(dt.month() as u32))
            && self.dows.contains(&cron_dow)
    }
}

fn parse_field(field: &str, lo: u32, hi: u32) -> Result<BTreeSet<u32>, String> {
    let mut out = BTreeSet::new();
    for part in field.split(',') {
        let (range_part, step) = match part.split_once('/') {
            Some((r, s)) => {
                let step: u32 = s
                    .parse()
                    .map_err(|_| format!("cron step must be a positive integer: {:?}", part))?;
                if step == 0 {
                    return Err(format!("cron step must be positive: {:?}", part));
                }
                (r, step)
            }
            None => (part, 1u32),
        };
        let (start, end) = if range_part == "*" {
            (lo, hi)
        } else if let Some((a, b)) = range_part.split_once('-') {
            let a: u32 = a
                .parse()
                .map_err(|_| format!("cron field {:?} not an integer", part))?;
            let b: u32 = b
                .parse()
                .map_err(|_| format!("cron field {:?} not an integer", part))?;
            (a, b)
        } else {
            let v: u32 = range_part
                .parse()
                .map_err(|_| format!("cron field {:?} not an integer", part))?;
            (v, v)
        };
        if start < lo || end > hi || start > end {
            return Err(format!(
                "cron field {:?} out of range [{},{}] or inverted",
                part, lo, hi
            ));
        }
        let mut v = start;
        while v <= end {
            out.insert(v);
            v = v.saturating_add(step);
            if step == 0 {
                break;
            }
        }
    }
    Ok(out)
}

/// Return the unix timestamp of the next boundary strictly after
/// `from_unix`, at minute precision, in the system local time zone.
pub fn next_after_unix(expr: &str, from_unix: i64) -> Result<i64, String> {
    next_after_unix_in_tz(expr, from_unix, &Local)
}

/// TZ-parameterized variant of `next_after_unix`. Production code
/// uses `Local`; tests use a fixed zone (e.g. `chrono_tz::US::Eastern`)
/// so DST edge cases are reproducible regardless of the host's
/// timezone. `pub(crate)` because it's a testing seam, not a stable
/// API — bindings should call `next_after_unix`.
pub(crate) fn next_after_unix_in_tz<Tz>(
    expr: &str,
    from_unix: i64,
    tz: &Tz,
) -> Result<i64, String>
where
    Tz: chrono::TimeZone,
{
    let sched = CronSchedule::parse(expr)?;
    // Treat `from_unix` as a unix timestamp, project into `tz`'s
    // local wall clock, truncate to minute, then increment
    // minute-by-minute until a match. Cap at ~5 years of minutes
    // so a degenerate schedule raises instead of looping forever.
    let local = match tz.timestamp_opt(from_unix, 0) {
        chrono::LocalResult::Single(t) => t,
        chrono::LocalResult::Ambiguous(t, _) => t,
        chrono::LocalResult::None => {
            return Err(format!("invalid timestamp: {}", from_unix));
        }
    };
    let start_naive = local.naive_local().with_second(0).and_then(|d| d.with_nanosecond(0));
    let Some(start_naive) = start_naive else {
        return Err("failed to truncate to minute".to_string());
    };
    let mut cand = start_naive + Duration::minutes(1);
    let cap = 5 * 366 * 24 * 60;
    for _ in 0..cap {
        if sched.matches(&cand) {
            // Convert back to unix. Local DST ambiguity: take the
            // earlier occurrence (matches chrono's default for
            // Ambiguous); nonexistent (spring-forward gap) → skip
            // forward.
            match tz.from_local_datetime(&cand) {
                chrono::LocalResult::Single(t) => return Ok(t.timestamp()),
                chrono::LocalResult::Ambiguous(t, _) => return Ok(t.timestamp()),
                chrono::LocalResult::None => {}
            }
        }
        cand += Duration::minutes(1);
    }
    Err(format!(
        "no cron match found within 5 years after unix_ts={}: {:?}",
        from_unix, expr
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
        Local
            .with_ymd_and_hms(y, mo, d, h, mi, 0)
            .single()
            .unwrap()
            .timestamp()
    }

    #[test]
    fn star_every_minute() {
        // `* * * * *` — next minute boundary.
        let from = ts(2026, 4, 19, 12, 30);
        let got = next_after_unix("* * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 31));
    }

    #[test]
    fn every_five_minutes() {
        // 12:30 -> 12:35 on `*/5 * * * *`.
        let from = ts(2026, 4, 19, 12, 30);
        let got = next_after_unix("*/5 * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 35));
    }

    #[test]
    fn nightly_3am() {
        // `0 3 * * *` at 10am today → 3am tomorrow.
        let from = ts(2026, 4, 19, 10, 0);
        let got = next_after_unix("0 3 * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 20, 3, 0));
    }

    #[test]
    fn strictly_after() {
        // On the boundary itself → returns the NEXT boundary.
        let from = ts(2026, 4, 19, 3, 0);
        let got = next_after_unix("0 3 * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 20, 3, 0));
    }

    #[test]
    fn range_with_step() {
        // `0-30/10 * * * *` at 12:05 → 12:10.
        let from = ts(2026, 4, 19, 12, 5);
        let got = next_after_unix("0-30/10 * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 10));
    }

    #[test]
    fn comma_list() {
        // `0,30 * * * *` at 12:10 → 12:30.
        let from = ts(2026, 4, 19, 12, 10);
        let got = next_after_unix("0,30 * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 30));
    }

    #[test]
    fn dow_filter() {
        // `0 12 * * 1` (Mondays at noon). 2026-04-19 is a Sunday;
        // next Monday is 2026-04-20.
        let from = ts(2026, 4, 19, 0, 0);
        let got = next_after_unix("0 12 * * 1", from).unwrap();
        assert_eq!(got, ts(2026, 4, 20, 12, 0));
    }

    #[test]
    fn field_count_error() {
        assert!(next_after_unix("* * * *", 0).is_err());
        assert!(next_after_unix("* * * * * *", 0).is_err());
    }

    #[test]
    fn out_of_range_error() {
        assert!(next_after_unix("60 * * * *", 0).is_err());
        assert!(next_after_unix("* 24 * * *", 0).is_err());
        assert!(next_after_unix("* * 0 * *", 0).is_err());
    }

    #[test]
    fn inverted_range_error() {
        assert!(next_after_unix("5-3 * * * *", 0).is_err());
    }

    #[test]
    fn zero_step_error() {
        assert!(next_after_unix("*/0 * * * *", 0).is_err());
    }

    // ---------- DST edge cases (US/Eastern) ----------
    //
    // These use a fixed `chrono_tz::US::Eastern` so the behavior is
    // reproducible regardless of where the test runs. The production
    // `next_after_unix` uses `Local`, which reads TZ env var +
    // /etc/localtime — great for users, terrible for portable tests.

    use chrono_tz::US::Eastern;

    /// Spring-forward: on 2024-03-10 in US/Eastern, 2am does not
    /// exist locally (clock jumps 1:59 EST → 3:00 EDT). A cron like
    /// `30 2 * * *` has no valid boundary that day. We expect
    /// `next_after_unix_in_tz` to SKIP it and return 2:30am on the
    /// next day. Previously, a naive implementation could return a
    /// bogus timestamp from `from_local_datetime` on a nonexistent
    /// local time.
    #[test]
    fn dst_spring_forward_skips_nonexistent_hour() {
        // 2024-03-10 00:00 EST (before spring-forward). EST = UTC-5.
        // 00:00 EST = 05:00 UTC = 1710046800.
        let from = Eastern
            .with_ymd_and_hms(2024, 3, 10, 0, 0, 0)
            .single()
            .unwrap()
            .timestamp();
        let got = next_after_unix_in_tz("30 2 * * *", from, &Eastern).unwrap();
        // Expected: 2024-03-11 02:30 EDT (EDT = UTC-4).
        let want = Eastern
            .with_ymd_and_hms(2024, 3, 11, 2, 30, 0)
            .single()
            .unwrap()
            .timestamp();
        assert_eq!(
            got, want,
            "spring-forward day's 2:30 should be skipped, next fire is tomorrow's 2:30"
        );
    }

    /// Fall-back: on 2024-11-03 in US/Eastern, 1am happens twice
    /// (once at 1:00 EDT, once at 1:00 EST). A cron like
    /// `30 1 * * *` should fire exactly ONCE per calendar day — our
    /// naive-wall-clock walk naturally picks the earlier (EDT)
    /// occurrence and advances past it without revisiting. The
    /// second 1:30 EST (same local wall clock, different UTC) is
    /// skipped.
    #[test]
    fn dst_fall_back_fires_once_not_twice() {
        // 2024-11-03 00:00 EDT (before fall-back). EDT = UTC-4.
        // 00:00 EDT = 04:00 UTC.
        let from = Eastern
            .with_ymd_and_hms(2024, 11, 3, 0, 0, 0)
            .earliest()
            .unwrap()
            .timestamp();

        // First call: next 1:30 is 1:30 EDT (the earlier of the two
        // ambiguous 1:30s that day).
        let first = next_after_unix_in_tz("30 1 * * *", from, &Eastern).unwrap();
        let want_first = Eastern
            .with_ymd_and_hms(2024, 11, 3, 1, 30, 0)
            .earliest() // pre-fall-back: 1:30 EDT (UTC-4)
            .unwrap()
            .timestamp();
        assert_eq!(first, want_first, "first 1:30 on fall-back day is EDT");

        // Second call: must NOT return the other 1:30 (EST, one hour
        // later in UTC). Instead, next day's 1:30 EST.
        let second = next_after_unix_in_tz("30 1 * * *", first, &Eastern).unwrap();
        let would_be_duplicate = Eastern
            .with_ymd_and_hms(2024, 11, 3, 1, 30, 0)
            .latest() // post-fall-back: 1:30 EST (UTC-5)
            .unwrap()
            .timestamp();
        assert_ne!(
            second, would_be_duplicate,
            "second 1:30 (EST) must not re-fire same calendar day"
        );
        let want_next = Eastern
            .with_ymd_and_hms(2024, 11, 4, 1, 30, 0)
            .single()
            .unwrap()
            .timestamp();
        assert_eq!(second, want_next, "next fire is 1:30 the following day");
    }
}
