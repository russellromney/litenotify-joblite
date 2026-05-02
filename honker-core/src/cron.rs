//! Scheduler expression parser + next-fire calculator.
//!
//! Exposed as `honker_cron_next_after(expr, from_unix) -> next_unix` via
//! `attach_honker_functions`. The historical name sticks, but `expr` now
//! supports three forms:
//!
//!   * 5-field cron: `minute hour dom month dow`
//!   * 6-field cron: `second minute hour dom month dow`
//!   * interval expressions: `@every <n><unit>` (e.g. `@every 1s`)
//!
//! Calendar arithmetic runs in the SYSTEM LOCAL TIME ZONE — same as the
//! previous implementation and standard cron. Interval expressions are
//! deterministic second-based steps.

use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime, Timelike, Weekday};

use std::collections::BTreeSet;

#[derive(Debug, Clone)]
enum ScheduleExpr {
    Every { interval_s: i64 },
    Cron(CronSchedule),
}

impl ScheduleExpr {
    fn parse(expr: &str) -> Result<Self, String> {
        let expr = expr.trim();
        if let Some(rest) = expr.strip_prefix("@every") {
            return Ok(Self::Every {
                interval_s: parse_every_interval(rest.trim())?,
            });
        }
        Ok(Self::Cron(CronSchedule::parse(expr)?))
    }
}

/// Parsed cron expression. Each field is the set of integer values
/// that satisfy that field.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    seconds: BTreeSet<u32>,
    minutes: BTreeSet<u32>,
    hours: BTreeSet<u32>,
    days: BTreeSet<u32>,
    months: BTreeSet<u32>,
    dows: BTreeSet<u32>,
}

impl CronSchedule {
    pub fn parse(expr: &str) -> Result<Self, String> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        match parts.len() {
            5 => Ok(Self {
                seconds: BTreeSet::from([0]),
                minutes: parse_field(parts[0], 0, 59)?,
                hours: parse_field(parts[1], 0, 23)?,
                days: parse_field(parts[2], 1, 31)?,
                months: parse_field(parts[3], 1, 12)?,
                dows: parse_field(parts[4], 0, 6)?,
            }),
            6 => Ok(Self {
                seconds: parse_field(parts[0], 0, 59)?,
                minutes: parse_field(parts[1], 0, 59)?,
                hours: parse_field(parts[2], 0, 23)?,
                days: parse_field(parts[3], 1, 31)?,
                months: parse_field(parts[4], 1, 12)?,
                dows: parse_field(parts[5], 0, 6)?,
            }),
            _ => Err(format!(
                "schedule expression requires 5 or 6 cron fields, or '@every <n><unit>'; got {}: {:?}",
                parts.len(),
                expr
            )),
        }
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
        self.seconds.contains(&dt.second())
            && self.minutes.contains(&dt.minute())
            && self.hours.contains(&dt.hour())
            && self.days.contains(&(dt.day() as u32))
            && self.months.contains(&(dt.month() as u32))
            && self.dows.contains(&cron_dow)
    }
}

fn parse_every_interval(body: &str) -> Result<i64, String> {
    if body.is_empty() {
        return Err("interval expression must be '@every <n><unit>'".to_string());
    }
    let digits_len = body.chars().take_while(|c| c.is_ascii_digit()).count();
    if digits_len == 0 || digits_len == body.len() {
        return Err(format!(
            "interval expression must be '@every <n><unit>'; got {:?}",
            body
        ));
    }
    let n: i64 = body[..digits_len]
        .parse()
        .map_err(|_| format!("interval count must be an integer: {:?}", body))?;
    if n <= 0 {
        return Err(format!("interval count must be positive: {:?}", body));
    }
    let unit = &body[digits_len..];
    let mult = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 60 * 60 * 24,
        _ => {
            return Err(format!(
                "unsupported interval unit {:?}; expected one of s, m, h, d",
                unit
            ));
        }
    };
    n.checked_mul(mult)
        .ok_or_else(|| format!("interval is too large: {:?}", body))
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

fn cron_day_matches(sched: &CronSchedule, dt: &NaiveDateTime) -> bool {
    let cron_dow = match dt.weekday() {
        Weekday::Sun => 0,
        Weekday::Mon => 1,
        Weekday::Tue => 2,
        Weekday::Wed => 3,
        Weekday::Thu => 4,
        Weekday::Fri => 5,
        Weekday::Sat => 6,
    };
    sched.days.contains(&(dt.day() as u32)) && sched.dows.contains(&cron_dow)
}

fn make_dt(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|d| d.and_hms_opt(hour, minute, second))
        .expect("constructed invalid local datetime")
}

fn next_or_first(set: &BTreeSet<u32>, current: u32) -> (u32, bool) {
    if let Some(v) = set.range(current..).next() {
        (*v, false)
    } else {
        (*set.first().expect("schedule field set is empty"), true)
    }
}

fn cron_next_after_naive(
    sched: &CronSchedule,
    from: NaiveDateTime,
) -> Result<NaiveDateTime, String> {
    let mut cand = from + Duration::seconds(1);
    let cap_year = cand.year() + 100;

    while cand.year() <= cap_year {
        let month = cand.month();
        if !sched.months.contains(&month) {
            let (next_month, wrapped) = next_or_first(&sched.months, month);
            let year = if wrapped {
                cand.year() + 1
            } else {
                cand.year()
            };
            cand = make_dt(year, next_month, 1, 0, 0, 0);
            continue;
        }

        if !cron_day_matches(sched, &cand) {
            cand = make_dt(cand.year(), cand.month(), cand.day(), 0, 0, 0) + Duration::days(1);
            continue;
        }

        let hour = cand.hour();
        if !sched.hours.contains(&hour) {
            let (next_hour, wrapped) = next_or_first(&sched.hours, hour);
            if wrapped {
                cand = make_dt(cand.year(), cand.month(), cand.day(), 0, 0, 0) + Duration::days(1);
                cand = make_dt(cand.year(), cand.month(), cand.day(), next_hour, 0, 0);
            } else {
                cand = make_dt(cand.year(), cand.month(), cand.day(), next_hour, 0, 0);
            }
            continue;
        }

        let minute = cand.minute();
        if !sched.minutes.contains(&minute) {
            let (next_minute, wrapped) = next_or_first(&sched.minutes, minute);
            if wrapped {
                cand = make_dt(cand.year(), cand.month(), cand.day(), cand.hour(), 0, 0)
                    + Duration::hours(1);
                cand = make_dt(
                    cand.year(),
                    cand.month(),
                    cand.day(),
                    cand.hour(),
                    next_minute,
                    0,
                );
            } else {
                cand = make_dt(
                    cand.year(),
                    cand.month(),
                    cand.day(),
                    cand.hour(),
                    next_minute,
                    0,
                );
            }
            continue;
        }

        let second = cand.second();
        if !sched.seconds.contains(&second) {
            let (next_second, wrapped) = next_or_first(&sched.seconds, second);
            if wrapped {
                cand = make_dt(
                    cand.year(),
                    cand.month(),
                    cand.day(),
                    cand.hour(),
                    cand.minute(),
                    0,
                ) + Duration::minutes(1);
                cand = make_dt(
                    cand.year(),
                    cand.month(),
                    cand.day(),
                    cand.hour(),
                    cand.minute(),
                    next_second,
                );
            } else {
                cand = make_dt(
                    cand.year(),
                    cand.month(),
                    cand.day(),
                    cand.hour(),
                    cand.minute(),
                    next_second,
                );
            }
            continue;
        }

        if sched.matches(&cand) {
            return Ok(cand);
        }

        cand += Duration::seconds(1);
    }

    Err(format!(
        "no schedule match found within 100 years after local datetime {:?}",
        from
    ))
}

/// Return the unix timestamp of the next boundary strictly after
/// `from_unix`, in the system local time zone.
pub fn next_after_unix(expr: &str, from_unix: i64) -> Result<i64, String> {
    next_after_unix_in_tz(expr, from_unix, &Local)
}

/// TZ-parameterized variant of `next_after_unix`. Production code uses
/// `Local`; tests use fixed zones so DST edge cases are reproducible
/// regardless of the host's timezone.
pub(crate) fn next_after_unix_in_tz<Tz>(expr: &str, from_unix: i64, tz: &Tz) -> Result<i64, String>
where
    Tz: chrono::TimeZone,
{
    match ScheduleExpr::parse(expr)? {
        ScheduleExpr::Every { interval_s } => from_unix
            .checked_add(interval_s)
            .ok_or_else(|| format!("next interval overflows unix timestamp for {:?}", expr)),
        ScheduleExpr::Cron(sched) => {
            let local = match tz.timestamp_opt(from_unix, 0) {
                chrono::LocalResult::Single(t) => t,
                chrono::LocalResult::Ambiguous(t, _) => t,
                chrono::LocalResult::None => {
                    return Err(format!("invalid timestamp: {}", from_unix));
                }
            };
            let mut cand = cron_next_after_naive(&sched, local.naive_local())?;
            let cap_year = cand.year() + 100;
            while cand.year() <= cap_year {
                match tz.from_local_datetime(&cand) {
                    chrono::LocalResult::Single(t) => return Ok(t.timestamp()),
                    chrono::LocalResult::Ambiguous(t, _) => return Ok(t.timestamp()),
                    chrono::LocalResult::None => {
                        cand += Duration::seconds(1);
                    }
                }
            }
            Err(format!(
                "no valid local schedule match found within 100 years after unix_ts={}: {:?}",
                from_unix, expr
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
        Local
            .with_ymd_and_hms(y, mo, d, h, mi, s)
            .single()
            .unwrap()
            .timestamp()
    }

    #[test]
    fn star_every_minute() {
        let from = ts(2026, 4, 19, 12, 30, 0);
        let got = next_after_unix("* * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 31, 0));
    }

    #[test]
    fn every_five_minutes() {
        let from = ts(2026, 4, 19, 12, 30, 0);
        let got = next_after_unix("*/5 * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 35, 0));
    }

    #[test]
    fn six_field_seconds() {
        let from = ts(2026, 4, 19, 12, 30, 5);
        let got = next_after_unix("*/10 * * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 30, 10));
    }

    #[test]
    fn every_interval_seconds() {
        let from = ts(2026, 4, 19, 12, 30, 5);
        let got = next_after_unix("@every 1s", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 30, 6));
    }

    #[test]
    fn nightly_3am() {
        let from = ts(2026, 4, 19, 10, 0, 0);
        let got = next_after_unix("0 3 * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 20, 3, 0, 0));
    }

    #[test]
    fn strictly_after() {
        let from = ts(2026, 4, 19, 3, 0, 0);
        let got = next_after_unix("0 3 * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 20, 3, 0, 0));
    }

    #[test]
    fn range_with_step() {
        let from = ts(2026, 4, 19, 12, 5, 0);
        let got = next_after_unix("0-30/10 * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 10, 0));
    }

    #[test]
    fn comma_list() {
        let from = ts(2026, 4, 19, 12, 10, 0);
        let got = next_after_unix("0,30 * * * *", from).unwrap();
        assert_eq!(got, ts(2026, 4, 19, 12, 30, 0));
    }

    #[test]
    fn dow_filter() {
        let from = ts(2026, 4, 19, 0, 0, 0);
        let got = next_after_unix("0 12 * * 1", from).unwrap();
        assert_eq!(got, ts(2026, 4, 20, 12, 0, 0));
    }

    #[test]
    fn field_count_error() {
        assert!(next_after_unix("* * * *", 0).is_err());
        assert!(next_after_unix("* * *", 0).is_err());
    }

    #[test]
    fn interval_parse_error() {
        assert!(next_after_unix("@every", 0).is_err());
        assert!(next_after_unix("@every 0s", 0).is_err());
        assert!(next_after_unix("@every 5w", 0).is_err());
    }

    #[test]
    fn six_field_validation() {
        assert!(next_after_unix("60 * * * * *", 0).is_err());
        assert!(next_after_unix("* * 24 * * *", 0).is_err());
    }

    #[test]
    fn dst_spring_forward_gap_skips_to_real_time() {
        use chrono_tz::US::Eastern;

        let from = Eastern
            .with_ymd_and_hms(2026, 3, 8, 1, 59, 59)
            .single()
            .unwrap()
            .timestamp();
        let got = next_after_unix_in_tz("0 * * * * *", from, &Eastern).unwrap();
        let expected = Eastern
            .with_ymd_and_hms(2026, 3, 8, 3, 0, 0)
            .single()
            .unwrap()
            .timestamp();
        assert_eq!(got, expected);
    }
}
