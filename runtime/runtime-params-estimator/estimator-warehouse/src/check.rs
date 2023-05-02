use crate::db::{Db, EstimationRow};
use crate::zulip::{ZulipEndpoint, ZulipReport};
use crate::Metric;
use std::collections::BTreeSet;

#[derive(clap::Parser, Debug)]
pub(crate) struct CheckConfig {
    /// Send notifications from checks to specified stream.
    /// Notifications are sent iff stream or user is set.
    #[clap(long)]
    zulip_stream: Option<String>,
    /// Send notifications from checks to specified Zulip user ID.
    /// Notifications are sent iff stream or user is set.
    #[clap(long)]
    zulip_user: Option<u64>,
    /// Checks have to be done on one specific metric.
    #[clap(long, value_enum)]
    metric: Metric,
    /// First git commit hash used for comparisons, used as base to calculate
    /// the relative changes. If left unspecified, the two commits that were
    /// inserted most recently are compared.
    #[clap(long)]
    commit_before: Option<String>,
    /// Second git commit hash used for comparisons. If left unspecified, the
    /// two commits that were inserted most recently are compared.
    #[clap(long)]
    commit_after: Option<String>,
    /// Names of estimations that should be checked. Leave empty to perform
    /// comparison on all available estimations.
    #[clap(long)]
    estimations: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Status {
    Ok = 0,
    Warn = 1,
    // Critical = 2,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Notice {
    RelativeChange(RelativeChange),
    UncertainChange(UncertainChange),
}

#[derive(Debug, PartialEq)]
pub(crate) struct RelativeChange {
    pub estimation: String,
    pub before: f64,
    pub after: f64,
}

#[derive(Debug, PartialEq)]
pub(crate) struct UncertainChange {
    pub estimation: String,
    pub before: String,
    pub after: String,
}

pub(crate) fn check(db: &Db, config: &CheckConfig) -> anyhow::Result<()> {
    let report = create_report(db, config)?;

    // This is the check command output to observe directly in the terminal.
    for change in report.changes() {
        println!("{change:?}");
    }

    let zulip_receiver = {
        if let Some(user) = config.zulip_user {
            Some(ZulipEndpoint::to_user(user)?)
        } else if let Some(stream) = &config.zulip_stream {
            Some(ZulipEndpoint::to_stream(stream.clone())?)
        } else {
            None
        }
    };

    if let Some(zulip) = zulip_receiver {
        zulip.post(&report)?;
    }
    Ok(())
}

pub(crate) fn create_report(db: &Db, config: &CheckConfig) -> anyhow::Result<ZulipReport> {
    let (commit_after, commit_before) = match (&config.commit_after, &config.commit_before) {
        (Some(a), Some(b)) => (a.clone(), b.clone()),
        (None, None) => {
            let mut commits = EstimationRow::commits_sorted_by_date(db, Some(config.metric))?;
            if commits.len() < 2 {
                anyhow::bail!("need data for at least 2 commits to perform comparison");
            }
            (commits.pop().unwrap().0, commits.pop().unwrap().0)
        }
        _ => anyhow::bail!("you have to either specify both commits for comparison or neither"),
    };
    let estimations = if !config.estimations.is_empty() {
        config.estimations.clone()
    } else {
        let rows_a = EstimationRow::select_by_commit_and_metric(db, &commit_after, config.metric)?;
        let rows_b = EstimationRow::select_by_commit_and_metric(db, &commit_before, config.metric)?;
        let estimations_a = rows_a.into_iter().map(|row| row.name).collect::<BTreeSet<_>>();
        let estimations_b = rows_b.into_iter().map(|row| row.name).collect::<BTreeSet<_>>();
        estimations_a.intersection(&estimations_b).cloned().collect()
    };
    let warnings =
        estimation_changes(db, &estimations, &commit_before, &commit_after, 0.1, config.metric)?;

    let warnings_uncertain = estimation_uncertain_changes(
        db,
        &estimations,
        &commit_before,
        &commit_after,
        config.metric,
    )?;

    let mut report = ZulipReport::new(commit_before, commit_after);
    for warning in warnings {
        report.add(warning, Status::Warn)
    }
    for warning in warnings_uncertain {
        report.add(warning, Status::Warn)
    }
    Ok(report)
}

fn estimation_changes(
    db: &Db,
    estimation_names: &[String],
    commit_before: &str,
    commit_after: &str,
    tolerance: f64,
    metric: Metric,
) -> anyhow::Result<Vec<Notice>> {
    let mut warnings = Vec::new();
    for name in estimation_names {
        let b = &EstimationRow::get(db, name, commit_before, metric)?[0];
        let a = &EstimationRow::get(db, name, commit_after, metric)?[0];
        let rel_change = (b.gas - a.gas).abs() / b.gas;
        if rel_change > tolerance {
            warnings.push(Notice::RelativeChange(RelativeChange {
                estimation: name.clone(),
                before: b.gas,
                after: a.gas,
            }))
        }
    }

    Ok(warnings)
}

fn estimation_uncertain_changes(
    db: &Db,
    estimation_names: &[String],
    commit_before: &str,
    commit_after: &str,
    metric: Metric,
) -> anyhow::Result<Vec<Notice>> {
    let mut warnings = Vec::new();
    for name in estimation_names {
        let b = EstimationRow::get(db, name, commit_before, metric)?.remove(0);
        let a = EstimationRow::get(db, name, commit_after, metric)?.remove(0);
        match (b.uncertain_reason, a.uncertain_reason) {
            (None, None) => continue,
            (Some(uncertain_before), None) => {
                add_warning(&mut warnings, name.clone(), uncertain_before, "None".to_owned())
            }
            (None, Some(uncertain_after)) => {
                add_warning(&mut warnings, name.clone(), "None".to_owned(), uncertain_after)
            }
            (Some(uncertain_before), Some(uncertain_after)) => {
                add_warning(&mut warnings, name.clone(), uncertain_before, uncertain_after);
            }
        }
    }

    Ok(warnings)
}

fn add_warning(warnings: &mut Vec<Notice>, name: String, before: String, after: String) {
    warnings.push(Notice::UncertainChange(UncertainChange { estimation: name, before, after }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn generate_test_report(input: &str, metric: Metric, estimations: &[&str]) -> ZulipReport {
        let db = Db::test_with_data(input);
        let config = CheckConfig {
            zulip_stream: None,
            zulip_user: None,
            metric,
            commit_before: None,
            commit_after: None,
            estimations: estimations.iter().map(|&s| s.to_owned()).collect(),
        };
        create_report(&db, &config).unwrap()
    }

    #[test]
    fn test_check_command() {
        let input_a = r#"
        0000a
        {"computed_in":{"nanos":800,"secs":44},"name":"LogBase","result":{"gas":1000000000.0,"instructions":8000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":809,"secs":26},"name":"LogByte","result":{"gas":1000000000.0,"instructions":8000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}

        0001a
        {"computed_in":{"nanos":814,"secs":9},"name":"LogBase","result":{"gas":2000000000.0,"instructions":16000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":694,"secs":33},"name":"LogByte","result":{"gas":1002000000.0,"instructions":8016.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}

        0002a
        {"computed_in":{"nanos":331,"secs":24},"name":"LogBase","result":{"gas":3000000000.0,"instructions":24000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":511,"secs":52},"name":"LogByte","result":{"gas":1004000000.0,"instructions":8032.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}

        0003a
        {"computed_in":{"nanos":633,"secs":7},"name":"LogBase","result":{"gas":4000000000.0,"instructions":32000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":173,"secs":2},"name":"LogByte","result":{"gas":1006000000.0,"instructions":8048.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":633,"secs":7},"name":"UncertainTest","result":{"gas":4000000000.0,"instructions":32000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":"NEGATIVE-COST"}}
        {"computed_in":{"nanos":655,"secs":56},"name":"LogByte","result":{"gas":20000000.0,"time_ns":20,"metric":"time","uncertain_reason":null}}

        0004a
        {"computed_in":{"nanos":319,"secs":19},"name":"LogBase","result":{"gas":5000000000.0,"instructions":40000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":527,"secs":15},"name":"LogByte","result":{"gas":1008000000.0,"instructions":8064.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":633,"secs":7},"name":"UncertainTest","result":{"gas":4000000000.0,"instructions":32000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":"HIGH-VARIANCE"}}
        {"computed_in":{"nanos":633,"secs":7},"name":"UncertainTest2","result":{"gas":4000000000.0,"instructions":32000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":661,"secs":11},"name":"LogByte","result":{"gas":15000000.0,"time_ns":15,"metric":"time","uncertain_reason":null}}
        {"computed_in":{"nanos":661,"secs":11},"name":"LogByte","result":{"gas":15000000.0,"time_ns":15,"metric":"time","uncertain_reason":null}}
        {"computed_in":{"nanos":0,"secs":0},"name":"AltBn128Sum","result":{"gas":0.0,"time_ns":0,"metric":"time","uncertain_reason":null}}
        {"computed_in":{"nanos":0,"secs":0},"name":"AltBn128MultiExp","result":{"gas":0.0,"time_ns":0,"metric":"time","uncertain_reason":null}}
        "#;

        // Only "LogBase" changes enough to show up in report.
        let report = generate_test_report(input_a, Metric::ICount, &[]);
        insta::assert_snapshot!(report.to_string());

        // Add more data and verify the notifications are updated.
        let input_b = input_a.to_owned()
            + r#"

        WAIT

        0000b
        {"computed_in":{"nanos":119,"secs":46},"name":"LogBase","result":{"gas":6000000000.0,"instructions":48000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":372,"secs":12},"name":"LogByte","result":{"gas":7000000000.0,"instructions":56000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":633,"secs":7},"name":"UncertainTest","result":{"gas":4000000000.0,"instructions":32000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":633,"secs":7},"name":"UncertainTest2","result":{"gas":4000000000.0,"instructions":32000.0,"io_r_bytes":0.0,"io_w_bytes":0.0,"metric":"icount","uncertain_reason":"BLOCK-MEASUREMENT-OVERHEAD"}}
        {"computed_in":{"nanos":262,"secs":15},"name":"LogByte","result":{"gas":20000000.0,"time_ns":20,"metric":"time","uncertain_reason":null}}
        {"computed_in":{"nanos":0,"secs":0},"name":"AltBn128Sum","result":{"gas":0.0,"time_ns":0,"metric":"time","uncertain_reason":null}}
        {"computed_in":{"nanos":0,"secs":0},"name":"AltBn128MultiExp","result":{"gas":10.0,"time_ns":10,"metric":"time","uncertain_reason":null}}
        "#;

        // Now both estimations have changed.
        let report = generate_test_report(&input_b, Metric::ICount, &[]);
        insta::assert_snapshot!(report.to_string());

        // Verify that filter for specific estimations works.
        let report = generate_test_report(&input_b, Metric::ICount, &["LogBase"]);
        insta::assert_snapshot!(report.to_string());

        // Filter for metric.
        let report = generate_test_report(&input_b, Metric::Time, &[]);
        insta::assert_snapshot!(report.to_string());
    }
}
