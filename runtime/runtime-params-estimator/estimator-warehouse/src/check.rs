use crate::db::{Db, EstimationRow};
use crate::zulip::{ZulipEndpoint, ZulipReport};
use crate::Metric;
use clap::Parser;
use std::collections::BTreeSet;

#[derive(Parser, Debug)]
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
    #[clap(long, arg_enum)]
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
}

#[derive(Debug, PartialEq)]
pub(crate) struct RelativeChange {
    pub estimation: String,
    pub before: f64,
    pub after: f64,
}

pub(crate) fn check(db: &Db, config: &CheckConfig) -> anyhow::Result<()> {
    let (commit_before, commit_after, warnings) = inner_check(config, db)?;
    for warning in &warnings {
        println!("{warning:?}");
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
        let mut report = ZulipReport::new(commit_before, commit_after);
        warnings.into_iter().for_each(|w| report.add(w, Status::Warn));
        zulip.post(&report)?;
    }
    Ok(())
}

fn inner_check(
    config: &CheckConfig,
    db: &Db,
) -> Result<(String, String, Vec<Notice>), anyhow::Error> {
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
    let estimations = if config.estimations.len() > 0 {
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
    Ok((commit_before, commit_after, warnings))
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
        let rel_change = if b.gas == 0.0 { 1.0 } else { (b.gas - a.gas).abs() / b.gas };
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

#[cfg(test)]
mod tests {
    use super::{inner_check, CheckConfig, Notice, RelativeChange};
    use crate::db::{Db, EstimationRow};
    use crate::Metric;

    #[track_caller]
    fn check_estimation_changes(
        db: &Db,
        estimation_names: impl Iterator<Item = &'static str>,
        commit_before: &str,
        commit_after: &str,
        tolerance: f64,
        metric: Metric,
        expected: Vec<Notice>,
    ) {
        let output = super::estimation_changes(
            &db,
            &estimation_names.map(|s| s.to_owned()).collect::<Vec<_>>(),
            commit_before,
            commit_after,
            tolerance,
            metric,
        )
        .unwrap();
        assert_eq!(expected, output);
    }

    #[test]
    fn test_estimation_changes() {
        let db = Db::test();
        // Data with large difference between commits
        EstimationRow::insert_test_data_set(&db, "LogBase", 1e9, 1e9, Metric::ICount, 3);
        // Data with virtually no difference between commits
        EstimationRow::insert_test_data_set(&db, "LogByte", 1e9, 1.0, Metric::ICount, 3);
        EstimationRow::insert_test_data_set(&db, "LogBase", 2e9, 0.0, Metric::Time, 3);
        EstimationRow::insert_test_data_set(&db, "LogByte", 2e9, 0.0, Metric::Time, 3);

        let expected_icount_notice = Notice::RelativeChange(RelativeChange {
            estimation: "LogBase".to_owned(),
            before: 2e9,
            after: 3e9,
        });

        // The difference is exactly 50%, therefore 49.9% is breached.
        check_estimation_changes(
            &db,
            ["LogBase", "LogByte"].into_iter(),
            "0001beef",
            "0002beef",
            0.499,
            Metric::ICount,
            vec![expected_icount_notice],
        );

        // Tolerate 50% and check there is no notice.
        check_estimation_changes(
            &db,
            ["LogBase", "LogByte"].into_iter(),
            "0001beef",
            "0002beef",
            0.5,
            Metric::ICount,
            vec![],
        );

        // Time based estimation has no gas difference, thus no notice.
        check_estimation_changes(
            &db,
            ["LogBase", "LogByte"].into_iter(),
            "0001beef",
            "0002beef",
            0.001,
            Metric::Time,
            vec![],
        );
    }

    /// Checker function for tests involving the check command
    #[track_caller]
    fn check_check_command(
        db: &Db,
        metric: Metric,
        checked_estimations: &[&str],
        expected_notices: &[Notice],
        expected_commit_before: &str,
        expected_commit_after: &str,
    ) {
        let config = CheckConfig {
            zulip_stream: None,
            zulip_user: None,
            metric,
            commit_before: None,
            commit_after: None,
            estimations: checked_estimations.iter().map(|&s| s.to_owned()).collect(),
        };

        let (commit_before, commit_after, notices) = inner_check(&config, db).unwrap();
        assert_eq!(expected_commit_before, commit_before);
        assert_eq!(expected_commit_after, commit_after);
        assert_eq!(expected_notices, &notices);
    }

    #[test]
    fn test_check_command() {
        let db = Db::test();
        // Data with large difference between commits
        EstimationRow::insert_test_data_set_ex(&db, "LogBase", 1e9, 1e9, Metric::ICount, 5, "a", 0);
        // Data with insignificant difference between commits
        EstimationRow::insert_test_data_set_ex(&db, "LogByte", 1e9, 2e6, Metric::ICount, 5, "a", 0);

        check_check_command(
            &db,
            Metric::ICount,
            &["LogBase", "LogByte"],
            &[Notice::RelativeChange(RelativeChange {
                estimation: "LogBase".to_owned(),
                before: 4e9,
                after: 5e9,
            })],
            "0003a",
            "0004a",
        );

        // Add more data and verify the notifications are updated
        EstimationRow::insert_test_data_set_ex(&db, "LogBase", 6e9, 0., Metric::ICount, 1, "b", 10);
        EstimationRow::insert_test_data_set_ex(&db, "LogByte", 7e9, 0., Metric::ICount, 1, "b", 10);

        check_check_command(
            &db,
            Metric::ICount,
            &["LogBase", "LogByte"],
            &[
                Notice::RelativeChange(RelativeChange {
                    estimation: "LogBase".to_owned(),
                    before: 5e9,
                    after: 6e9,
                }),
                Notice::RelativeChange(RelativeChange {
                    estimation: "LogByte".to_owned(),
                    before: 1.008e9,
                    after: 7e9,
                }),
            ],
            "0004a",
            "0000b",
        );

        // Verify that filter for specific estimations also works
        check_check_command(
            &db,
            Metric::ICount,
            &["LogBase"],
            &[Notice::RelativeChange(RelativeChange {
                estimation: "LogBase".to_owned(),
                before: 5e9,
                after: 6e9,
            })],
            "0004a",
            "0000b",
        );
    }
}
