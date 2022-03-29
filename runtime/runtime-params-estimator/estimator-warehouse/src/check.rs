use std::collections::BTreeSet;

use clap::Parser;

use crate::{
    db::{Db, EstimationRow},
    zulip::{ZulipEndpoint, ZulipReport},
    Metric,
};

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
    /// Base commit for comparisons. If not specified, the latest two commits
    /// are compared.
    #[clap(long)]
    commit_a: Option<String>,
    /// Second commit for comparisons. If not specified, the latest two commits
    /// are compared.
    #[clap(long)]
    commit_b: Option<String>,
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

#[derive(Debug)]
pub(crate) enum Notice {
    RelativeChange(RelativeChange),
}

#[derive(Debug)]
pub(crate) struct RelativeChange {
    pub estimation: String,
    pub before: f64,
    pub after: f64,
}

pub(crate) fn check(db: &Db, config: &CheckConfig) -> anyhow::Result<()> {
    let (commit_a, commit_b) = match (&config.commit_a, &config.commit_b) {
        (Some(a), Some(b)) => (a.clone(), b.clone()),
        (None, None) => {
            let mut commits = EstimationRow::commits_sorted_by_date(db, Some(config.metric))?;
            if commits.len() < 2 {
                anyhow::bail!("Need data for at least 2 commits to perform comparison.");
            }
            (commits.pop().unwrap().0, commits.pop().unwrap().0)
        }
        _ => anyhow::bail!("You have to either specify both commits for comparison or neither."),
    };

    let estimations = if config.estimations.len() > 0 {
        config.estimations.clone()
    } else {
        let rows_a = EstimationRow::select_by_commit_and_metric(db, &commit_a, config.metric)?;
        let rows_b = EstimationRow::select_by_commit_and_metric(db, &commit_b, config.metric)?;
        let estimations_a = rows_a.into_iter().map(|row| row.name).collect::<BTreeSet<_>>();
        let estimations_b = rows_b.into_iter().map(|row| row.name).collect::<BTreeSet<_>>();
        estimations_a.intersection(&estimations_b).cloned().collect()
    };
    let warnings = estimation_changes(db, &estimations, &commit_a, &commit_b, 0.1, Metric::Time)?;
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
        let mut report = ZulipReport::new(commit_a, commit_b);
        warnings.into_iter().for_each(|w| report.add(w, Status::Warn));
        zulip.post(&report)?;
    }
    Ok(())
}

fn estimation_changes(
    db: &Db,
    estimation_names: &[String],
    commit_a: &str,
    commit_b: &str,
    tolerance: f64,
    metric: Metric,
) -> anyhow::Result<Vec<Notice>> {
    let mut warnings = Vec::new();
    for name in estimation_names {
        let a = &EstimationRow::get(db, name, commit_a, metric)?[0];
        let b = &EstimationRow::get(db, name, commit_b, metric)?[0];
        let rel_change = if a.gas == 0.0 { 100.0 } else { (a.gas - b.gas).abs() / a.gas };
        if rel_change > tolerance {
            warnings.push(Notice::RelativeChange(RelativeChange {
                estimation: name.clone(),
                before: a.gas,
                after: b.gas,
            }))
        }
    }

    Ok(warnings)
}
