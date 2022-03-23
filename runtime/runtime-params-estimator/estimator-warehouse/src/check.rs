use std::collections::BTreeSet;

use clap::Clap;
use reqwest::blocking::Client;

use crate::{
    db::{EstimationRow, DB},
    Metric,
};

#[derive(Clap, Debug)]
pub(crate) struct CheckConfig {
    /// Send notifications from checks to specified server.
    /// Notifications are sent iff server and stream are set.
    #[clap(long)]
    zulip_server: Option<String>,
    /// Send notifications from checks to specified stream.
    /// Notifications are sent iff server and stream are set.
    #[clap(long)]
    zulip_stream: Option<String>,
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

// impl CheckConfig {
//     pub(crate) fn new(zulip: Option<&str>, metric: Metric) -> Self {
//         let zulip = zulip.map(|s| {
//             let mut split_iter = s.split(":").map(String::from);
//             let (url, stream) =
//                 (split_iter.next().unwrap_or_default(), split_iter.next().unwrap_or_default());
//             if url.is_empty() || stream.is_empty() {
//                 panic!("Zulip parameter parsing failed. Please specify as <domain:stream>");
//             }
//             ZulipConfig { url, stream }
//         });
//         // TODO: Allow to configure commits_to_compare and estimations in CLI
//         Self { zulip, metric, commits_to_compare: None, estimations: vec![] }
//     }
// }

#[derive(Debug)]
struct Warning {
    message: String,
}

pub(crate) fn check(db: &DB, config: &CheckConfig) -> anyhow::Result<()> {
    if config.zulip_server.is_some() != config.zulip_stream.is_some() {
        anyhow::bail!("Please either specify both Zulip stream and server or neither.")
    }

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
        let estimations_a = rows_a.into_iter().map(|row| row.name);
        let estimations_b = rows_b.into_iter().map(|row| row.name);
        let mut intersection = vec![];
        let lut = estimations_a.collect::<BTreeSet<_>>();
        for b in estimations_b {
            if lut.contains(&b) {
                intersection.push(b);
            }
        }
        intersection
    };
    let warnings = estimation_changes(db, &estimations, &commit_a, &commit_b, 0.1, Metric::Time)?;
    for warning in &warnings {
        println!("{warning:?}");
    }
    // if let Some(zulip) = &config.zulip {
    //     let client = Client::new();
    //     let user = "params-estimator-bot";
    //     let pw = "TODO";
    //     let domain = &zulip.url;
    //     let url = format!("https://{user}:{pw}@{domain}/api/v1/messages");
    //     for warning in &warnings {
    //         let message = &warning.message;
    //         let topic = "test";
    //         let params =
    //             [("type", "stream"), ("to", &zulip.stream), ("topic", topic), ("content", message)];
    //         client.post(&url).form(&params).send().unwrap();
    //     }
    // }
    Ok(())
}

fn estimation_changes(
    db: &DB,
    estimation_names: &[String],
    commit_a: &str,
    commit_b: &str,
    tolerance: f64,
    metric: Metric,
) -> anyhow::Result<Vec<Warning>> {
    let mut warnings = Vec::new();
    for name in estimation_names {
        let a = &EstimationRow::get(db, name, commit_a, metric)?[0];
        let b = &EstimationRow::get(db, name, commit_b, metric)?[0];
        let rel_change = (a.gas - b.gas).abs() / a.gas;
        if rel_change > tolerance {
            warnings
                .push(Warning { message: format!("{name}: Changed by {}%", rel_change * 100.0) })
        }
    }

    Ok(warnings)
}
