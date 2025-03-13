use std::fmt::Debug;
use std::str::FromStr;
use std::time::{Duration, Instant};

use reqwest::Client;
use tokio::time;
use tokio::time::Interval;
use tracing::info;

/// The response sent by a NEAR node for `/metrics` queries.
///
/// Structured text, containing comments prefixed by `#` and lines like
/// ```
/// metric_name{optional_specifiers} metric_value
/// ```
pub type Report<'a> = &'a str;

/// Functions required to facilitate the handling of metrics. Should be implemented by every metric
/// handled in this module.
trait Metric: Sized {
    /// Returns the name the metric has in a [`Report`].
    fn name_in_report() -> &'static str;
    /// Extracts the metric from a [`Report`].
    fn from_report(report: Report, time: Instant) -> anyhow::Result<Self>;
}

/// Counts the number of transactions a node has successfully processed since it was started.
///
/// Note that here successful refers to the conversion of a transaction to receipts. It does not
/// take the status of the receipts into account.
#[derive(PartialEq, Debug, Clone, Copy)]
struct SuccessfulTxsMetric {
    /// Successfully processed transactions since the node was started.
    num: u64,
    /// The time when the measurement was taken.
    time: Instant,
}

impl SuccessfulTxsMetric {
    #[allow(dead_code)] // so far only used in tests
    fn new(num: u64, time: Instant) -> Self {
        Self { num, time }
    }
}

impl Default for SuccessfulTxsMetric {
    fn default() -> Self {
        Self { num: 0, time: Instant::now() }
    }
}

impl Metric for SuccessfulTxsMetric {
    fn name_in_report() -> &'static str {
        "near_transaction_processed_successfully_total"
    }

    fn from_report(report: Report, time: Instant) -> anyhow::Result<Self> {
        let mut lines = report.lines();
        let line = lines.find(|line| line.starts_with(Self::name_in_report()));
        let num = if let Some(line) = line {
            parse_at_eol(line)?
        } else {
            // Absence means the node did not yet process successful transactions since it was
            // started.
            0
        };
        Ok(Self { num, time })
    }
}

/// Parses `F` on the last word in the `line`.
fn parse_at_eol<F>(line: &str) -> anyhow::Result<F>
where
    F: FromStr,
    <F as FromStr>::Err: Debug,
{
    let result = match line.rsplit_once(" ") {
        None => line.parse(),
        Some((_, last_word)) => last_word.parse(),
    };
    result.map_err(|err| anyhow::anyhow!("Failed to parse at end of line: {:?}", err))
}

/// Provides information regarding the status of transactions.
pub struct TransactionStatisticsService {
    refresh_interval: Interval,
    metrics_url: String,
    /// Data for the beginning of the observed interval.
    data_t0: SuccessfulTxsMetric,
    /// Data at time `t1`, where transaction processing was still ongoing after `t1` .
    data_t1: SuccessfulTxsMetric,
    /// Data at time `t2`. Transaction processing might have stopped in the interval `[t1, t2]`.
    data_t2: SuccessfulTxsMetric,
    http_client: Client,
}

impl TransactionStatisticsService {
    /// Construction must be followed by a call to [`Self::start`].
    pub fn new(rpc_url: String, refresh_interval: Duration) -> Self {
        let mut metrics_url = rpc_url;
        metrics_url.push_str("/metrics");

        Self {
            refresh_interval: time::interval(refresh_interval),
            metrics_url,
            data_t0: Default::default(),
            data_t1: Default::default(),
            data_t2: Default::default(),
            http_client: Client::new(),
        }
    }

    /// Measures and reports statistics related to the number of successfully processed
    /// transactions.
    ///
    /// Should be called inside a tokio task, as it waits for transaction processing to end.
    pub async fn start(mut self) -> anyhow::Result<()> {
        // Wait for transaction processing to start.
        let report = self.get_report().await?;
        let initial_count = SuccessfulTxsMetric::from_report(&report, Instant::now())?;
        let mut interval_wait_txs_start = time::interval(Duration::from_millis(100));
        info!("Waiting for transaction processing to start");
        loop {
            interval_wait_txs_start.tick().await;
            let report = self.get_report().await?;
            let metric = SuccessfulTxsMetric::from_report(&report, Instant::now())?;
            if metric.num > initial_count.num {
                self.data_t0 = metric;
                self.data_t1 = metric;
                info!("Observed successful transactions");
                break;
            }
        }

        // Measure TPS.
        // The first interval tick fires immediately, but we just started observing transactions.
        // So start measuring TPS only with the next tick, after some time passed.
        self.refresh_interval.tick().await;
        loop {
            self.refresh_interval.tick().await;
            let report = self.get_report().await?;
            let new_metric = SuccessfulTxsMetric::from_report(&report, Instant::now())?;

            if new_metric.num <= self.data_t2.num {
                // No progress since the last observation, so assuming the workload is finished.
                break;
            }

            // More transactions were processed, so update data points and print latest TPS.
            self.data_t1 = self.data_t2;
            self.data_t2 = new_metric;
            self.log_tps();
        }

        info!(
            r#"Tx statistics cut off a small time from the observation period.
            This is done to avoid statistic services slowing down the system.
            So statistics miss a few successful txs, but adjust the observation period.
            Hence for workloads running more than a few secs, TPS are representative.
        "#
        );

        Ok(())
    }

    async fn get_report(&self) -> anyhow::Result<String> {
        self.http_client.get(&self.metrics_url).send().await?.text().await.map_err(Into::into)
    }

    /// Calculates and logs successfully processed transactions per second (TPS) observed during
    /// the lifetime of `self`. Reports TPS of zero if the observation period is less than 1 second.
    fn log_tps(&self) -> u64 {
        // Using `data_t1` as transaction processing was still ongoing at `t1`.
        // Don't use `t2` as processing might have stopped before `t2`, see field doc comments.
        let elapsed_secs = (self.data_t1.time - self.data_t0.time).as_secs();
        if elapsed_secs == 0 {
            return 0;
        }
        let num = self.data_t1.num - self.data_t0.num;
        let tps = num / elapsed_secs;
        info!("Observed {num} successful txs in {elapsed_secs} seconds => {tps} TPS");
        tps
    }
}

#[cfg(test)]
mod tests {
    // cspell:words indoc
    use indoc::indoc;

    use super::*;

    #[test]
    fn test_get_metric_successful_transactions() -> anyhow::Result<()> {
        let time = Instant::now();

        let report_some = indoc! {r#"
            # HELP near_transaction_processed_successfully_total The number of transactions processed successfully since starting this node
            # TYPE near_transaction_processed_successfully_total counter
            near_transaction_processed_successfully_total 100
        "#};
        assert_eq!(
            SuccessfulTxsMetric::from_report(report_some, time)?,
            SuccessfulTxsMetric::new(100, time),
        );

        let report_zero = indoc! {r#"
            # HELP near_transaction_processed_successfully_total The number of transactions processed successfully since starting this node
            # TYPE near_transaction_processed_successfully_total counter
            near_transaction_processed_successfully_total 0
        "#};
        assert_eq!(
            SuccessfulTxsMetric::from_report(report_zero, time)?,
            SuccessfulTxsMetric::new(0, time),
        );

        let report_none = indoc! {r#"
            # A report which does not contain any metrics.
        "#};
        assert_eq!(
            SuccessfulTxsMetric::from_report(report_none, time)?,
            SuccessfulTxsMetric::new(0, time),
        );

        Ok(())
    }
}
