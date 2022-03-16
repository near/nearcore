use near_metrics::{try_create_gauge_vec, try_create_int_gauge};
use prometheus::{GaugeVec, IntGauge};
use std::collections::HashMap;

#[derive(Default)]
/// Creates prometheus metrics on-demand for exporting RocksDB statistics.
pub(crate) struct RocksDBMetrics {
    // Contains counters and sums, which are integer statistics in RocksDB.
    int_gauges: HashMap<String, IntGauge>,
    // Contains floating point statistics, such as quantiles of timings.
    gauges: HashMap<String, GaugeVec>,
}

impl RocksDBMetrics {
    pub fn export_stats_as_metrics(&mut self, stats: &[(&str, Vec<StatsValue>)]) {
        for (stats_name, values) in stats {
            if values.len() == 1 {
                // A counter stats.
                // A statistic 'a.b.c' creates the following prometheus metric:
                // - near_a_b_c
                if let StatsValue::Count(value) = values[0] {
                    let entry = self.int_gauges.entry(stats_name.to_string());
                    entry
                        .or_insert_with(|| {
                            try_create_int_gauge(
                                &get_prometheus_metric_name(stats_name),
                                stats_name,
                            )
                            .unwrap()
                        })
                        .set(value);
                }
            } else {
                // A summary stats.
                // A statistic 'a.b.c' creates the following prometheus metrics:
                // - near_a_b_c_sum
                // - near_a_b_c_count
                // - near_a_b_c{quantile="0.95"}
                for stats_value in values {
                    match stats_value {
                        StatsValue::Count(value) => {
                            let entry =
                                self.int_gauges.entry(get_stats_summary_count_key(stats_name));
                            entry
                                .or_insert_with(|| {
                                    try_create_int_gauge(
                                        &get_metric_name_summary_count_gauge(stats_name),
                                        stats_name,
                                    )
                                    .unwrap()
                                })
                                .set(*value);
                        }
                        StatsValue::Sum(value) => {
                            let entry =
                                self.int_gauges.entry(get_stats_summary_sum_key(stats_name));
                            entry
                                .or_insert_with(|| {
                                    try_create_int_gauge(
                                        &get_metric_name_summary_sum_gauge(stats_name),
                                        stats_name,
                                    )
                                    .unwrap()
                                })
                                .set(*value);
                        }
                        StatsValue::Percentile(percentile, value) => {
                            let entry = self.gauges.entry(stats_name.to_string());
                            entry
                                .or_insert_with(|| {
                                    try_create_gauge_vec(
                                        &get_prometheus_metric_name(stats_name),
                                        stats_name,
                                        &["quantile"],
                                    )
                                    .unwrap()
                                })
                                .with_label_values(&[&format!("{:.2}", *percentile as f64 * 0.01)])
                                .set(*value);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum StatsValue {
    Count(i64),
    Sum(i64),
    Percentile(u32, f64),
}

/// Parses a string containing RocksDB statistics.
pub(crate) fn parse_statistics(
    statistics: &str,
) -> Result<Vec<(&str, Vec<StatsValue>)>, anyhow::Error> {
    let mut result = vec![];
    // Statistics are given one per line.
    for line in statistics.split('\n') {
        let mut values = vec![];
        let words: Vec<&str> = line.split(' ').collect();
        // Each line follows one of two formats:
        // 1) <stat_name> COUNT : <value>
        // 2) <stat_name> P50 : <value> P90 : <value> COUNT : <value> SUM : <value>
        // Each line gets split into words and we parse statistics according to this format.
        if words.len() > 1 {
            let stats_name = words[0];
            for i in (1..words.len()).step_by(3) {
                if words[i] == "COUNT" {
                    values.push(StatsValue::Count(
                        words[i + 2].parse::<i64>().map_err(|err| anyhow::anyhow!(err))?,
                    ));
                } else if words[i] == "SUM" {
                    values.push(StatsValue::Sum(
                        words[i + 2].parse::<i64>().map_err(|err| anyhow::anyhow!(err))?,
                    ));
                } else if words[i].starts_with("P") {
                    values.push(StatsValue::Percentile(
                        words[i][1..].parse::<u32>().map_err(|err| anyhow::anyhow!(err))?,
                        words[i + 2].parse::<f64>().map_err(|err| anyhow::anyhow!(err))?,
                    ));
                } else {
                    return Err(anyhow::anyhow!(
                        "Unsupported stats value: {} in {}",
                        words[i],
                        line
                    ));
                }
            }
            result.push((stats_name, values));
        }
    }
    Ok(result)
}

fn get_prometheus_metric_name(stats_name: &str) -> String {
    format!("near_{}", stats_name.replace(".", "_"))
}

fn get_metric_name_summary_count_gauge(stats_name: &str) -> String {
    format!("near_{}_count", stats_name.replace(".", "_"))
}

fn get_metric_name_summary_sum_gauge(stats_name: &str) -> String {
    format!("near_{}_sum", stats_name.replace(".", "_"))
}

fn get_stats_summary_count_key(stats_name: &str) -> String {
    format!("{}.count", stats_name)
}

fn get_stats_summary_sum_key(stats_name: &str) -> String {
    format!("{}.sum", stats_name)
}
