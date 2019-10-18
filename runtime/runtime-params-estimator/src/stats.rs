use gnuplot::{AxesCommon, Caption, Color, DotDotDash, Figure, Graph, LineStyle, PointSymbol};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

/// A single measurement data point -- we ran a block that performed a certain operation multiple
/// times by processing multiple transactions, also a single transaction might have performed the
/// same operation multiple times.
pub struct DataPoint {
    /// The name of the metric that we are measuring.
    metric_name: &'static str,
    /// What is the block size in terms of number of transactions per block (excluding receipts from
    /// the previous blocks).
    block_size: usize,
    /// How much time did it take to process this block.
    block_duration: Duration,
    /// How many times this operation was repeated within a single transaction.
    operation_repetitions: usize,
    /// If operation is parametrized how much did we try to load this operation in bytes?
    operation_load: Option<usize>,
}

/// Stores measurements per block.
#[derive(Default)]
pub struct Measurements {
    data: Vec<DataPoint>,
}

impl Measurements {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_measurement(
        &mut self,
        metric_name: &'static str,
        block_size: usize,
        block_duration: Duration,
        operation_repetitions: usize,
        operation_load: Option<usize>,
    ) {
        self.data.push(DataPoint {
            metric_name,
            block_size,
            block_duration,
            operation_repetitions,
            operation_load,
        });
    }

    /// Groups measurements into stats by:
    /// `metric_name`, `operation_repetitions`, `operation_load`.
    pub fn group(&self) -> Vec<(&'static str, usize, Option<usize>, DataStats)> {
        let mut grouped: BTreeMap<(&'static str, usize, Option<usize>), Vec<u128>> =
            Default::default();
        for point in &self.data {
            grouped
                .entry((point.metric_name, point.operation_repetitions, point.operation_load))
                .or_insert_with(Vec::new)
                .push(point.block_duration.as_nanos() / point.block_size as u128);
        }
        grouped
            .into_iter()
            .map(|((metric_name, operation_repetitions, operation_load), v)| {
                (metric_name, operation_repetitions, operation_load, DataStats::from_nanos(v))
            })
            .collect()
    }

    pub fn print(&self) {
        println!("metrics_name\t\toperation_repetitions\t\toperation_load\t\tstats");
        for (metric_name, operation_repetitions, operation_load, stats) in self.group() {
            println!(
                "{}\t\t{}\t\t{:?}\t\t{}",
                metric_name, operation_repetitions, operation_load, stats
            );
        }
    }

    pub fn save_to_csv(&self, path: &Path) {
        let mut writer = csv::Writer::from_path(path).unwrap();
        writer
            .write_record(&[
                "metric_name",
                "operation_repetitions",
                "operation_load",
                "mean_micros",
                "stddev_micros",
                "5ile_micros",
                "95ile_micros",
            ])
            .unwrap();
        for (metric_name, operation_repetitions, operation_load, stats) in self.group() {
            writer
                .write_record(&[
                    format!("{}", metric_name),
                    format!("{}", operation_repetitions),
                    format!("{:?}", operation_load),
                    format!("{}", stats.mean.as_micros()),
                    format!("{}", stats.stddev.as_micros()),
                    format!("{}", stats.ile5.as_micros()),
                    format!("{}", stats.ile95.as_micros()),
                ])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    pub fn plot(&self, path: &Path) {
        // metric_name -> (operation_repetitions, operation_size -> [block_size -> Vec<block duration>])
        let mut grouped_by_metric: BTreeMap<
            &'static str,
            BTreeMap<(usize, Option<usize>), BTreeMap<usize, Vec<Duration>>>,
        > = Default::default();

        for point in &self.data {
            grouped_by_metric
                .entry(point.metric_name)
                .or_insert_with(Default::default)
                .entry((point.operation_repetitions, point.operation_load))
                .or_insert_with(Default::default)
                .entry(point.block_size)
                .or_insert_with(Default::default)
                .push(point.block_duration);
        }

        // Different metrics are displayed with different graph windows.

        for (metric_name, data) in grouped_by_metric {
            const COLORS: &[&str] = &["red", "orange", "green", "blue", "violet"];
            const POINTS: &[char] = &['o', 'x', '*', 's', 't', 'd', 'r'];

            let mut fg = Figure::new();
            let axes = fg
                .axes2d()
                .set_title(metric_name, &[])
                .set_legend(Graph(0.5), Graph(0.9), &[], &[])
                .set_x_label("Block size", &[])
                .set_y_label("Duration micros", &[])
                .set_grid_options(true, &[LineStyle(DotDotDash), Color("black")])
                .set_x_log(Some(2.0))
                .set_x_grid(true)
                .set_y_log(Some(2.0))
                .set_y_grid(true);

            for (i, ((operation_repetitions, operation_load), points)) in
                data.into_iter().enumerate()
            {
                let line_caption = if let Some(operation_load) = operation_load {
                    format!("{}b x {}", operation_load, operation_repetitions)
                } else {
                    format!("x {}", operation_repetitions)
                };
                let mut xs = vec![];
                let mut ys = vec![];
                let mut mean_xs = vec![];
                let mut mean_ys = vec![];
                for (block_size, durations) in points {
                    for duration in &durations {
                        xs.push(block_size as u64);
                        ys.push(duration.as_micros() as u64 / block_size as u64);
                    }
                    mean_xs.push(block_size as u64);
                    mean_ys.push(
                        durations.iter().map(|d| d.as_micros() as u64).sum::<u64>()
                            / durations.len() as u64
                            / block_size as u64,
                    );
                }
                axes.points(
                    xs.as_slice(),
                    ys.as_slice(),
                    &[Color(COLORS[i % COLORS.len()]), PointSymbol(POINTS[i % POINTS.len()])],
                )
                .lines_points(
                    mean_xs.as_slice(),
                    mean_ys.as_slice(),
                    &[
                        Color(COLORS[i % COLORS.len()]),
                        PointSymbol('.'),
                        Caption(line_caption.as_str()),
                    ],
                );
            }
            let mut buf = path.to_path_buf();
            buf.push(format!("{}.svg", metric_name));
            fg.save_to_svg(buf.to_str().unwrap(), 800, 800).unwrap();
        }
    }
}

pub struct DataStats {
    pub mean: Duration,
    pub stddev: Duration,
    pub ile5: Duration,
    pub ile95: Duration,
}

impl DataStats {
    pub fn from_nanos(mut nanos: Vec<u128>) -> Self {
        nanos.sort();
        let mean = (nanos.iter().sum::<u128>() / (nanos.len() as u128)) as i128;
        let stddev2 = nanos.iter().map(|x| (*x as i128 - mean) * (*x as i128 - mean)).sum::<i128>()
            / (nanos.len() as i128 - 1);
        let stddev = (stddev2 as f64).sqrt() as u128;
        let ile5 = nanos[nanos.len() * 5 / 100];
        let ile95 = nanos[nanos.len() * 95 / 100];

        Self {
            mean: Duration::from_nanos(mean as u64),
            stddev: Duration::from_nanos(stddev as u64),
            ile5: Duration::from_nanos(ile5 as u64),
            ile95: Duration::from_nanos(ile95 as u64),
        }
    }
}

impl std::fmt::Display for DataStats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.mean.as_secs() > 100 {
            write!(
                f,
                "{}s±{}s ({}s, {}s)",
                self.mean.as_secs(),
                self.stddev.as_secs(),
                self.ile5.as_secs(),
                self.ile95.as_secs()
            )
        } else if self.mean.as_millis() > 100 {
            write!(
                f,
                "{}ms±{}ms ({}ms, {}ms)",
                self.mean.as_millis(),
                self.stddev.as_millis(),
                self.ile5.as_millis(),
                self.ile95.as_millis()
            )
        } else if self.mean.as_micros() > 100 {
            write!(
                f,
                "{}μ±{}μ ({}μ, {}μ)",
                self.mean.as_micros(),
                self.stddev.as_micros(),
                self.ile5.as_micros(),
                self.ile95.as_micros()
            )
        } else {
            write!(
                f,
                "{}n±{}n ({}n, {}n)",
                self.mean.as_nanos(),
                self.stddev.as_nanos(),
                self.ile5.as_nanos(),
                self.ile95.as_nanos()
            )
        }
    }
}
