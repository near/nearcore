use crate::cases::Metric;
use gnuplot::{AxesCommon, Caption, Color, DotDotDash, Figure, Graph, LineStyle, PointSymbol};
use near_primitives::types::Gas;
use rand::Rng;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

/// Stores measurements per block.
#[derive(Default)]
pub struct Measurements {
    data: BTreeMap<Metric, Vec<(usize, Duration)>>,
}

impl Measurements {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_measurement(
        &mut self,
        metric: Metric,
        block_size: usize,
        block_duration: Duration,
    ) {
        self.data.entry(metric).or_insert_with(Vec::new).push((block_size, block_duration));
    }

    pub fn aggregate(&self) -> BTreeMap<Metric, DataStats> {
        self.data
            .iter()
            .map(|(metric, measurements)| {
                let nanos: Vec<_> = measurements
                    .iter()
                    .map(|(block_size, block_duration)| {
                        block_duration.as_nanos() / (*block_size as u128)
                    })
                    .collect();
                (metric.clone(), DataStats::from_nanos(nanos))
            })
            .collect()
    }

    pub fn print(&self) {
        for (metric, stats) in self.aggregate() {
            println!("{:?}\t\t\t\t{}", metric, stats);
        }
    }

    pub fn save_to_csv(&self, path: &Path) {
        let mut writer = csv::Writer::from_path(path).unwrap();
        writer
            .write_record(&[
                "metric",
                "mean_micros",
                "stddev_micros",
                "5ile_micros",
                "95ile_micros",
            ])
            .unwrap();
        for (metric, stats) in self.aggregate() {
            writer
                .write_record(&[
                    format!("{:?}", metric),
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
        // Different metrics are displayed with different colors.
        let mut fg = Figure::new();
        let axes = fg
            .axes2d()
            .set_title("Metrics in micros", &[])
            .set_legend(Graph(0.5), Graph(0.9), &[], &[])
            .set_x_label("Block size", &[])
            .set_y_label("Duration micros", &[])
            .set_grid_options(true, &[LineStyle(DotDotDash), Color("black")])
            .set_x_log(Some(2.0))
            .set_x_grid(true)
            .set_y_log(Some(2.0))
            .set_y_grid(true);

        for (i, (metric, data)) in self.data.iter().enumerate() {
            const POINTS: &[char] = &['o', 'x', '*', 's', 't', 'd', 'r'];
            let marker = POINTS[i % POINTS.len()];

            let (xs, ys): (Vec<_>, Vec<_>) = data
                .iter()
                .cloned()
                .map(|(block_size, block_duration)| {
                    (block_size as u64, (block_duration.as_micros() / block_size as u128) as u64)
                })
                .unzip();

            // Aggregate per block size.
            let mut aggregate: BTreeMap<usize, Vec<u64>> = Default::default();
            for (x, y) in xs.iter().zip(ys.iter()) {
                aggregate.entry(*x as usize).or_insert_with(Vec::new).push(*y);
            }
            let (mean_xs, mean_ys): (Vec<_>, Vec<_>) = aggregate
                .into_iter()
                .map(|(x, ys)| (x, (ys.iter().sum::<u64>() as u64) / (ys.len() as u64)))
                .unzip();

            let metric_name = format!("{:?}", metric);
            let color = random_color();
            axes.points(
                xs.as_slice(),
                ys.as_slice(),
                &[Color(color.as_str()), PointSymbol(marker)],
            )
            .lines_points(
                mean_xs.as_slice(),
                mean_ys.as_slice(),
                &[Color(color.as_str()), PointSymbol('.'), Caption(metric_name.as_str())],
            );
        }
        fg.save_to_svg(path.join("metrics.svg").to_str().unwrap(), 800, 800).unwrap();
    }
}

pub fn random_color() -> String {
    let res = (0..3)
        .map(|_| {
            let b = rand::thread_rng().gen::<u8>();
            format!("{:02X}", b)
        })
        .collect::<Vec<_>>()
        .join("");
    format!("#{}", res).to_uppercase()
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

    /// Get mean + 4*sigma in micros
    pub fn sigma_micros(&self) -> u64 {
        (self.mean.as_micros() + 4 * self.stddev.as_micros()) as u64
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
