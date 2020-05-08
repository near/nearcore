use crate::cases::Metric;
use crate::testbed_runners::GasMetric;
use gnuplot::{AxesCommon, Caption, Color, DotDotDash, Figure, Graph, LineStyle, PointSymbol};
use near_vm_logic::ExtCosts;
use rand::Rng;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

type ExecutionCost = u64;

/// Stores measurements per block.
#[derive(Clone)]
#[allow(clippy::type_complexity)] // TODO: could capture large tuple as a struct
pub struct Measurements {
    data: BTreeMap<Metric, Vec<(usize, ExecutionCost, HashMap<ExtCosts, u64>)>>,
    gas_metric: GasMetric,
}

impl Measurements {
    pub fn new(gas_metric: &GasMetric) -> Self {
        Self { data: BTreeMap::new(), gas_metric: gas_metric.clone() }
    }

    pub fn record_measurement(
        &mut self,
        metric: Metric,
        block_size: usize,
        block_cost: ExecutionCost,
    ) {
        let ext_costs = node_runtime::EXT_COSTS_COUNTER
            .with(|f| f.borrow_mut().drain().collect::<HashMap<_, _>>());
        let normalized = self.normalize(block_cost);
        self.data.entry(metric).or_insert_with(Vec::new).push((block_size, normalized, ext_costs));
    }

    pub fn normalize(&self, cost: u64) -> u64 {
        match self.gas_metric {
            GasMetric::Time => cost,
            // We use factor of 8 to approximately match the price of SHA256 operation between
            // time-based and icount-based metric as measured on 3.2Ghz Core i5.
            GasMetric::ICount => cost / 8,
        }
    }

    pub fn aggregate(&self) -> BTreeMap<Metric, DataStats> {
        self.data
            .iter()
            .map(|(metric, measurements)| (*metric, DataStats::aggregate(measurements)))
            .collect()
    }

    pub fn print(&self) {
        for (metric, stats) in self.aggregate() {
            println!("{:?}\t\t\t\t{}", metric, stats);
        }
    }

    pub fn save_to_csv(&self, path: &Path) {
        let mut writer = csv::Writer::from_path(path).unwrap();
        writer.write_record(&["metric", "mean_ko", "stddev_ko", "5ile_ko", "95ile_ko"]).unwrap();
        for (metric, stats) in self.aggregate() {
            writer
                .write_record(&[
                    format!("{:?}", metric),
                    format!("{}", stats.mean / 1000),
                    format!("{}", stats.stddev / 1000),
                    format!("{}", stats.ile5 / 1000),
                    format!("{}", stats.ile95 / 1000),
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
            .set_y_label("Execution cost", &[])
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
                .map(|(block_size, block_cost, _)| {
                    (block_size as u64, ((block_cost / 1000 / block_size as u64) as u128) as u64)
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
    pub mean: ExecutionCost,
    pub stddev: ExecutionCost,
    pub ile5: ExecutionCost,
    pub ile95: ExecutionCost,
    pub ext_costs: BTreeMap<ExtCosts, f64>,
}

impl DataStats {
    #[allow(clippy::type_complexity)] // TODO: capture tuple as struct
    pub fn aggregate(un_aggregated: &[(usize, ExecutionCost, HashMap<ExtCosts, u64>)]) -> Self {
        let mut costs = un_aggregated
            .iter()
            .map(|(block_size, execution_cost, _)| {
                (*execution_cost as u128) / (*block_size as u128)
            })
            .collect::<Vec<_>>();
        costs.sort();
        let mean = (costs.iter().sum::<u128>() / (costs.len() as u128)) as i128;
        let stddev2 = costs.iter().map(|x| (*x as i128 - mean) * (*x as i128 - mean)).sum::<i128>()
            / if costs.len() > 1 { costs.len() as i128 - 1 } else { 1 };
        let stddev = (stddev2 as f64).sqrt() as u128;
        let ile5 = costs[costs.len() * 5 / 100];
        let ile95 = costs[costs.len() * 95 / 100];

        let mut ext_costs: BTreeMap<ExtCosts, f64> = BTreeMap::new();
        let mut div: BTreeMap<ExtCosts, u64> = BTreeMap::new();
        for (block_size, _, un_aggregated_ext_costs) in un_aggregated {
            for (ext_cost, count) in un_aggregated_ext_costs {
                *ext_costs.entry(*ext_cost).or_default() += *count as f64;
                *div.entry(*ext_cost).or_default() += *block_size as u64;
            }
        }
        for (k, v) in div {
            *ext_costs.get_mut(&k).unwrap() /= v as f64;
        }

        Self {
            mean: mean as u64,
            stddev: stddev as u64,
            ile5: ile5 as u64,
            ile95: ile95 as u64,
            ext_costs,
        }
    }

    /// Get mean + 4*sigma in micros
    pub fn upper(&self) -> u128 {
        (self.mean + 4u64 * self.stddev) as u128
    }
}

impl std::fmt::Display for DataStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.mean > 100 * 1000 * 1000 * 1000 {
            write!(
                f,
                "{}go±{}go ({}go, {}go)",
                self.mean / (1000 * 1000 * 1000),
                self.stddev / (1000 * 1000 * 1000),
                self.ile5 / (1000 * 1000 * 1000),
                self.ile95 / (1000 * 1000 * 1000)
            )?;
        } else if self.mean > 100 * 1000 * 1000 {
            write!(
                f,
                "{}mo±{}mo ({}mo, {}mo)",
                self.mean / (1000 * 1000),
                self.stddev / (1000 * 1000),
                self.ile5 / (1000 * 1000),
                self.ile95 / (1000 * 1000)
            )?;
        } else if self.mean > 100 * 1000 {
            write!(
                f,
                "{}ko±{}ko ({}ko, {}ko)",
                self.mean / 1000,
                self.stddev / 1000,
                self.ile5 / 1000,
                self.ile95 / 1000
            )?;
        } else {
            write!(f, "{}o±{}o ({}o, {}o)", self.mean, self.stddev, self.ile5, self.ile95)?;
        }
        for (ext_cost, cnt) in &self.ext_costs {
            write!(f, " {:?}=>{:.2}", ext_cost, cnt)?;
        }
        Ok(())
    }
}
