use futures::future;

use near_actix_test_utils::{run_actix_until_stop, spawn_interruptible as spawn};
use near_client::{ClientActor, ViewClientActor};
use near_primitives::types::{BlockHeight, BlockHeightDelta, NumSeats, NumShards};
use testlib::{start_nodes, test_helpers::heavy_test};

pub enum ClusterConfigVariant {
    HeavyTest(bool),
    Shards(NumShards),
    ValidatorSeats(NumSeats),
    LightClients(usize),
    EpochLength(BlockHeightDelta),
    GenesisHeight(BlockHeight),
}

use ClusterConfigVariant::*;

#[derive(Default, Debug)]
pub struct NodeCluster {
    dirs: Vec<tempfile::TempDir>,
    is_heavy: bool,
    num_shards: Option<NumShards>,
    num_validator_seats: Option<NumSeats>,
    num_lightclient: Option<usize>,
    epoch_length: Option<BlockHeightDelta>,
    genesis_height: Option<BlockHeight>,
}

impl NodeCluster {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(mut self, config: ClusterConfigVariant) -> Self {
        match config {
            HeavyTest(is_heavy) => self.is_heavy = is_heavy,
            Shards(n) => self.num_shards = Some(n),
            ValidatorSeats(n) => self.num_validator_seats = Some(n),
            LightClients(n) => self.num_lightclient = Some(n),
            EpochLength(l) => self.epoch_length = Some(l),
            GenesisHeight(h) => self.genesis_height = Some(h),
        };
        self
    }

    pub fn mkdir<F: Fn(usize) -> String>(mut self, capacity: usize, gen_dirname: F) -> Self {
        self.dirs = Vec::with_capacity(capacity);
        self.dirs.extend(
            (0..capacity).map(|index| {
                tempfile::Builder::new().prefix(&gen_dirname(index)).tempdir().unwrap()
            }),
        );
        self
    }

    fn _exec<F, R>(self, f: F)
    where
        R: future::Future<Output = ()> + 'static,
        F: Fn(
            near_chain_configs::Genesis,
            Vec<String>,
            Vec<(
                actix::Addr<ClientActor>,
                actix::Addr<ViewClientActor>,
                Vec<actix_rt::ArbiterHandle>,
            )>,
        ) -> R,
    {
        run_actix_until_stop(async {
            assert!(
                !self.dirs.is_empty(),
                "cluster config: expected a non-zero number of directories"
            );
            let (genesis, rpc_addrs, clients) = start_nodes(
                self.num_shards.expect("cluster config: [num_shards] undefined"),
                &self.dirs,
                self.num_validator_seats.expect("cluster config: [num_validator_seats] undefined"),
                self.num_lightclient.expect("cluster config: [num_lightclient] undefined"),
                self.epoch_length.expect("cluster config: [epoch_length] undefined"),
                self.genesis_height.expect("cluster config: [genesis_height] undefined"),
            );

            spawn(f(genesis, rpc_addrs, clients));
        });
    }

    pub fn exec<F, R>(self, f: F)
    where
        R: future::Future<Output = ()> + 'static,
        F: Fn(
            near_chain_configs::Genesis,
            Vec<String>,
            Vec<(
                actix::Addr<ClientActor>,
                actix::Addr<ViewClientActor>,
                Vec<actix_rt::ArbiterHandle>,
            )>,
        ) -> R,
    {
        if self.is_heavy {
            heavy_test(|| self._exec(f))
        } else {
            self._exec(f)
        }
    }
}
