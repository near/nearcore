use futures::future;

use near_actix_test_utils::{run_actix_until_stop, spawn_interruptible};
use near_client::{ClientActor, ViewClientActor};
use near_primitives::types::{BlockHeight, BlockHeightDelta, NumSeats, NumShards};
use testlib::{start_nodes, test_helpers::heavy_test};

#[derive(Debug, Default)]
pub struct NodeCluster {
    dirs: Vec<tempfile::TempDir>,
    num_shards: Option<NumShards>,
    num_validator_seats: Option<NumSeats>,
    num_lightclient: Option<usize>,
    epoch_length: Option<BlockHeightDelta>,
    genesis_height: Option<BlockHeight>,
}

impl NodeCluster {
    pub fn new<F: Fn(usize) -> String>(node_count: usize, gen_dirname: F) -> Self {
        Self {
            dirs: (0..node_count)
                .map(|index| {
                    tempfile::Builder::new().prefix(&gen_dirname(index)).tempdir().unwrap()
                })
                .collect(),
            ..Default::default()
        }
    }

    pub fn set_num_shards(mut self, n: NumShards) -> Self {
        self.num_shards = Some(n);
        self
    }

    pub fn set_num_validator_seats(mut self, n: NumSeats) -> Self {
        self.num_validator_seats = Some(n);
        self
    }

    pub fn set_num_lightclients(mut self, n: usize) -> Self {
        self.num_lightclient = Some(n);
        self
    }

    pub fn set_epoch_length(mut self, l: BlockHeightDelta) -> Self {
        self.epoch_length = Some(l);
        self
    }

    pub fn set_genesis_height(mut self, h: BlockHeight) -> Self {
        self.genesis_height = Some(h);
        self
    }

    pub fn exec_until_stop<F, R>(self, f: F)
    where
        R: future::Future<Output = ()> + 'static,
        F: FnOnce(
            near_chain_configs::Genesis,
            Vec<String>,
            Vec<(
                actix::Addr<ClientActor>,
                actix::Addr<ViewClientActor>,
                Vec<actix_rt::ArbiterHandle>,
            )>,
        ) -> R,
    {
        assert!(!self.dirs.is_empty(), "cluster config: expected a non-zero number of directories");
        let (num_shards, num_validator_seats, num_lightclient, epoch_length, genesis_height) = (
            self.num_shards.expect("cluster config: [num_shards] undefined"),
            self.num_validator_seats.expect("cluster config: [num_validator_seats] undefined"),
            self.num_lightclient.expect("cluster config: [num_lightclient] undefined"),
            self.epoch_length.expect("cluster config: [epoch_length] undefined"),
            self.genesis_height.expect("cluster config: [genesis_height] undefined"),
        );
        heavy_test(|| {
            run_actix_until_stop(async {
                let (genesis, rpc_addrs, clients) = start_nodes(
                    num_shards,
                    &self.dirs,
                    num_validator_seats,
                    num_lightclient,
                    epoch_length,
                    genesis_height,
                );
                spawn_interruptible(f(genesis, rpc_addrs, clients));
            });
        });
    }
}
