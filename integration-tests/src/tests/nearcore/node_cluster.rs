use crate::test_helpers::heavy_test;
use actix::Addr;
use actix_rt::ArbiterHandle;
use futures::future;
use near_actix_test_utils::{run_actix, spawn_interruptible};
use near_chain_configs::Genesis;
use near_client::{ClientActor, ViewClientActor};
use near_logger_utils::init_integration_logger;
use near_network::test_utils::{convert_boot_nodes, open_port};
use near_primitives::types::{BlockHeight, BlockHeightDelta, NumSeats, NumShards};
use nearcore::{config::GenesisExt, load_test_config, start_with_config};
use tempfile::TempDir;

pub fn start_nodes(
    num_shards: NumShards,
    dirs: &[TempDir],
    num_validator_seats: NumSeats,
    num_lightclient: usize,
    epoch_length: BlockHeightDelta,
    genesis_height: BlockHeight,
) -> (Genesis, Vec<String>, Vec<(Addr<ClientActor>, Addr<ViewClientActor>, Vec<ArbiterHandle>)>) {
    init_integration_logger();

    let num_nodes = dirs.len();
    let num_tracking_nodes = num_nodes - num_lightclient;
    let seeds = (0..num_nodes).map(|i| format!("near.{}", i)).collect::<Vec<_>>();
    let mut genesis = Genesis::test_sharded_new_version(
        seeds.iter().map(|s| s.parse().unwrap()).collect(),
        num_validator_seats,
        (0..num_shards).map(|_| num_validator_seats).collect(),
    );
    genesis.config.epoch_length = epoch_length;
    genesis.config.genesis_height = genesis_height;

    let validators = (0..num_validator_seats).map(|i| format!("near.{}", i)).collect::<Vec<_>>();
    let mut near_configs = vec![];
    let first_node = open_port();
    let mut rpc_addrs = vec![];
    for i in 0..num_nodes {
        let mut near_config = load_test_config(
            if i < num_validator_seats as usize { &validators[i] } else { "" },
            if i == 0 { first_node } else { open_port() },
            genesis.clone(),
        );
        rpc_addrs.push(near_config.rpc_addr().unwrap().to_owned());
        near_config.client_config.min_num_peers = num_nodes - 1;
        if i > 0 {
            near_config.network_config.boot_nodes =
                convert_boot_nodes(vec![("near.0", first_node)]);
        }
        // if non validator, track all shards
        if i >= (num_validator_seats as usize) && i < num_tracking_nodes {
            near_config.client_config.tracked_shards = vec![0];
        }
        near_config.client_config.epoch_sync_enabled = false;
        near_configs.push(near_config);
    }

    let mut res = vec![];
    for (i, near_config) in near_configs.into_iter().enumerate() {
        let nearcore::NearNode { client, view_client, arbiters, .. } =
            start_with_config(dirs[i].path(), near_config).expect("start_with_config");
        res.push((client, view_client, arbiters))
    }
    (genesis, rpc_addrs, res)
}

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
            run_actix(async {
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
