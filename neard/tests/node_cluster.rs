use futures::future;

use near_actix_test_utils::{run_actix_until_stop, spawn_interruptible};
use near_client::{ClientActor, ViewClientActor};
use near_primitives::types::{BlockHeight, BlockHeightDelta, NumSeats, NumShards};
use testlib::{start_nodes, test_helpers::heavy_test};

#[derive(Debug, Default)]
pub struct NodeCluster {
    dirs: Vec<tempfile::TempDir>,
    is_heavy: bool,
    num_shards: Option<NumShards>,
    num_validator_seats: Option<NumSeats>,
    num_lightclient: Option<usize>,
    epoch_length: Option<BlockHeightDelta>,
    genesis_height: Option<BlockHeight>,
}

macro_rules! add_mut_helpers {
    ($($method:ident($self:ident $(,$arg:ident:$type:ty)?) => $expr:expr),+) => {
        $(
            pub fn $method(mut $self$(, $arg: $type)?) -> Self {
                $expr;
                $self
            }
        )+
    }
}

impl NodeCluster {
    pub fn new<F: Fn(usize) -> String>(capacity: usize, gen_dirname: F) -> Self {
        Self {
            dirs: (0..capacity)
                .map(|index| {
                    tempfile::Builder::new().prefix(&gen_dirname(index)).tempdir().unwrap()
                })
                .collect(),
            ..Default::default()
        }
    }

    add_mut_helpers! {
        set_heavy_test(self) => self.is_heavy = true,
        set_shards(self, n: NumShards) => self.num_shards = Some(n),
        set_validator_seats(self, n: NumSeats) => self.num_validator_seats = Some(n),
        set_lightclient(self, n: usize) => self.num_lightclient = Some(n),
        set_epoch_length(self, l: BlockHeightDelta) => self.epoch_length = Some(l),
        set_genesis_height(self, h: BlockHeight) => self.genesis_height = Some(h)
    }

    fn exec<F, R>(self, f: F)
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
        if self.is_heavy {
            heavy_test(|| self.exec(f))
        } else {
            self.exec(f)
        }
    }
}
