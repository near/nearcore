use crate::broadcast;
use crate::network_protocol::testonly as data;
use crate::peer;
use crate::peer_manager::testonly::{ActorHandler, start as start_pm};
use crate::testonly::{Rng, abort_on_panic, make_rng};
use crate::types::{NetworkInfo, PeerMessage};
use near_async::time;
use near_primitives::block::Block;
use near_primitives::network::PeerId;
use near_primitives::types::BlockHeight;
use near_store::db::TestDB;
use std::collections::HashSet;
use std::sync::Arc;

fn block_at_height(chain: &data::Chain, height: BlockHeight) -> Arc<Block> {
    let mut block = Block::clone(chain.blocks.last().unwrap());
    block.mut_header().set_height(height);
    Arc::new(block)
}

async fn connect_peer_and_send_block(
    pm: &ActorHandler,
    chain: &Arc<data::Chain>,
    rng: &mut Rng,
    clock: &time::FakeClock,
    block: Arc<Block>,
) -> peer::testonly::PeerHandle {
    let peer = pm
        .start_inbound(chain.clone(), chain.make_config(rng))
        .await
        .handshake(&clock.clock())
        .await;
    peer.send(PeerMessage::Block(block)).await;
    peer
}

async fn wait_for_network_info_with_blocks_from(
    network_info: &mut broadcast::Receiver<NetworkInfo>,
    num_peers: usize,
) -> NetworkInfo {
    network_info
        .recv_until(|info| {
            let peers_with_block = info
                .connected_peers
                .iter()
                .filter(|peer| peer.full_peer_info.chain_info.last_block.is_some())
                .count();
            (peers_with_block == num_peers).then_some(info)
        })
        .await
}

fn peer_ids(info: &NetworkInfo) -> HashSet<PeerId> {
    info.highest_height_peers.iter().map(|peer| peer.peer_info.id.clone()).collect()
}

#[tokio::test]
async fn far_ahead_minority_does_not_push_honest_peers_out_of_highest_height_peers() {
    abort_on_panic();
    let mut rng = make_rng(921853234);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let pm = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let mut network_info = pm.network_info.clone();

    let honest_height = 100;
    let far_ahead_height = honest_height + 1_000_000;
    let mut peers = vec![];
    for height in [honest_height, honest_height, honest_height, far_ahead_height, far_ahead_height]
    {
        let block = block_at_height(&chain, height);
        peers.push(connect_peer_and_send_block(&pm, &chain, rng, &clock, block).await);
    }
    let all_peer_ids: HashSet<PeerId> =
        peers.iter().map(|peer| peer.cfg.network.node_id()).collect();

    let info = wait_for_network_info_with_blocks_from(&mut network_info, peers.len()).await;
    assert_eq!(peer_ids(&info), all_peer_ids);
}

#[tokio::test]
async fn far_ahead_half_does_not_push_honest_peers_out_of_highest_height_peers() {
    abort_on_panic();
    let mut rng = make_rng(921853235);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let pm = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let mut network_info = pm.network_info.clone();

    let honest_height = 100;
    let far_ahead_height = honest_height + 1_000_000;
    let mut peers = vec![];
    for height in [honest_height, honest_height, far_ahead_height, far_ahead_height] {
        let block = block_at_height(&chain, height);
        peers.push(connect_peer_and_send_block(&pm, &chain, rng, &clock, block).await);
    }
    let all_peer_ids: HashSet<PeerId> =
        peers.iter().map(|peer| peer.cfg.network.node_id()).collect();

    let info = wait_for_network_info_with_blocks_from(&mut network_info, peers.len()).await;
    assert_eq!(peer_ids(&info), all_peer_ids);
}

#[tokio::test]
async fn peers_below_median_height_minus_horizon_leave_highest_height_peers() {
    abort_on_panic();
    let mut rng = make_rng(921853236);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let pm = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let mut network_info = pm.network_info.clone();

    let median_height = 100;
    let lowest_kept_height = median_height - pm.cfg.highest_peer_horizon;
    let mut median_peers = vec![];
    for _ in 0..3 {
        let block = block_at_height(&chain, median_height);
        median_peers.push(connect_peer_and_send_block(&pm, &chain, rng, &clock, block).await);
    }
    let edge_block = block_at_height(&chain, lowest_kept_height);
    let edge_peer = connect_peer_and_send_block(&pm, &chain, rng, &clock, edge_block).await;
    let below_block = block_at_height(&chain, lowest_kept_height - 1);
    let below_peer = connect_peer_and_send_block(&pm, &chain, rng, &clock, below_block).await;

    let info = wait_for_network_info_with_blocks_from(&mut network_info, 5).await;
    let kept_peer_ids = peer_ids(&info);
    for peer in &median_peers {
        assert!(kept_peer_ids.contains(&peer.cfg.network.node_id()));
    }
    assert!(kept_peer_ids.contains(&edge_peer.cfg.network.node_id()));
    assert!(!kept_peer_ids.contains(&below_peer.cfg.network.node_id()));
}
