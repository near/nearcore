use crate::network_protocol::testonly as data;
use crate::routing;
use crate::routing::routing_table_view::*;
use crate::testonly::make_rng;
use rand::seq::SliceRandom;
use std::sync::Arc;

#[test]
fn find_route() {
    let mut rng = make_rng(385305732);
    let rng = &mut rng;

    // Create a sample NextHopTable.
    let peers: Vec<_> = (0..10).map(|_| data::make_peer_id(rng)).collect();
    let mut next_hops = routing::NextHopTable::new();
    for p in &peers {
        next_hops.insert(p.clone(), (0..3).map(|_| peers.choose(rng).cloned().unwrap()).collect());
    }
    let next_hops = Arc::new(next_hops);

    // Check that RoutingTableView always selects a valid next hop.
    let rtv = RoutingTableView::new();
    rtv.update(next_hops.clone());
    for _ in 0..1000 {
        let p = peers.choose(rng).unwrap();
        let got = rtv.find_next_hop_for_target(&p).unwrap();
        assert!(next_hops.get(p).unwrap().contains(&got));
    }
}
