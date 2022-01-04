pub use crate::tests::network::runner::*;

/// Spin up four nodes and connect them in a square.
/// Each node will have at most two connections.
/// Turn off two non adjacent nodes, and check other two nodes create
/// a connection among them.
#[test]
fn churn_attack() {
    let mut runner = Runner::new(4, 4).enable_outbound().max_num_peers(2);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::AddEdge(2, 3));
    runner.push(Action::AddEdge(3, 0));
    runner.push(Action::AddEdge(1, 2));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (3, vec![3]), (2, vec![1, 3])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (3, vec![3]), (0, vec![1, 3])]));
    runner.push(Action::Stop(1));
    runner.push(Action::Stop(3));
    runner.push(Action::CheckRoutingTable(0, vec![(2, vec![2])]));
    runner.push(Action::CheckRoutingTable(2, vec![(0, vec![0])]));

    start_test(runner);
}
