use crate::tests::network::runner::*;

#[test]
fn account_propagation() -> anyhow::Result<()> {
    let mut runner = Runner::new(3, 2);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::CheckAccountId(1, vec![0, 1]));
    runner.push(Action::AddEdge { from: 0, to: 2, force: true });
    runner.push(Action::CheckAccountId(2, vec![0, 1]));

    start_test(runner)
}
