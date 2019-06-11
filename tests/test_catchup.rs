//! Checks that late validator can catch-up and start validating.
#[test]
#[cfg(feature = "expensive_tests")]
fn test_catchup() {
    use std::time::Duration;

    use std::sync::{Arc, RwLock};
    use testlib::node::{create_nodes, Node};
    use testlib::test_helpers::{heavy_test, wait};

    /// Creates a network of `num_nodes` nodes, but starts only `num_nodes - 1`. After
    /// `num_blocks_to_wait` starts the last node and verifies that it can start validating within
    /// `catchup_timeout`.
    fn run_multiple_nodes(
        num_nodes: usize,
        num_blocks_to_wait: usize,
        catchup_timeout: Duration,
        block_generation_timeout: Duration,
        test_prefix: &str,
    ) {
        let mut nodes = create_nodes(num_nodes, test_prefix);

        let mut nodes: Vec<Arc<RwLock<dyn Node>>> =
            nodes.drain(..).map(|cfg| Node::new_sharable(cfg)).collect();

        let late_node = nodes.pop().unwrap();
        // Start all but one.
        for node in &mut nodes {
            node.write().unwrap().start();
        }

        // Wait for the blocks to be produced.
        wait(
            || {
                if let Some(ind) = nodes[0].read().unwrap().user().get_best_block_index() {
                    ind > (num_blocks_to_wait as u64)
                } else {
                    false
                }
            },
            100,
            block_generation_timeout.as_millis() as u64,
        );

        // Start the late node.
        late_node.write().unwrap().start();

        // Wait for it to have the same block index as other nodes.
        wait(
            || {
                if let ind @ Some(_) = nodes[0].read().unwrap().user().get_best_block_index() {
                    late_node.read().unwrap().user().get_best_block_index() == ind
                } else {
                    false
                }
            },
            400,
            catchup_timeout.as_millis() as u64,
        );
    }

    heavy_test(|| {
        run_multiple_nodes(4, 20, Duration::from_secs(120), Duration::from_secs(60), "4_20")
    });
}
