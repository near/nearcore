//! Checks that late validator can catch-up and start validating.
use std::time::Duration;

use crate::node::{create_nodes, Node};
use crate::test_helpers::{heavy_test, wait};
use std::sync::{Arc, RwLock};

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_catchup() {
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
        let nodes = create_nodes(num_nodes, test_prefix);

        let mut nodes: Vec<Arc<RwLock<dyn Node>>> =
            nodes.into_iter().map(|cfg| <dyn Node>::new_sharable(cfg)).collect();

        let late_node = nodes.pop().unwrap();
        // Start all but one.
        for node in &mut nodes {
            node.write().unwrap().start();
        }

        // Wait for the blocks to be produced.
        wait(
            || {
                if let Some(ind) = nodes[0].read().unwrap().user().get_best_height() {
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

        // Wait for it to have the same block height as other nodes.
        wait(
            || {
                if let ind @ Some(_) = nodes[0].read().unwrap().user().get_best_height() {
                    late_node.read().unwrap().user().get_best_height() == ind
                } else {
                    false
                }
            },
            400,
            catchup_timeout.as_millis() as u64,
        );
    }

    heavy_test(|| {
        // `num_min_peers` defaults to 3, and since the last node is not initially up and running, we need 5 peers total (4 to have 3 peers each launching initially + 1 launching later)
        run_multiple_nodes(5, 20, Duration::from_secs(120), Duration::from_secs(60), "4_20")
    });
}
