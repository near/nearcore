use crate::setup::block_observer::BlockSource;
use crate::setup::builder::TestLoopBuilder;
use itertools::Itertools;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::BlockHeight;
use std::cell::{Cell, RefCell};
use std::ops::ControlFlow;
use std::rc::Rc;

#[test]
fn test_block_observers_run_once_per_head_height_in_registration_order() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().build();
    let calls: Rc<RefCell<Vec<(&'static str, BlockHeight)>>> = Rc::default();
    for observer_name in ["first", "second"] {
        let calls = calls.clone();
        env.on_each_block(BlockSource::Node(0), move |block| {
            calls.borrow_mut().push((observer_name, block.height()));
            ControlFlow::Continue(())
        });
    }

    let start_height = env.validator().head().height;
    env.validator_runner().run_for_number_of_blocks(3);
    // A new runner keeps each observer's last height, so the current height is not repeated.
    env.validator_runner().run_for_number_of_blocks(3);
    let end_height = env.validator().head().height;

    let calls = calls.borrow();
    let observer_names = calls.iter().map(|(observer_name, _)| *observer_name).collect_vec();
    let expected_observer_names = ["first", "second"].repeat(calls.len() / 2);
    assert_eq!(observer_names, expected_observer_names);
    let heights = calls.iter().map(|(_, height)| *height).dedup().collect_vec();
    assert!(heights.iter().tuple_windows().all(|(previous, next)| previous < next));
    assert_eq!(heights.first(), Some(&start_height));
    assert_eq!(heights.last(), Some(&end_height));
}

#[test]
fn test_block_observer_removed_after_break_leaves_the_others() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().build();
    let num_calls_of_removed_observer = Rc::new(Cell::new(0));
    let num_calls_before_break = 2;
    {
        let num_calls_of_removed_observer = num_calls_of_removed_observer.clone();
        env.on_each_block(BlockSource::Node(0), move |_block| {
            num_calls_of_removed_observer.set(num_calls_of_removed_observer.get() + 1);
            if num_calls_of_removed_observer.get() == num_calls_before_break {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
    }
    let num_calls_of_kept_observer = Rc::new(Cell::new(0));
    {
        let num_calls_of_kept_observer = num_calls_of_kept_observer.clone();
        env.on_each_block(BlockSource::Node(0), move |_block| {
            num_calls_of_kept_observer.set(num_calls_of_kept_observer.get() + 1);
            ControlFlow::Continue(())
        });
    }

    env.validator_runner().run_for_number_of_blocks(5);

    assert_eq!(num_calls_of_removed_observer.get(), num_calls_before_break);
    assert!(num_calls_of_kept_observer.get() > num_calls_before_break);
}

#[test]
fn test_block_sources_select_observed_node() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(2, 0).enable_rpc().build();
    let num_slowest_node_calls = Rc::new(Cell::new(0));
    let num_selected_node_calls = Rc::new(Cell::new(0));
    {
        let num_slowest_node_calls = num_slowest_node_calls.clone();
        env.on_each_block(BlockSource::SlowestNode, move |block| {
            let lowest_height = block.nodes.iter().map(|node| node.head().height).min().unwrap();
            let first_node_with_lowest_height =
                block.nodes.iter().find(|node| node.head().height == lowest_height).unwrap();
            assert_eq!(
                block.observed_node.node_data.account_id,
                first_node_with_lowest_height.node_data.account_id
            );
            num_slowest_node_calls.set(num_slowest_node_calls.get() + 1);
            ControlFlow::Continue(())
        });
    }
    let selected_node_index = 1;
    let selected_node_account_id = env.node_datas[selected_node_index].account_id.clone();
    {
        let num_selected_node_calls = num_selected_node_calls.clone();
        env.on_each_block(BlockSource::Node(selected_node_index), move |block| {
            assert_eq!(block.observed_node.node_data.account_id, selected_node_account_id);
            num_selected_node_calls.set(num_selected_node_calls.get() + 1);
            ControlFlow::Continue(())
        });
    }

    env.rpc_runner().run_for_number_of_blocks(5);

    assert!(num_slowest_node_calls.get() > 0);
    assert!(num_selected_node_calls.get() > 0);
}

#[test]
fn test_run_future_calls_block_observers() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().build();
    let observed_heights: Rc<RefCell<Vec<BlockHeight>>> = Rc::default();
    {
        let observed_heights = observed_heights.clone();
        env.on_each_block(BlockSource::Node(0), move |block| {
            observed_heights.borrow_mut().push(block.height());
            ControlFlow::Continue(())
        });
    }

    let clock = env.test_loop.clock();
    let sleep_duration = Duration::seconds(3);
    env.validator_runner().run_future(
        "sleep",
        async move { clock.sleep(sleep_duration).await },
        Duration::seconds(5),
    );

    assert!(observed_heights.borrow().len() > 1);
}

#[test]
fn test_direct_test_loop_run_skips_block_observers() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().build();
    let num_calls = Rc::new(Cell::new(0));
    {
        let num_calls = num_calls.clone();
        env.on_each_block(BlockSource::Node(0), move |_block| {
            num_calls.set(num_calls.get() + 1);
            ControlFlow::Continue(())
        });
    }

    env.test_loop.run_for(Duration::seconds(3));

    assert_eq!(num_calls.get(), 0);
}

struct SetFlagOnDrop(Rc<Cell<bool>>);

impl Drop for SetFlagOnDrop {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn test_env_drop_releases_block_observers() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().build();
    let observer_dropped = Rc::new(Cell::new(false));
    let set_flag_on_drop = SetFlagOnDrop(observer_dropped.clone());
    env.on_each_block(BlockSource::Node(0), move |_block| {
        let _ = &set_flag_on_drop;
        ControlFlow::Continue(())
    });
    env.validator_runner().run_for_number_of_blocks(1);

    drop(env);

    assert!(observer_dropped.get());
}
