use std::cell::Cell;
use std::rc::Rc;

use near_async::test_loop::data::TestLoopData;
use near_primitives::types::AccountId;

use crate::test_loop::env::TestData;

/// Signature of functions callable from inside the inner loop of a testloop test.
pub(crate) type LoopActionFn = Box<dyn Fn(&[TestData], &mut TestLoopData, AccountId)>;

/// Wraps a callable to be used inside the inner loop of a testloop test.
/// Stores a flag denoting whether the callable can be considered successful
/// if wouldn't be called again.
///
/// The flag is supposed to be updated at some point by the callable.
pub(crate) struct LoopAction {
    action_fn: LoopActionFn,
    succeeded: Rc<Cell<bool>>,
}

impl LoopAction {
    /// The `succeeded` flag should be shared with `action_fn` that will update the flag at some point.
    pub fn new(action_fn: LoopActionFn, succeeded: Rc<Cell<bool>>) -> LoopAction {
        LoopAction { action_fn, succeeded }
    }

    /// Call the action callable with provided arguments.
    pub fn call(
        &self,
        test_data: &[TestData],
        test_loop_data: &mut TestLoopData,
        account_id: AccountId,
    ) {
        (self.action_fn)(test_data, test_loop_data, account_id)
    }

    /// Whether the action can be considered successful if won't be called again.
    pub fn has_succeeded(&self) -> bool {
        self.succeeded.get()
    }
}
