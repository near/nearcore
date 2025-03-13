use std::cell::Cell;
use std::rc::Rc;

use near_async::test_loop::data::TestLoopData;
use near_primitives::types::AccountId;

use crate::setup::state::NodeExecutionData;

/// Signature of functions callable from inside the inner loop of a testloop test.
pub(crate) type LoopActionFn = Box<dyn Fn(&[NodeExecutionData], &mut TestLoopData, AccountId)>;

/// A wrapper for a callable (action) to be used inside a testloop test.
///
/// The action has two failure modes:
/// 1. It can fail during an execution of the callable (e.g. assert failure).
/// 2. The `succeeded` has never been set by the action and the testloop test is over.
///
/// The expectation is that `succeeded` would eventually be set to true by some iteration of `action_fn`.
pub(crate) struct LoopAction {
    action_fn: LoopActionFn,
    started: Cell<bool>,
    succeeded: Rc<Cell<bool>>,
}

impl LoopAction {
    /// Returns a pair of pointers to the same flag, initially set to false.
    /// To be used for a success flag that is shared between `LoopAction` and its `LoopActionFn`.
    pub fn shared_success_flag() -> (Rc<Cell<bool>>, Rc<Cell<bool>>) {
        let flag = Rc::new(Cell::new(false));
        (flag.clone(), flag)
    }
}

/// Current status for a `LoopAction`.
#[derive(Debug)]
pub enum LoopActionStatus {
    /// The action has never been called.
    NotStarted,
    /// The action has been called, but it has not set the `succeeded` flag yet.
    Started,
    /// The `succeeded` flag has been set.
    Succeeded,
}

impl LoopAction {
    /// The `succeeded` flag should be shared with `action_fn` that will update the flag at some point.
    pub fn new(action_fn: LoopActionFn, succeeded: Rc<Cell<bool>>) -> LoopAction {
        LoopAction { action_fn, started: Cell::new(false), succeeded }
    }

    /// Call the action callable with provided arguments.
    pub fn call(
        &self,
        node_datas: &[NodeExecutionData],
        test_loop_data: &mut TestLoopData,
        account_id: AccountId,
    ) {
        self.started.set(true);
        (self.action_fn)(node_datas, test_loop_data, account_id)
    }

    /// Return the current status of the loop action.
    pub fn get_status(&self) -> LoopActionStatus {
        if self.succeeded.get() {
            return LoopActionStatus::Succeeded;
        }
        if self.started.get() {
            return LoopActionStatus::Started;
        }
        LoopActionStatus::NotStarted
    }
}
