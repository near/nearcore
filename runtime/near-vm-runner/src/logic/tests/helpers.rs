use crate::logic::tests::TestVMLogic;

use crate::logic::errors::VMLogicError;
use near_primitives_core::{config::ExtCosts, types::Gas};
use std::collections::HashMap;

type Result<T> = ::std::result::Result<T, VMLogicError>;

pub(super) fn promise_create(
    logic: &mut TestVMLogic<'_>,
    account_id: &[u8],
    amount: u128,
    gas: Gas,
) -> Result<u64> {
    let account_id = logic.internal_mem_write(account_id);
    let method = logic.internal_mem_write(b"promise_create");
    let args = logic.internal_mem_write(b"args");
    let amount = logic.internal_mem_write(&amount.to_le_bytes());

    logic.promise_create(
        account_id.len,
        account_id.ptr,
        method.len,
        method.ptr,
        args.len,
        args.ptr,
        amount.ptr,
        gas,
    )
}

#[allow(dead_code)]
pub(super) fn promise_batch_create(logic: &mut TestVMLogic, account_id: &str) -> Result<u64> {
    let account_id = logic.internal_mem_write(account_id.as_bytes());
    logic.promise_batch_create(account_id.len, account_id.ptr)
}

#[allow(dead_code)]
pub(super) fn promise_batch_action_function_call(
    logic: &mut TestVMLogic<'_>,
    promise_index: u64,
    amount: u128,
    gas: Gas,
) -> Result<()> {
    promise_batch_action_function_call_ext(
        logic,
        promise_index,
        b"promise_batch_action",
        b"promise_batch_action_args",
        amount,
        gas,
    )
}

#[allow(dead_code)]
pub(super) fn promise_batch_action_function_call_ext(
    logic: &mut TestVMLogic<'_>,
    promise_index: u64,
    method_id: &[u8],
    args: &[u8],
    amount: u128,
    gas: Gas,
) -> Result<()> {
    let method_id = logic.internal_mem_write(method_id);
    let args = logic.internal_mem_write(args);
    let amount = logic.internal_mem_write(&amount.to_le_bytes());

    logic.promise_batch_action_function_call(
        promise_index,
        method_id.len,
        method_id.ptr,
        args.len,
        args.ptr,
        amount.ptr,
        gas,
    )
}

#[allow(dead_code)]
pub(super) fn promise_batch_action_function_call_weight(
    logic: &mut TestVMLogic<'_>,
    promise_index: u64,
    amount: u128,
    gas: Gas,
    weight: u64,
) -> Result<()> {
    promise_batch_action_function_call_weight_ext(
        logic,
        promise_index,
        b"promise_batch_action",
        b"promise_batch_action_args",
        amount,
        gas,
        weight,
    )
}

#[allow(dead_code)]
pub(super) fn promise_batch_action_function_call_weight_ext(
    logic: &mut TestVMLogic<'_>,
    promise_index: u64,
    method_id: &[u8],
    args: &[u8],
    amount: u128,
    gas: Gas,
    weight: u64,
) -> Result<()> {
    let method_id = logic.internal_mem_write(method_id);
    let args = logic.internal_mem_write(args);
    let amount = logic.internal_mem_write(&amount.to_le_bytes());

    logic.promise_batch_action_function_call_weight(
        promise_index,
        method_id.len,
        method_id.ptr,
        args.len,
        args.ptr,
        amount.ptr,
        gas,
        weight,
    )
}

#[allow(dead_code)]
pub(super) fn promise_batch_action_add_key_with_function_call(
    logic: &mut TestVMLogic<'_>,
    promise_index: u64,
    public_key: &[u8],
    nonce: u64,
    allowance: u128,
    receiver_id: &[u8],
    method_names: &[u8],
) -> Result<()> {
    let public_key = logic.internal_mem_write(public_key);
    let receiver_id = logic.internal_mem_write(receiver_id);
    let allowance = logic.internal_mem_write(&allowance.to_le_bytes());
    let method_names = logic.internal_mem_write(method_names);

    logic.promise_batch_action_add_key_with_function_call(
        promise_index,
        public_key.len,
        public_key.ptr,
        nonce,
        allowance.ptr,
        receiver_id.len,
        receiver_id.ptr,
        method_names.len,
        method_names.ptr,
    )
}

#[allow(dead_code)]
pub(super) fn promise_batch_action_add_key_with_full_access(
    logic: &mut TestVMLogic<'_>,
    promise_index: u64,
    public_key: &[u8],
    nonce: u64,
) -> Result<()> {
    let public_key = logic.internal_mem_write(public_key);

    logic.promise_batch_action_add_key_with_full_access(
        promise_index,
        public_key.len,
        public_key.ptr,
        nonce,
    )
}

#[macro_export]
macro_rules! map(
    { $($key:path: $value:expr,)+ } => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(
                m.insert($key, $value);
            )+
            m
        }
     };
);

pub(super) fn reset_costs_counter() {
    crate::logic::with_ext_cost_counter(|cc| cc.clear())
}

#[track_caller]
pub(super) fn assert_costs(expected: HashMap<ExtCosts, u64>) {
    crate::logic::with_ext_cost_counter(|cc| assert_eq!(*cc, expected));
    reset_costs_counter();
}
