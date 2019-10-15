use near_vm_logic::types::Gas;
use near_vm_logic::VMLogic;
use near_vm_errors::HostErrorOrStorageError;

type Result<T> = ::std::result::Result<T, HostErrorOrStorageError>;

pub fn promise_create(
    logic: &mut VMLogic,
    account_id: &[u8],
    amount: u128,
    gas: Gas,
) -> Result<u64> {
    let method = b"promise_create";
    let args = b"args";
    logic.promise_create(
        account_id.len() as _,
        account_id.as_ptr() as _,
        method.len() as _,
        method.as_ptr() as _,
        args.len() as _,
        args.as_ptr() as _,
        amount.to_le_bytes().as_ptr() as _,
        gas,
    )
}

pub fn promise_batch_action_function_call(
    logic: &mut VMLogic,
    promise_index: u64,
    amount: u128,
    gas: Gas,
) -> Result<()> {
    let method_id = b"promise_batch_action";
    let args = b"promise_batch_action_args";

    logic.promise_batch_action_function_call(
        promise_index,
        method_id.len() as _,
        method_id.as_ptr() as _,
        args.len() as _,
        args.as_ptr() as _,
        amount.to_le_bytes().as_ptr() as _,
        gas,
    )
}

pub fn promise_batch_action_add_key_with_function_call(
    logic: &mut VMLogic,
    promise_index: u64,
    public_key: &[u8],
    nonce: u64,
    allowance: u128,
    receiver_id: &[u8],
    method_names: &[u8],
) -> Result<()> {
    logic.promise_batch_action_add_key_with_function_call(
        promise_index,
        public_key.len() as _,
        public_key.as_ptr() as _,
        nonce,
        allowance.to_le_bytes().as_ptr() as _,
        receiver_id.len() as _,
        receiver_id.as_ptr() as _,
        method_names.len() as _,
        method_names.as_ptr() as _,
    )
}

