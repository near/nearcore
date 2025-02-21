use crate::{GGAS, GGas, ReceiptDefinition, ShardId};

// The refund is just a Transfer. I think this it is fairly small but I didn't
// check the exact number.
const REFUND_RECEIPT_SIZE: u64 = 128;
// action_transfer.execution + action_receipt_creation.execution
const REFUND_GAS: GGas = 223 * GGAS;

pub fn refund_receipt(receiver: ShardId) -> ReceiptDefinition {
    ReceiptDefinition {
        receiver,
        size: REFUND_RECEIPT_SIZE,
        attached_gas: REFUND_GAS,
        execution_gas: REFUND_GAS,
    }
}
