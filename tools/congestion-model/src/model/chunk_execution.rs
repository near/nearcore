use super::queue_bundle::QueueBundle;
use super::transaction_registry::TransactionRegistry;
use super::BlockInfo;
use crate::model::transaction::ExecutionResult;
use crate::{GGas, Queue, QueueId, Receipt, Round, ShardId, TransactionId, GAS_LIMIT};
use std::collections::{BTreeMap, VecDeque};

/// Transient struct created once for each shard per model execution round,
/// containing all data required for chunk execution.
pub struct ChunkExecutionContext<'model> {
    queues: &'model mut QueueBundle,
    transactions: &'model mut TransactionRegistry,
    prev_block_info: &'model BTreeMap<ShardId, BlockInfo>,

    round: Round,
    shard: ShardId,
    gas_burnt: GGas,
    outgoing_receipts: Vec<Receipt>,
    block_info_output: BlockInfo,
}

impl<'model> ChunkExecutionContext<'model> {
    pub(super) fn new(
        queues: &'model mut QueueBundle,
        transactions: &'model mut TransactionRegistry,
        prev_block_info: &'model BTreeMap<ShardId, BlockInfo>,
        round: Round,
        shard_id: ShardId,
    ) -> Self {
        ChunkExecutionContext {
            queues,
            transactions,
            prev_block_info,
            round,
            shard: shard_id,
            gas_burnt: 0,
            outgoing_receipts: vec![],
            block_info_output: BlockInfo::default(),
        }
    }

    /// Mutable reference to a BlockInfo to put data inside a block and make it
    /// available to all shards for the next round.
    pub fn current_block_info(&mut self) -> &mut BlockInfo {
        &mut self.block_info_output
    }

    /// Block information from the previous round, as provided by each shard.
    pub fn prev_block_info(&self) -> &BTreeMap<ShardId, BlockInfo> {
        self.prev_block_info
    }

    /// Access the implicitly defined incoming receipts queue.
    pub fn incoming_receipts(&mut self) -> &mut Queue {
        self.queues.incoming_receipts_mut(self.shard)
    }

    /// Access a queue that was created in the init method.
    pub fn queue(&mut self, id: QueueId) -> &mut Queue {
        self.queues.queue_mut(id)
    }

    pub fn incoming_transactions(&mut self) -> &mut VecDeque<TransactionId> {
        self.queues.incoming_transactions_mut(self.shard)
    }

    pub fn gas_burnt(&self) -> GGas {
        self.gas_burnt
    }

    /// Accept a transaction and convert it to a receipt.
    pub fn accept_transaction(&mut self, tx: TransactionId) -> Receipt {
        // note: Check the total gas limit, not the TX gas limit because we want
        // to allow changes to how the chunk space is split between transactions
        // and receipts.
        assert!(
            self.gas_burnt < GAS_LIMIT,
            "trying to accept more transactions than gas limit allows",
        );
        let ExecutionResult { gas_burnt, mut new_receipts } =
            self.transactions[tx].start(self.round);
        assert_eq!(new_receipts.len(), 1, "transaction should result in exactly one receipt");

        self.gas_burnt += gas_burnt;
        new_receipts.pop().unwrap()
    }

    pub fn execute_receipt(&mut self, receipt: Receipt) -> Vec<Receipt> {
        assert!(
            self.gas_burnt < GAS_LIMIT,
            "trying to execute more than receipts than the gas limit allows",
        );
        let tx = receipt.transaction_id();
        let ExecutionResult { gas_burnt, new_receipts } =
            self.transactions[tx].execute_receipt(receipt, self.round);

        self.gas_burnt += gas_burnt;
        new_receipts
    }

    pub fn drop_receipt(&mut self, receipt: Receipt) {
        let tx = receipt.transaction_id();
        self.transactions[tx].drop_receipt(receipt, self.round);
    }

    pub fn forward_receipt(&mut self, receipt: Receipt) {
        self.outgoing_receipts.push(receipt);
    }

    /// Finalize the chunk execution and return the output to the model to
    /// integrate with the global execution context.
    pub(crate) fn finish(self) -> (Vec<Receipt>, BlockInfo) {
        (self.outgoing_receipts, self.block_info_output)
    }

    /// A sequence of increasing numbers.
    pub fn block_height(&self) -> Round {
        self.round
    }

    /// Gas cost to convert the transaction to a receipt.
    pub fn tx_conversion_gas(&self, id: TransactionId) -> GGas {
        self.transactions[id].tx_conversion_cost
    }

    /// Gas attached to the transaction after conversion.
    ///
    /// Like in real nearcore, this does not include the conversion cost. Unlike
    /// real nearcore, we are not splitting between action execution gas and
    /// attached gas.
    pub fn tx_attached_gas(&self, id: TransactionId) -> GGas {
        self.transactions[id].initial_receipt_gas()
    }

    pub fn tx_receiver(&self, id: TransactionId) -> ShardId {
        self.transactions[id].initial_receipt_receiver()
    }
}
