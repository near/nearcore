use std::collections::{HashMap, HashSet};

use crate::{GGas, Receipt, Round, ShardId, Transaction, TransactionId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReceiptId(TransactionId, usize);

/// Description of a single receipt to be created as part of a transaction.
#[derive(Clone, Debug)]
pub struct ReceiptDefinition {
    pub receiver: ShardId,
    pub size: u64,
    pub attached_gas: GGas,
    pub execution_gas: GGas,
}

/// The `TransactionBuilder` combines [`ReceiptDefinition`] objects to a DAG and
/// assigned a [`ReceiptId`] to each of them.
#[derive(Clone, Debug)]
pub struct TransactionBuilder {
    // Internal invariant: `receipts`, `outgoing` and `dependencies` have the
    // same length and can be indexed by ReceiptId.1 if ReceiptId.0 matches.
    //
    /// Unique ID of the transaction within the model execution.
    pub id: TransactionId,
    /// When the transaction is produced.
    round: Round,
    /// Where the transaction is converted to the first receipt.
    sender_shard: ShardId,
    /// Gas burnt for converting the transaction to the first receipt.
    tx_conversion_cost: GGas,
    /// All receipts that belong to the transitive receipts DAG rooted at the transaction.
    receipts: Vec<ReceiptDefinition>,
    /// Definition of directed edges of the DAG.
    outgoing: Vec<Vec<ReceiptId>>,
    /// Reverse edge index for quick access.
    dependencies: Vec<Vec<ReceiptId>>,
}

impl TransactionBuilder {
    pub fn new(id: TransactionId, sender_shard: ShardId, round: Round) -> Self {
        TransactionBuilder {
            id,
            round,
            tx_conversion_cost: 0,
            sender_shard,
            receipts: vec![],
            outgoing: vec![],
            dependencies: vec![],
        }
    }

    pub fn add_first_receipt(
        &mut self,
        def: ReceiptDefinition,
        conversion_cost: GGas,
    ) -> ReceiptId {
        assert!(
            self.receipts.is_empty(),
            "`add_first_receipt` called with receipts in transaction"
        );
        self.tx_conversion_cost = conversion_cost;
        self.add_receipt(def)
    }

    /// Create a new receipt after an existing receipt finishes.
    ///
    /// Call this multiple times to create multiple outgoing receipts.
    pub fn new_outgoing_receipt(
        &mut self,
        predecessor: ReceiptId,
        def: ReceiptDefinition,
    ) -> ReceiptId {
        self.assert_transaction_id(predecessor);
        let successor = self.add_receipt(def);
        self.outgoing[predecessor.1].push(successor);
        self.dependencies[successor.1].push(predecessor);
        successor
    }

    /// Connect two existing receipts that depend on each other.
    ///
    /// Use this to model promise dependencies.
    ///
    /// For example, two outgoing receipts might need to both resolve before the
    /// next receipt can start executing. To model this, create three outgoing
    /// receipt using [`TransactionBuilder::new_outgoing_receipt`] and then add
    /// dependencies from the first two receipts to the third.
    ///
    /// ```rust,ignore
    /// let builder = TransactionBuilder::new(todo!());
    /// let a = builder.add_first_receipt(todo!());
    /// let b = builder.new_outgoing_receipt(a, todo!());
    /// let c = builder.new_outgoing_receipt(a, todo!());
    /// let d = builder.new_outgoing_receipt(a, todo!());
    /// builder.new_dependency(b,d);
    /// builder.new_dependency(c,d);
    /// ```
    ///
    /// result in a DAG like this:
    ///      a
    ///     /|\
    ///    b | c
    ///     \|/
    ///      d
    pub fn new_dependency(&mut self, predecessor: ReceiptId, successor: ReceiptId) {
        self.assert_transaction_id(predecessor);
        self.assert_transaction_id(successor);
        self.outgoing[predecessor.1].push(successor);
        self.dependencies[successor.1].push(predecessor);
    }

    /// Unique ID of the transaction within the model execution.
    pub fn id(&self) -> TransactionId {
        self.id
    }

    /// The shard from which the transaction originates, i.e. where the
    /// transaction is converted to the first receipt.
    pub fn sender_shard(&self) -> ShardId {
        self.sender_shard
    }

    pub(crate) fn build(self) -> Transaction {
        let transaction_id = self.id();
        let initial_receipt = self.first_receipt();

        // Convert ReceiptDefinition to Receipt
        let receipts: HashMap<ReceiptId, Receipt> = self
            .receipts
            .into_iter()
            .enumerate()
            .map(|(index, receipt_def)| {
                let id = ReceiptId(transaction_id, index);
                (
                    id,
                    Receipt::new_future_receipt(
                        ReceiptId(transaction_id, index),
                        receipt_def.receiver,
                        receipt_def.size,
                        receipt_def.attached_gas,
                        receipt_def.execution_gas,
                    ),
                )
            })
            .collect();

        // Convert graph definition from vectors to hashmaps. This might be a
        // bit less efficient to work with. But it's easier and isolates
        // ReceiptId internals from the actual model execution.
        // (In the builder it made more sense to use vectors, as we needed to
        // create new ReceiptIds anyway and hence we couldn't avoid thinking
        // about how they are constructed and how uniqueness is guaranteed.)
        let outgoing = self
            .outgoing
            .into_iter()
            .enumerate()
            .map(|(index, outgoing)| {
                let receipt_id = ReceiptId(transaction_id, index);
                (receipt_id, outgoing)
            })
            .collect();
        let dependencies = self
            .dependencies
            .into_iter()
            .enumerate()
            .map(|(index, dependencies)| {
                let receipt_id = ReceiptId(transaction_id, index);
                (receipt_id, dependencies)
            })
            .collect();

        Transaction {
            id: self.id,
            submitted_at: self.round,
            sender_shard: self.sender_shard,
            initial_receipt_receiver: receipts[&initial_receipt].receiver,
            initial_receipt_gas: receipts[&initial_receipt].attached_gas,
            initial_receipt,
            tx_conversion_cost: self.tx_conversion_cost,
            outgoing,
            dependencies,
            future_receipts: receipts,
            pending_receipts: HashSet::new(),
            dropped_receipts: HashMap::new(),
            executed_receipts: HashMap::new(),
        }
    }

    /// Returns the ID of the initial receipt from converting the transaction to a receipt.
    ///
    /// Because [`TransactionBuilder::add_first_receipt`] only works on empty
    /// receipts, we can guarantee the first receipt is at index 0.
    fn first_receipt(&self) -> ReceiptId {
        ReceiptId(self.id(), 0)
    }

    /// Add a receipt to the transaction and assign a [`ReceiptId`] to it.
    fn add_receipt(&mut self, def: ReceiptDefinition) -> ReceiptId {
        let id = ReceiptId(self.id, self.receipts.len());
        self.receipts.push(def);
        self.outgoing.push(vec![]);
        self.dependencies.push(vec![]);
        id
    }

    #[track_caller]
    fn assert_transaction_id(&self, id: ReceiptId) {
        assert_eq!(id.0, self.id, "receipt belongs to a different transaction");
    }
}

impl ReceiptId {
    pub fn transaction_id(&self) -> TransactionId {
        self.0
    }
}
