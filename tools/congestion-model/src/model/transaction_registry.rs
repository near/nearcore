use crate::{Round, ShardId, Transaction, TransactionBuilder};

#[derive(Default)]
/// Stores all transactions of a model execution and assigns TransactionIds.
pub(crate) struct TransactionRegistry {
    /// Append-only list of all transactions in the system. More are added every
    /// round.
    ///
    /// An `Option::None` value means the transaction ID has been reserved but
    /// the transaction was not built, yet.
    ///
    /// Invariant: Can be indexed by any TransactionId created for this
    /// TransactionRegistry.
    ///
    /// Note: This code could be simplified by using a `HashMap<TransactionId,
    /// Transaction>` instead. But we avoid expensive rehashing as we add more
    /// transactions. And we avoid pointer indirection by keeping the memory
    /// continuous. This optimization seems justified since there could be
    /// millions, if not billions, of transactions in a model execution.
    transactions: Vec<Option<Transaction>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TransactionId(usize);

impl TransactionRegistry {
    /// Create a new [`TransactionBuilder`] which accepts receipts to be added.
    ///
    /// The internal [`TransactionId`] is only valid once the
    /// [`TransactionBuilder`] has finished building the [`Transaction`].
    pub(crate) fn new_transaction_builder(
        &mut self,
        shard_id: ShardId,
        round: Round,
    ) -> TransactionBuilder {
        TransactionBuilder::new(self.next_id(), shard_id, round)
    }

    pub(crate) fn build_transaction(&mut self, builder: TransactionBuilder) -> TransactionId {
        let id = builder.id;
        assert!(self.transactions[id.0].is_none(), "transaction should only be built once");
        self.transactions[id.0] = Some(builder.build());
        id
    }

    fn next_id(&mut self) -> TransactionId {
        let index = self.transactions.len();
        self.transactions.push(None);
        TransactionId(index)
    }

    pub(crate) fn all_transactions(&self) -> impl Iterator<Item = &Transaction> {
        self.transactions.iter().filter_map(|id| id.as_ref())
    }
}

impl std::ops::Index<TransactionId> for TransactionRegistry {
    type Output = Transaction;

    fn index(&self, index: TransactionId) -> &Self::Output {
        // the model should always finish creating transactions before it starts executing them
        self.transactions[index.0].as_ref().expect("tried to access unfinished transaction")
    }
}

impl std::ops::IndexMut<TransactionId> for TransactionRegistry {
    fn index_mut(&mut self, index: TransactionId) -> &mut Self::Output {
        // the model should always finish creating transactions before it starts executing them
        self.transactions[index.0].as_mut().expect("tried to access unfinished transaction")
    }
}
