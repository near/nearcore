use near_primitives::transaction::TransactionEnvelope;

#[derive(Clone)]
pub struct SignedValidPeriodTransactions {
    /// Transactions.
    ///
    /// Not all of them may be valid. See the other fields. Access the transactions via
    /// [`Self::iter_nonexpired_transactions`] or similar accessors, as appropriate.
    transactions: Vec<TransactionEnvelope>,
    /// List of the transactions that are valid and should be processed by `apply`.
    ///
    /// This list is exactly the length of the corresponding `Self::transactions` field. Element at
    /// the index N in this array corresponds to an element at index N in the transactions list.
    ///
    /// Transactions for which a `false` is stored here must be ignored/dropped/skipped.
    ///
    /// All elements will be true for protocol versions where `RelaxedChunkValidation` is not
    /// enabled.
    transaction_validity_check_passed: Vec<bool>,
}

impl SignedValidPeriodTransactions {
    pub fn new<T: Into<TransactionEnvelope>>(
        transactions: Vec<T>,
        validity_check_results: Vec<bool>,
    ) -> Self {
        assert_eq!(transactions.len(), validity_check_results.len());
        Self {
            transactions: transactions.into_iter().map(Into::into).collect(),
            transaction_validity_check_passed: validity_check_results,
        }
    }

    pub fn empty() -> Self {
        Self::new(Vec::<TransactionEnvelope>::new(), vec![])
    }

    pub fn iter_nonexpired_transactions<'a>(
        &'a self,
    ) -> impl Iterator<Item = &'a TransactionEnvelope> {
        self.transactions
            .iter()
            .zip(&self.transaction_validity_check_passed)
            .filter_map(|(t, v)| v.then_some(t))
    }

    pub fn into_nonexpired_transactions(mut self) -> Vec<TransactionEnvelope> {
        let mut index = 0;
        self.transactions.retain(|_| {
            let retain = self.transaction_validity_check_passed[index];
            index += 1;
            retain
        });
        self.transactions
    }

    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    /// get references to underlying fields
    pub fn get_potentially_expired_transactions_and_expiration_flags(
        &self,
    ) -> (&[TransactionEnvelope], &[bool]) {
        (&self.transactions, &self.transaction_validity_check_passed)
    }
}
