use near_primitives::transaction::SignedTransaction;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

pub struct SignedValidPeriodTransactions {
    /// Transactions.
    ///
    /// Not all of them may be valid. See the other fields. Access the transactions via
    /// [`Self::iter_nonexpired_transactions`] or similar accessors, as appropriate.
    transactions: Vec<SignedTransaction>,
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
    pub fn new(transactions: Vec<SignedTransaction>, validity_check_results: Vec<bool>) -> Self {
        assert_eq!(transactions.len(), validity_check_results.len());
        Self { transactions, transaction_validity_check_passed: validity_check_results }
    }

    pub fn empty() -> Self {
        Self::new(vec![], vec![])
    }

    pub fn iter_nonexpired_transactions<'a>(
        &'a self,
    ) -> impl Iterator<Item = &'a SignedTransaction> {
        self.transactions
            .iter()
            .zip(&self.transaction_validity_check_passed)
            .filter_map(|(t, v)| v.then_some(t))
    }

    pub fn into_par_iter_nonexpired_transactions(
        self,
    ) -> impl ParallelIterator<Item = SignedTransaction> {
        self.transactions
            .into_par_iter()
            .zip(self.transaction_validity_check_passed)
            .filter_map(|(t, v)| v.then_some(t))
    }

    pub fn len(&self) -> usize {
        self.transactions.len()
    }
}
