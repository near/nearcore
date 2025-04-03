use near_primitives::transaction::SignedTransaction;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator as _, ParallelIterator};

#[derive(Clone, Copy)]
pub struct SignedValidPeriodTransactions<'a> {
    /// Transactions.
    ///
    /// Not all of them may be valid. See the other fields. Access the transactions via
    /// [`Self::iter_nonexpired_transactions`] or similar accessors, as appropriate.
    transactions: &'a [SignedTransaction],
    /// List of the transactions that are valid and should be processed by `apply`.
    ///
    /// This list is exactly the length of the corresponding `Self::transactions` field. Element at
    /// the index N in this array corresponds to an element at index N in the transactions list.
    ///
    /// Transactions for which a `false` is stored here must be ignored/dropped/skipped.
    ///
    /// All elements will be true for protocol versions where `RelaxedChunkValidation` is not
    /// enabled.
    transaction_validity_check_passed: &'a [bool],
}

impl<'a> SignedValidPeriodTransactions<'a> {
    pub fn new(transactions: &'a [SignedTransaction], validity_check_results: &'a [bool]) -> Self {
        assert_eq!(transactions.len(), validity_check_results.len());
        Self { transactions, transaction_validity_check_passed: validity_check_results }
    }

    pub fn empty() -> Self {
        Self::new(&[], &[])
    }

    pub fn iter_nonexpired_transactions(&self) -> impl Iterator<Item = &'a SignedTransaction> {
        self.transactions
            .into_iter()
            .zip(self.transaction_validity_check_passed.into_iter())
            .filter_map(|(t, v)| v.then_some(t))
    }

    pub fn par_iter_nonexpired_transactions(
        &self,
    ) -> impl ParallelIterator<Item = &'a SignedTransaction> {
        self.transactions
            .par_iter()
            .zip(self.transaction_validity_check_passed)
            .filter_map(|(t, v)| v.then_some(t))
    }

    pub fn len(&self) -> usize {
        self.transactions.len()
    }
}
