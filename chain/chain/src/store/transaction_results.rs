//! Transaction and receipt results assembled from the stored execution outcomes.

use super::{ChainStore, ChainStoreAccess};
use crate::Error;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::transaction::ExecutionOutcomeWithIdAndProof;
use near_primitives::types::BlockHeight;
use near_primitives::views::{
    EccTransactionView, ExecutionOutcomeWithIdView, ExecutionStatusView, FinalExecutionOutcomeView,
    FinalExecutionOutcomeWithReceiptView, FinalExecutionStatus, TransactionEnvelopeView,
};
use std::sync::Arc;

impl ChainStore {
    /// Returns if given block header is on the current chain.
    ///
    /// This is done by fetching header by height and checking that it's the
    /// same one as provided.
    pub fn is_on_current_chain(&self, header: &BlockHeader) -> Result<bool, Error> {
        let chain_header = self.get_block_header_by_height(header.height())?;
        Ok(chain_header.hash() == header.hash())
    }

    /// Returns execution status based on the list of currently existing outcomes
    fn get_execution_status(
        &self,
        outcomes: &[ExecutionOutcomeWithIdView],
        transaction_hash: &CryptoHash,
    ) -> FinalExecutionStatus {
        if outcomes.is_empty() {
            return FinalExecutionStatus::NotStarted;
        }
        let mut looking_for_id = *transaction_hash;
        let num_outcomes = outcomes.len();
        outcomes
            .iter()
            .find_map(|outcome_with_id| {
                if outcome_with_id.id == looking_for_id {
                    match &outcome_with_id.outcome.status {
                        ExecutionStatusView::Unknown if num_outcomes == 1 => {
                            Some(FinalExecutionStatus::NotStarted)
                        }
                        ExecutionStatusView::Unknown => Some(FinalExecutionStatus::Started),
                        ExecutionStatusView::Failure(e) => {
                            Some(FinalExecutionStatus::Failure(e.clone()))
                        }
                        ExecutionStatusView::SuccessValue(v) => {
                            Some(FinalExecutionStatus::SuccessValue(v.clone()))
                        }
                        ExecutionStatusView::SuccessReceiptId(id) => {
                            looking_for_id = *id;
                            None
                        }
                    }
                } else {
                    None
                }
            })
            .unwrap_or(FinalExecutionStatus::Started)
    }

    /// Collect all the execution outcomes existing at the current moment
    /// Fails if there are non executed receipts, and require_all_outcomes == true
    fn get_recursive_transaction_results(
        &self,
        outcomes: &mut Vec<ExecutionOutcomeWithIdView>,
        id: &CryptoHash,
        require_all_outcomes: bool,
    ) -> Result<(), Error> {
        let outcome = match self.get_execution_outcome(id) {
            Ok(outcome) => outcome,
            Err(err) => return if require_all_outcomes { Err(err) } else { Ok(()) },
        };
        outcomes.push(ExecutionOutcomeWithIdView::from(outcome));
        let outcome_idx = outcomes.len() - 1;
        for idx in 0..outcomes[outcome_idx].outcome.receipt_ids.len() {
            let id = outcomes[outcome_idx].outcome.receipt_ids[idx];
            self.get_recursive_transaction_results(outcomes, &id, require_all_outcomes)?;
        }
        Ok(())
    }

    /// Returns FinalExecutionOutcomeView for the given transaction.
    /// Waits for the end of the execution of all corresponding receipts
    pub fn get_final_transaction_result(
        &self,
        transaction_hash: &CryptoHash,
    ) -> Result<FinalExecutionOutcomeView, Error> {
        let mut outcomes = Vec::new();
        self.get_recursive_transaction_results(&mut outcomes, transaction_hash, true)?;
        let status = self.get_execution_status(&outcomes, transaction_hash);
        let receipts_outcome = outcomes.split_off(1);
        let transaction = self.get_transaction_view(transaction_hash)?;
        let transaction_outcome = outcomes.pop().unwrap();
        Ok(FinalExecutionOutcomeView { status, transaction, transaction_outcome, receipts_outcome })
    }

    /// Returns the `FinalExecutionOutcomeView` for the given transaction,
    /// assembled from whatever execution outcomes exist so far (it does not
    /// wait for all receipts to finish).
    ///
    /// Errors with `DBNotFoundErr` if the transaction is unknown to this node
    /// *or* has no execution outcome yet. Use
    /// [`Self::get_partial_transaction_result_option`] when those two cases
    /// need to be told apart.
    pub fn get_partial_transaction_result(
        &self,
        transaction_hash: &CryptoHash,
    ) -> Result<FinalExecutionOutcomeView, Error> {
        self.get_partial_transaction_result_option(transaction_hash)?.ok_or_else(|| {
            Error::DBNotFoundErr(format!(
                "Transaction {} has no execution outcome",
                transaction_hash
            ))
        })
    }

    /// Returns the `Ok(Some(FinalExecutionOutcomeView))` for the given
    /// transaction, assembled from whatever execution outcomes exist so far (it
    /// does not wait for all receipts to finish).
    ///
    /// Returns `Ok(None)` if the transaction is recorded on chain but has no
    /// execution outcome yet (included, not executed), and `Err(DBNotFoundErr)`
    /// if the transaction is not in the store at all. An external call is found
    /// by its outcome, so one with no outcome yet is not found.
    pub fn get_partial_transaction_result_option(
        &self,
        transaction_hash: &CryptoHash,
    ) -> Result<Option<FinalExecutionOutcomeView>, Error> {
        let transaction = self.get_transaction_view(transaction_hash)?;

        let mut outcomes = Vec::new();
        self.get_recursive_transaction_results(&mut outcomes, transaction_hash, false)?;
        if outcomes.is_empty() {
            // The transaction is in the store (included in a chunk) but its execution outcome has
            // not been recorded yet, so there is no result to assemble.
            return Ok(None);
        }

        let status = self.get_execution_status(&outcomes, transaction_hash);
        let receipts_outcome = outcomes.split_off(1);
        let transaction_outcome = outcomes.pop().unwrap();
        Ok(Some(FinalExecutionOutcomeView {
            status,
            transaction,
            transaction_outcome,
            receipts_outcome,
        }))
    }

    /// Returns corresponding receipts for provided outcome
    /// The incoming list in receipts_outcome may be partial
    pub fn get_transaction_result_with_receipt(
        &self,
        outcome: FinalExecutionOutcomeView,
    ) -> Result<FinalExecutionOutcomeWithReceiptView, Error> {
        let receipt_id_from_transaction =
            outcome.transaction_outcome.outcome.receipt_ids.get(0).cloned();
        // Only a signed transaction converts into a receipt to its own signer, which is left out.
        // An external call's receipts come from its run and are all listed.
        let is_local_receipt =
            outcome.transaction.as_signed().is_some_and(|tx| tx.signer_id == tx.receiver_id);

        let receipts = outcome
            .receipts_outcome
            .iter()
            .filter_map(|outcome| {
                if Some(outcome.id) == receipt_id_from_transaction && is_local_receipt {
                    None
                } else {
                    Some(
                        self.get_receipt(&outcome.id)
                            .map(|r| Receipt::clone(&r).into())
                            .ok_or_else(|| {
                                Error::DBNotFoundErr(format!("Receipt {} is not found", outcome.id))
                            }),
                    )
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FinalExecutionOutcomeWithReceiptView { final_outcome: outcome, receipts })
    }

    /// The transaction whose outcome has id `id`. A signed transaction is looked up by its hash, and
    /// an external call by the unique id of its copy.
    fn get_transaction_view(&self, id: &CryptoHash) -> Result<TransactionEnvelopeView, Error> {
        let not_found = || Error::DBNotFoundErr(format!("Transaction {} is not found", id));
        // The id doesn't say its kind. For an external call this read misses, since a unique id is
        // never a key of the transactions column, and the column's bloom filter answers it.
        if let Some(transaction) = self.get_transaction(id) {
            let signed_tx =
                Arc::unwrap_or_clone(transaction).into_signed().ok_or_else(not_found)?;
            return Ok(TransactionEnvelopeView::Signed(signed_tx.into()));
        }
        let outcome = self.get_execution_outcome(id).map_err(|_| not_found())?;
        let tx_hash = self.get_external_call_hash(id, &outcome.block_hash).ok_or_else(not_found)?;
        let transaction = self.get_transaction(&tx_hash).ok_or_else(not_found)?;
        let call = transaction.as_unsigned().ok_or_else(not_found)?.clone();
        Ok(TransactionEnvelopeView::Unsigned(EccTransactionView::new(call, *id)))
    }

    /// Get transaction result for given hash of transaction or receipt id
    /// Chain may not be canonical yet
    ///
    /// An id can have outcomes in several canonical blocks. One that changed state is returned, or
    /// the earliest when none did.
    pub fn get_execution_outcome(
        &self,
        id: &CryptoHash,
    ) -> Result<ExecutionOutcomeWithIdAndProof, Error> {
        let mut rejected = Vec::new();
        for outcome in self.get_outcomes_by_id(id)? {
            if !outcome.outcome_with_id.outcome.status.has_changed_state() {
                rejected.push(outcome);
                continue;
            }
            let Ok(header) = self.get_block_header(&outcome.block_hash) else { continue };
            if self.is_on_current_chain(&header).unwrap_or(false) {
                return Ok(outcome);
            }
        }
        let mut earliest: Option<(BlockHeight, ExecutionOutcomeWithIdAndProof)> = None;
        for outcome in rejected {
            let Ok(header) = self.get_block_header(&outcome.block_hash) else { continue };
            if !self.is_on_current_chain(&header).unwrap_or(false) {
                continue;
            }
            if earliest.as_ref().is_none_or(|(height, _)| header.height() < *height) {
                earliest = Some((header.height(), outcome));
            }
        }
        earliest
            .map(|(_, outcome)| outcome)
            .ok_or_else(|| Error::DBNotFoundErr(format!("EXECUTION OUTCOME: {}", id)))
    }
}
