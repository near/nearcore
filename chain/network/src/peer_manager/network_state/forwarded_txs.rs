use crate::stats::metrics::MessageDropped;
use near_primitives::transaction::TransactionEnvelope;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Maximum number of forwarded transactions we accept between block messages. The purpose of
/// this limit is to ensure we do not spend too much time deserializing and dispatching
/// transactions when we should be focusing on consensus-related messages.
const MAX_FORWARDED_TXS_PER_BLOCK: usize = 1000;

/// Maximum number of forwarded ECCs we accept between block messages. They count toward
/// `MAX_FORWARDED_TXS_PER_BLOCK` too, so they can never fill more than this part of it.
// TODO(ecc): replace with the real limit once it is picked
const MAX_FORWARDED_ECCS_PER_BLOCK: usize = 500;

/// Counts the forwarded transactions this node accepted since the last block message, shared by
/// all the peers.
#[derive(Default)]
pub(crate) struct ForwardedTxCounters {
    txs: AtomicUsize,
    eccs: AtomicUsize,
}

impl ForwardedTxCounters {
    /// Counts a forwarded transaction, or says that it has to be dropped because the limit for
    /// this block was reached.
    pub(crate) fn try_count(&self, tx: &TransactionEnvelope) -> Result<(), MessageDropped> {
        // Strictly greater, so one more than the limit gets through. It has always been so.
        if self.txs.load(Ordering::Acquire) > MAX_FORWARDED_TXS_PER_BLOCK {
            return Err(MessageDropped::TransactionsPerBlockExceeded);
        }
        if tx.as_unsigned().is_some() {
            self.eccs
                .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    (count < MAX_FORWARDED_ECCS_PER_BLOCK).then_some(count + 1)
                })
                .map_err(|_| MessageDropped::EccsPerBlockExceeded)?;
        }
        self.txs.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Starts counting for the next block.
    pub(crate) fn reset(&self) {
        self.txs.store(0, Ordering::Release);
        self.eccs.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::hash::CryptoHash;
    use near_primitives::transaction::{EccTransaction, SignedTransaction};

    fn ecc() -> TransactionEnvelope {
        EccTransaction::new("contract.near".parse().unwrap(), vec![]).into()
    }

    fn signed_tx() -> TransactionEnvelope {
        SignedTransaction::empty(CryptoHash::default()).into()
    }

    fn count_many(counters: &ForwardedTxCounters, tx: &TransactionEnvelope, n: usize) -> usize {
        (0..n).filter(|_| counters.try_count(tx).is_ok()).count()
    }

    #[test]
    fn test_eccs_are_capped_below_the_total() {
        let counters = ForwardedTxCounters::default();
        assert_eq!(
            count_many(&counters, &ecc(), MAX_FORWARDED_ECCS_PER_BLOCK + 10),
            MAX_FORWARDED_ECCS_PER_BLOCK
        );
        assert!(matches!(counters.try_count(&ecc()), Err(MessageDropped::EccsPerBlockExceeded)));
        // Signed transactions still fit in the rest of the total.
        assert_eq!(
            count_many(&counters, &signed_tx(), MAX_FORWARDED_TXS_PER_BLOCK),
            MAX_FORWARDED_TXS_PER_BLOCK - MAX_FORWARDED_ECCS_PER_BLOCK + 1
        );
    }

    #[test]
    fn test_eccs_count_toward_the_total() {
        let counters = ForwardedTxCounters::default();
        assert_eq!(
            count_many(&counters, &signed_tx(), MAX_FORWARDED_TXS_PER_BLOCK + 10),
            MAX_FORWARDED_TXS_PER_BLOCK + 1
        );
        // The total is spent, so an ECC is dropped for it, not for its own cap.
        assert!(matches!(
            counters.try_count(&ecc()),
            Err(MessageDropped::TransactionsPerBlockExceeded)
        ));
    }

    #[test]
    fn test_dropped_ecc_does_not_use_the_total() {
        let counters = ForwardedTxCounters::default();
        count_many(&counters, &ecc(), MAX_FORWARDED_ECCS_PER_BLOCK);
        count_many(&counters, &ecc(), 100);
        assert_eq!(
            count_many(&counters, &signed_tx(), MAX_FORWARDED_TXS_PER_BLOCK),
            MAX_FORWARDED_TXS_PER_BLOCK - MAX_FORWARDED_ECCS_PER_BLOCK + 1
        );
    }

    #[test]
    fn test_reset_starts_a_new_block() {
        let counters = ForwardedTxCounters::default();
        count_many(&counters, &ecc(), MAX_FORWARDED_ECCS_PER_BLOCK);
        assert!(counters.try_count(&ecc()).is_err());
        counters.reset();
        assert!(counters.try_count(&ecc()).is_ok());
    }
}
