use std::sync::{Arc, RwLock};

use primitives::hash::CryptoHash;
use primitives::types::BlockId;
use primitives::transaction::{
    TransactionAddress, TransactionResult, TransactionLogs, TransactionStatus,
    FinalTransactionResult, FinalTransactionStatus
};
use primitives::block_traits::{SignedBlock, SignedHeader};
use storage::GenericStorage;
use std::marker::PhantomData;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// General BlockChain container.
pub struct BlockChain<H, B, S> {
    /// Storage backend.
    storage: Arc<RwLock<S>>,
    // TODO: Add fork choice rule, tracking of finalized, pending, etc blocks.
    phantom_header: PhantomData<H>,
    phantom_block: PhantomData<B>,
}

impl<H, B, S> BlockChain<H, B, S>
where
    H: SignedHeader,
    B: SignedBlock<SignedHeader = H>,
    S: GenericStorage<H, B>,
{
    pub fn new(genesis: B, storage: Arc<RwLock<S>>) -> Self {
        let bc =
            Self { storage, phantom_header: Default::default(), phantom_block: Default::default() };
        bc.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .blockchain_storage_mut()
            .set_genesis(genesis)
            .expect("Failed to initialize storage from genesis");
        bc
    }

    pub fn genesis_hash(&self) -> CryptoHash {
        *self.storage.write().expect(POISONED_LOCK_ERR).blockchain_storage_mut().genesis_hash()
    }

    pub fn best_block(&self) -> B {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        let hash = *guard.blockchain_storage_mut().best_block_hash().unwrap().unwrap();
        guard.blockchain_storage_mut().block(&hash).unwrap().unwrap().clone()
    }

    #[inline]
    pub fn best_index(&self) -> u64 {
        self.best_block().header().index()
    }

    #[inline]
    pub fn best_hash(&self) -> CryptoHash {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        *guard.blockchain_storage_mut().best_block_hash().unwrap().unwrap()
    }

    /// Check if block already is known.
    pub fn is_known(&self, hash: &CryptoHash) -> bool {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        match guard.blockchain_storage_mut().header(hash) {
            Ok(Some(_)) => true,
            _ => false,
        }
    }

    fn update_best_block(&self, block: B) {
        let hash = block.block_hash();
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        guard.blockchain_storage_mut().set_best_block_hash(hash).unwrap();
        guard.blockchain_storage_mut().add_block(block).unwrap();
    }

    /// Inserts a verified block.
    pub fn insert_block(&self, block: B) {
        let block_hash = block.block_hash();
        if self.is_known(&block_hash) {
            // TODO: known header but not known block.
            return;
        }

        if self
            .storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .blockchain_storage_mut()
            .block(&block.header().parent_hash())
            .unwrap()
            .is_none()
        {
            return;
        }
        self.update_best_block(block);
    }

    pub fn get_block(&self, id: &BlockId) -> Option<B> {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        let hash = *match id {
            BlockId::Number(idx) => guard.blockchain_storage_mut().hash_by_index(*idx).unwrap()?,
            BlockId::Hash(h) => h,
        };
        guard.blockchain_storage_mut().block(&hash).unwrap().cloned()
    }

    pub fn get_header(&self, id: &BlockId) -> Option<H> {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        let hash = *match id {
            BlockId::Number(idx) => guard.blockchain_storage_mut().hash_by_index(*idx).unwrap()?,
            BlockId::Hash(h) => h,
        };
        guard.blockchain_storage_mut().header(&hash).unwrap().cloned()
    }

    pub fn get_blocks_by_index(&self, start: u64, limit: u64) -> Result<Vec<B>, String> {
        let mut blocks = vec![];
        for i in start..=(start + limit) {
            if let Some(block) = self.get_block(&BlockId::Number(i)) {
                blocks.push(block)
            }
        }
        Ok(blocks)
    }

    pub fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        self.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .blockchain_storage_mut()
            .transaction_result(hash)
            .unwrap()
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_transaction_address(&self, hash: &CryptoHash) -> Option<TransactionAddress> {
        self.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .blockchain_storage_mut()
            .transaction_address(hash)
            .unwrap()
            .cloned()
    }

    fn collect_transaction_final_result(
        &self,
        transaction_result: &TransactionResult,
        logs: &mut Vec<TransactionLogs>,
    ) -> FinalTransactionStatus {
        match transaction_result.status {
            TransactionStatus::Unknown => FinalTransactionStatus::Unknown,
            TransactionStatus::Failed => FinalTransactionStatus::Failed,
            TransactionStatus::Completed => {
                for r in transaction_result.receipts.iter() {
                    let receipt_result = self.get_transaction_result(&r);
                    logs.push(TransactionLogs {
                        hash: *r,
                        lines: receipt_result.logs.clone(),
                        receipts: receipt_result.receipts.clone(),
                    });
                    match self.collect_transaction_final_result(&receipt_result, logs) {
                        FinalTransactionStatus::Failed => return FinalTransactionStatus::Failed,
                        FinalTransactionStatus::Completed => {}
                        _ => return FinalTransactionStatus::Started,
                    };
                }
                FinalTransactionStatus::Completed
            }
        }
    }

    pub fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        let transaction_result = self.get_transaction_result(hash);
        let mut result = FinalTransactionResult {
            status: FinalTransactionStatus::Unknown,
            logs: vec![TransactionLogs {
                hash: *hash,
                lines: transaction_result.logs.clone(),
                receipts: transaction_result.receipts.clone(),
            }],
        };
        result.status =
            self.collect_transaction_final_result(&transaction_result, &mut result.logs);
        result
    }
}
