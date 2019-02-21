use futures::{Future, Stream};
use log::error;
use primitives::chain::{ChainPayload, ReceiptBlock, SignedShardBlock};
use primitives::hash::hash_struct;
use primitives::merkle::verify_path;
use primitives::transaction::{verify_transaction_signature, SignedTransaction};
use shard::ShardBlockChain;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender};

const POISONED_LOCK_ERR: &str = "The lock was poisoned";
const TOKIO_RECV_ERR: &str = "Implementation of tokio Receiver should not return an error";

/// mempool that stores transactions and receipts for a chain
struct Pool {
    transactions: HashSet<SignedTransaction>,
    receipts: HashSet<ReceiptBlock>,
    // TODO: make it work for beacon chain as well
    chain: Arc<ShardBlockChain>,
}

impl Pool {
    fn new(chain: Arc<ShardBlockChain>) -> Self {
        Pool { transactions: HashSet::new(), receipts: HashSet::new(), chain }
    }

    fn add_transaction(&mut self, transaction: SignedTransaction) -> Result<(), String> {
        if self.chain.get_transaction_address(&transaction.get_hash()).is_some() {
            return Ok(());
        }
        let mut state_update = self.chain.get_state_update();
        let originator = transaction.body.get_originator();
        let public_keys =
            self.chain.trie_viewer.get_public_keys_for_account(&mut state_update, &originator)?;
        if !verify_transaction_signature(&transaction, &public_keys) {
            return Err(format!(
                "transaction not signed with a public key of originator {:?}",
                originator
            ));
        }
        self.transactions.insert(transaction);
        Ok(())
    }

    fn add_receipt(&mut self, receipt: ReceiptBlock) -> Result<(), String> {
        // TODO: cache hash of receipt
        if self.chain.get_transaction_address(&hash_struct(&receipt)).is_some() {
            return Ok(());
        }
        if !verify_path(receipt.header.body.receipt_merkle_root, &receipt.path, &receipt.receipts) {
            return Err("Invalid receipt block".to_string());
        }
        self.receipts.insert(receipt);
        Ok(())
    }

    #[allow(unused)]
    fn produce_payload(&mut self) -> ChainPayload {
        let transactions: Vec<_> = self.transactions.drain().collect();
        let receipts: Vec<_> = self.receipts.drain().collect();
        ChainPayload { transactions, receipts }
    }

    fn import_block(&mut self, block: &SignedShardBlock) {
        for transaction in block.body.transactions.iter() {
            self.transactions.remove(transaction);
        }
        for receipt in block.body.receipts.iter() {
            self.receipts.remove(receipt);
        }
    }
}

/// spawns mempool and use channels to communicate to other parts of the code
// TODO: use payload_tx to send payload when we receive some sort of signal
pub fn spawn_mempool(
    block_rx: Receiver<SignedShardBlock>,
    _payload_tx: Sender<ChainPayload>,
    transaction_rx: Receiver<SignedTransaction>,
    receipt_rx: Receiver<ReceiptBlock>,
    chain: Arc<ShardBlockChain>,
) {
    let pool = Arc::new(RwLock::new(Pool::new(chain)));
    let block_task = block_rx
        .for_each({
            let pool = pool.clone();
            move |b| {
                pool.write().expect(POISONED_LOCK_ERR).import_block(&b);
                Ok(())
            }
        })
        .map_err(|_| error!(target: "memorypool", "{}", TOKIO_RECV_ERR));
    tokio::spawn(block_task);
    let transaction_task = transaction_rx
        .for_each({
            let pool = pool.clone();
            move |t| {
                // TODO: report malicious behavior
                pool.write().expect(POISONED_LOCK_ERR).add_transaction(t)
                    .or_else(|e| Ok(error!(target: "memorypool", "Failed to write transaction into a memory pool {}", e)))
            }
        })
        .map_err(|_| error!(target: "memorypool", "{}", TOKIO_RECV_ERR));
    tokio::spawn(transaction_task);
    let receipt_task = receipt_rx
        .for_each({
            let pool = pool.clone();
            move |r| {
                // TODO: report malicious behavior
                pool.write().expect(POISONED_LOCK_ERR).add_receipt(r)
                    .or_else(|e| Ok(error!(target: "memorypool", "Failed to write receipt into a memory pool {}", e)))
            }
        })
        .map_err(|_| error!(target: "memorypool", "{}", TOKIO_RECV_ERR));
    tokio::spawn(receipt_task);
}

#[cfg(test)]
mod tests {
    use super::*;
    use node_runtime::test_utils::generate_test_chain_spec;
    use primitives::hash::CryptoHash;
    use primitives::signature::{sign, SecretKey};
    use primitives::transaction::{SendMoneyTransaction, TransactionBody};
    use storage::test_utils::create_beacon_shard_storages;

    fn get_test_chain() -> (ShardBlockChain, SecretKey) {
        let (chain_spec, _, secret_key) = generate_test_chain_spec();
        let shard_storage = create_beacon_shard_storages().1;
        let chain = ShardBlockChain::new(&chain_spec, shard_storage);
        (chain, secret_key)
    }

    #[test]
    fn test_import_block() {
        // let chain = Arc::new(get_test_chain());
        let (chain, secret_key) = get_test_chain();
        let mut pool = Pool::new(Arc::new(chain));
        let tx_body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 0,
            originator: "alice.near".to_string(),
            receiver: "bob.near".to_string(),
            amount: 1,
        });
        let hash = tx_body.get_hash();
        let signature = sign(hash.as_ref(), &secret_key);
        let transaction = SignedTransaction::new(signature, tx_body);
        pool.add_transaction(transaction.clone()).unwrap();
        assert_eq!(pool.transactions.len(), 1);
        let block = SignedShardBlock::new(
            0,
            0,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![transaction],
            vec![],
            CryptoHash::default(),
        );
        pool.import_block(&block);
        assert_eq!(pool.transactions.len(), 0);
    }
}
