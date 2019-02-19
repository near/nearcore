use std::collections::HashSet;
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::mpsc::{Sender, Receiver};
use futures::{Future, Stream};
use log::error;
use primitives::transaction::{SignedTransaction, verify_transaction_signature};
use primitives::chain::{ReceiptBlock, ChainPayload, SignedShardBlock};
use primitives::merkle::verify_path;
use primitives::hash::hash_struct;
use shard::ShardBlockChain;

/// mempool that stores transactions and receipts for a chain
struct Pool {
    transactions: HashSet<SignedTransaction>,
    receipts: HashSet<ReceiptBlock>,
    // TODO: make it work for beacon chain as well
    chain: Arc<ShardBlockChain>,
}

impl Pool {
    fn new(chain: Arc<ShardBlockChain>) -> Self {
        Pool {
            transactions: HashSet::new(),
            receipts: HashSet::new(),
            chain,
        }
    }

    fn add_transaction(&mut self, transaction: SignedTransaction) -> Result<(), String> {
        if self.chain.get_transaction_address(&transaction.get_hash()).is_some() {
            return Ok(());
        }
        let mut state_update = self.chain.get_state_update();
        let originator = transaction.body.get_originator();
        let public_keys = self.chain.trie_viewer.get_public_keys_for_account(
            &mut state_update,
            &originator
        )?;
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
            return Err("Invalid receipt block".to_string())
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
    let block_task = block_rx.for_each({
        let pool = pool.clone();
        move |b| {
            pool.write().import_block(&b);
            Ok(())
        }
    }).map_err(|e| error!("Error in receiving block: {:?}", e));
    tokio::spawn(block_task);
    let transaction_task = transaction_rx.for_each({
        let pool = pool.clone();
        move |t| {
            // TODO: report malicious behavior
            match pool.write().add_transaction(t) {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("{}", e);
                    Ok(())
                }
            }
        }
    }).map_err(|e| error!("Error in receiving transaction {:?}", e));
    tokio::spawn(transaction_task);
    let receipt_task = receipt_rx.for_each({
        let pool = pool.clone();
        move |r| {
            // TODO: report malicious behavior
            match pool.write().add_receipt(r) {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("{}", e);
                    Ok(())
                }
            }
        }
    }).map_err(|e| error!("Error in receiving receipt {:?}", e));
    tokio::spawn(receipt_task);
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::hash::CryptoHash;
    use primitives::signature::{SecretKey, sign};
    use primitives::transaction::{TransactionBody, SendMoneyTransaction};
    use node_runtime::test_utils::generate_test_chain_spec;
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
        let transaction = SignedTransaction::new(
            signature,
            tx_body
        );
        pool.add_transaction(transaction.clone()).unwrap();
        assert_eq!(pool.transactions.len(), 1);
        let block = SignedShardBlock::new(
            0,
            0,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![transaction],
            vec![],
            CryptoHash::default()
        );
        pool.import_block(&block);
        assert_eq!(pool.transactions.len(), 0);
    }
}