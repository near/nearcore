use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use abci::*;
use log::{error, info};
use protobuf::parse_from_bytes;

use near::GenesisConfig;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::traits::ToBytes;
use near_primitives::transaction::{SignedTransaction, TransactionStatus};
use near_primitives::types::{AccountId, AuthorityStake, BlockIndex, MerkleHash};
use near_primitives::utils::prefix_for_access_key;
use near_store::test_utils::create_test_store;
use near_store::{create_store, Store, Trie, TrieUpdate, COL_BLOCK_MISC};
use near_verifier::TransactionVerifier;
use node_runtime::adapter::{query_client, RuntimeAdapter};
use node_runtime::ethereum::EthashProvider;
use node_runtime::state_viewer::{AccountViewCallResult, TrieViewer};
use node_runtime::{ApplyState, Runtime, ETHASH_CACHE_PATH};

const STORAGE_PATH: &str = "storage";

fn chain_id_to_bytes(index: &u32) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            index as *const u32 as *const u8,
            std::mem::size_of::<u32>() / std::mem::size_of::<u8>(),
        )
    }
}

fn enc_slice(slice: &[u8]) -> Vec<u8> {
    let id: u32 = 0u32; // chain id from legacy storage.
    let mut res = vec![];
    res.extend_from_slice(chain_id_to_bytes(&id));
    res.extend_from_slice(slice);
    res
}

fn best_index_key(genesis_hash: &CryptoHash) -> Vec<u8> {
    let mut key = enc_slice(genesis_hash.as_ref());
    key.extend_from_slice(&[0]);
    key
}

fn best_hash_key(genesis_hash: &CryptoHash) -> Vec<u8> {
    enc_slice(genesis_hash.as_ref())
}

/// Connector of NEAR Core with Tendermint.
pub struct NearMint {
    genesis_config: GenesisConfig,
    runtime: Runtime,
    trie: Arc<Trie>,
    trie_viewer: TrieViewer,
    store: Arc<Store>,
    root: MerkleHash,
    state_update: Option<TrieUpdate>,
    apply_state: Option<ApplyState>,
    authority_proposals: Vec<AuthorityStake>,
    height: u64,
    genesis_hash: CryptoHash,
}

fn get_storage_path(base_path: &Path) -> String {
    let mut storage_path = base_path.to_owned();
    storage_path.push(STORAGE_PATH);
    match fs::canonicalize(storage_path.clone()) {
        Ok(path) => info!("Opening storage database at {:?}", path),
        _ => info!("Could not resolve {:?} path", storage_path),
    };
    storage_path.to_str().unwrap().to_owned()
}

impl NearMint {
    pub fn new_from_storage(
        base_path: &Path,
        store: Arc<Store>,
        genesis_config: GenesisConfig,
    ) -> Self {
        let trie = Arc::new(Trie::new(store.clone()));
        let mut ethash_path = base_path.to_owned();
        ethash_path.push(ETHASH_CACHE_PATH);
        let ethash_provider = Arc::new(Mutex::new(EthashProvider::new(ethash_path.as_path())));
        let runtime = Runtime::new(ethash_provider.clone());
        let trie_viewer = TrieViewer::new(ethash_provider);

        // Compute genesis from current spec.
        let state_update = TrieUpdate::new(trie.clone(), MerkleHash::default());
        let (db_changes, genesis_root) = runtime.apply_genesis_state(
            state_update,
            &genesis_config.accounts,
            &genesis_config.authorities,
            &genesis_config.contracts,
        );

        let maybe_best_hash = store.get_ser(COL_BLOCK_MISC, &best_hash_key(&genesis_root));
        let (root, height) = if let Ok(Some(best_hash)) = maybe_best_hash {
            (
                best_hash,
                store
                    .get_ser(COL_BLOCK_MISC, b"BEST_INDEX")
                    .expect("Failed to read from storage")
                    .expect("Missing index with hash present"),
            )
        } else {
            // Apply genesis.
            db_changes.commit().expect("Failed to commit genesis state");
            (genesis_root, 0)
        };

        NearMint {
            genesis_config,
            runtime,
            trie,
            trie_viewer,
            store,
            root,
            apply_state: None,
            state_update: None,
            authority_proposals: Vec::default(),
            height,
            genesis_hash: genesis_root,
        }
    }

    pub fn new(base_path: &Path, genesis_config: GenesisConfig) -> Self {
        let store = create_store(&get_storage_path(base_path));
        Self::new_from_storage(base_path, store, genesis_config)
    }

    /// In memory instance of nearmint used for test
    pub fn new_for_test(genesis_config: GenesisConfig) -> Self {
        let store = create_test_store();
        Self::new_from_storage(Path::new("test"), store, genesis_config)
    }
}

fn convert_tx(data: &[u8]) -> Result<SignedTransaction, String> {
    parse_from_bytes::<near_protos::signed_transaction::SignedTransaction>(&data)
        .map_err(|e| format!("Protobuf error: {}", e))
        .and_then(TryInto::try_into)
}

impl RuntimeAdapter for NearMint {
    fn view_account(
        &self,
        state_root: MerkleHash,
        account_id: &AccountId,
    ) -> Result<AccountViewCallResult, String> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.view_account(&state_update, account_id)
    }

    fn call_function(
        &self,
        state_root: MerkleHash,
        _height: BlockIndex,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, String> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        self.trie_viewer.call_function(
            state_update,
            self.height,
            contract_id,
            method_name,
            args,
            logs,
        )
    }

    fn view_access_key(&self, state_root: MerkleHash, account_id: &String) -> Result<Vec<PublicKey>, String> {
        let state_update = TrieUpdate::new(self.trie.clone(), state_root);
        let prefix = prefix_for_access_key(account_id);
        match state_update.iter(&prefix) {
            Ok(iter) => iter
                .map(|key| {
                    let public_key = &key[prefix.len()..];
                    PublicKey::try_from(public_key).map_err(|e| format!("{}", e))
                })
                .collect::<Result<Vec<_>, String>>(),
            Err(e) => Err(e),
        }
    }
}

impl Application for NearMint {
    fn info(&mut self, req: &RequestInfo) -> ResponseInfo {
        info!("Info: {:?}", req);
        let mut resp = ResponseInfo::new();
        if self.height > 0 {
            resp.set_last_block_app_hash(self.root.as_ref().to_vec());
            resp.set_last_block_height(self.height as i64);
        }
        resp
    }

    fn query(&mut self, req: &RequestQuery) -> ResponseQuery {
        let mut resp = ResponseQuery::new();
        match query_client(self, self.root, self.height, &req.path, &req.data) {
            Ok(response) => {
                resp.key = response.key;
                resp.value = response.value;
                resp.log = response.log;
                resp.code = response.code;
                resp.height = response.height;
                resp.info = response.info;
                resp.index = response.index;
            }
            Err(e) => {
                resp.code = 1;
                resp.log = e;
            }
        }
        resp
    }

    fn check_tx(&mut self, req: &RequestCheckTx) -> ResponseCheckTx {
        let mut resp = ResponseCheckTx::new();
        match convert_tx(&req.tx) {
            Ok(tx) => {
                info!("Check tx: {:?}", tx);
                let state_update = TrieUpdate::new(self.trie.clone(), self.root);
                let verifier = TransactionVerifier::new(&state_update);
                if let Err(e) = verifier.verify_transaction(&tx) {
                    error!("Failed check tx: {:?}, error: {}", req.tx, e);
                    resp.code = 1;
                }
            }
            Err(err) => {
                info!("Failed check tx: {:?}, error: {:?}", req.tx, err);
                resp.code = 1;
            }
        };
        resp
    }

    fn init_chain(&mut self, req: &RequestInitChain) -> ResponseInitChain {
        info!("Init chain: {:?}", req);
        let mut resp = ResponseInitChain::new();
        for (_, public_key, amount) in self.genesis_config.authorities.iter() {
            let mut validator = ValidatorUpdate::new();
            let mut pub_key = PubKey::new();
            pub_key.set_field_type("ed25519".to_string());
            pub_key.data = PublicKey::try_from(public_key.0.as_str())
                .expect("Failed to parse public key in chain spec")
                .to_bytes();
            validator.set_pub_key(pub_key);
            validator.power = *amount as i64;
            resp.validators.push(validator);
        }
        resp
    }

    fn begin_block(&mut self, req: &RequestBeginBlock) -> ResponseBeginBlock {
        info!("Begin block: {:?}", req);
        self.apply_state = Some(ApplyState {
            root: self.root,
            shard_id: 0,
            block_index: req.header.clone().unwrap().height as u64,
            parent_block_hash: self.root,
        });
        self.state_update = Some(TrieUpdate::new(self.trie.clone(), self.root));
        ResponseBeginBlock::new()
    }

    fn deliver_tx(&mut self, p: &RequestDeliverTx) -> ResponseDeliverTx {
        let mut resp = ResponseDeliverTx::new();
        if let Ok(tx) = convert_tx(&p.tx) {
            if let (Some(state_update), Some(apply_state)) =
                (&mut self.state_update, &self.apply_state)
            {
                let mut incoming_receipts = HashMap::default();
                let tx_result = Runtime::process_transaction(
                    &self.runtime,
                    state_update,
                    apply_state.block_index,
                    &tx,
                    &mut incoming_receipts,
                    &mut self.authority_proposals,
                );
                if tx_result.status != TransactionStatus::Completed {
                    info!("Failed tx: {:?}", tx_result);
                }
                let mut logs = tx_result.logs;
                if let Some(result) = tx_result.result {
                    resp.data = result;
                }
                if tx_result.status != TransactionStatus::Completed {
                    resp.code = 2;
                } else {
                    let mut receipts = incoming_receipts.remove(&0).unwrap_or_else(|| vec![]);
                    while !receipts.is_empty() {
                        let mut new_receipts = HashMap::default();
                        for receipt in receipts.iter() {
                            let receipt_result = Runtime::process_receipt(
                                &mut self.runtime,
                                state_update,
                                0,
                                apply_state.block_index,
                                receipt,
                                &mut new_receipts,
                            );
                            if receipt_result.status != TransactionStatus::Completed {
                                info!("Failed tx: {:?}", receipt_result);
                                resp.code = 1;
                            }
                            if let Some(result) = receipt_result.result {
                                resp.data = result;
                            }
                            logs.extend(receipt_result.logs);
                        }
                        receipts = new_receipts.remove(&0).unwrap_or_else(|| vec![]);
                    }
                }
                resp.log = logs.join("\n");
            }
        } else {
            resp.code = 1;
        }
        resp
    }

    fn end_block(&mut self, req: &RequestEndBlock) -> ResponseEndBlock {
        info!("End block: {:?}", req);
        self.height = req.height as u64;
        ResponseEndBlock::new()
    }

    fn commit(&mut self, req: &RequestCommit) -> ResponseCommit {
        let mut resp = ResponseCommit::new();
        if let Some(state_update) = self.state_update.take() {
            info!("Commit: {:?}", req);
            if let Some(apply_state) = &self.apply_state {
                let (mut db_changes, new_root) = state_update.finalize();
                db_changes
                    .set_ser(
                        COL_BLOCK_MISC,
                        &best_index_key(&self.genesis_hash),
                        &apply_state.block_index,
                    )
                    .expect("Failed to serialize new index");
                db_changes
                    .set_ser(COL_BLOCK_MISC, &best_hash_key(&self.genesis_hash), &new_root)
                    .expect("Failed to serialize new root");
                db_changes.commit().expect("Failed to commit to database");
                self.state_update = None;
                self.apply_state = None;
                self.root = new_root;
            }
        }
        resp.data = self.root.as_ref().to_vec();
        resp
    }
}

#[cfg(test)]
mod tests {
    use protobuf::Message;

    use near_primitives::crypto::signer::InMemorySigner;
    use near_primitives::hash::CryptoHash;
    use near_primitives::transaction::TransactionBody;
    use near_primitives::types::StructSignature;
    use node_runtime::adapter::RuntimeAdapter;
    use near::config::{GenesisConfig, TESTING_INIT_BALANCE};

    use super::*;

    #[test]
    fn test_apply_block() {
        let chain_spec = GenesisConfig::testing_spec(2, 1);
        let mut nearmint = NearMint::new_for_test(chain_spec);
        let req_init = RequestInitChain::new();
        let resp_init = nearmint.init_chain(&req_init);
        assert_eq!(resp_init.validators.len(), 1);
        let mut req_begin_block = RequestBeginBlock::new();
        let mut h = Header::new();
        h.height = 1;
        let mut last_block_id = BlockID::new();
        last_block_id.hash = CryptoHash::default().as_ref().to_vec();
        h.set_last_block_id(last_block_id);
        req_begin_block.set_header(h);
        nearmint.begin_block(&req_begin_block);
        let signer = InMemorySigner::from_seed("near.0", "near.0");
        // Send large enough amount of money so that we can simultaneously check whether they were
        // debited and whether the rent was applied too.
        let money_to_send = 1_000_000;
        let tx: near_protos::signed_transaction::SignedTransaction =
            TransactionBody::send_money(1, "near.0", "near.1", money_to_send).sign(&signer).into();
        let mut deliver_req = RequestDeliverTx::new();
        deliver_req.tx = tx.write_to_bytes().unwrap();
        let deliver_resp = nearmint.deliver_tx(&deliver_req);
        assert_eq!(deliver_resp.log, "");
        assert_eq!(deliver_resp.code, 0);
        let mut req_end_block = RequestEndBlock::new();
        req_end_block.height = 1;
        nearmint.end_block(&req_end_block);
        let req_commit = RequestCommit::new();
        nearmint.commit(&req_commit);

        let alice_info = nearmint.view_account(nearmint.root, &"near.0".to_string()).unwrap();
        // Should be strictly less, because the rent was applied too.
        assert!(alice_info.amount < TESTING_INIT_BALANCE - money_to_send);
        let bob_info = nearmint.view_account(nearmint.root, &"near.1".to_string()).unwrap();
        // The balance was applied but the rent was not subtracted because we have not performed
        // interactions from that account.
        assert_eq!(bob_info.amount, TESTING_INIT_BALANCE + money_to_send);
        assert_eq!(nearmint.height, 1);

        let mut req_query = RequestQuery::new();
        req_query.path = "account/near.0".to_string();
        let resp_query = nearmint.query(&req_query);
        assert_eq!(resp_query.code, 0);
        let resp: AccountViewCallResult = serde_json::from_slice(&resp_query.value).unwrap();
        assert_eq!(resp.amount, alice_info.amount);
    }

    #[test]
    fn test_invalid_transaction() {
        let chain_spec = GenesisConfig::test(vec!["alice.near", "bob.near"]);
        let mut nearmint = NearMint::new_for_test(chain_spec);
        let fake_signature = StructSignature::try_from(&[0u8; 64] as &[u8]).unwrap();
        let body = TransactionBody::send_money(1, "alice.near", "bob.near", 10);
        let invalid_tx: near_protos::signed_transaction::SignedTransaction =
            SignedTransaction::new(fake_signature, body, None).into();
        let mut req_tx = RequestCheckTx::new();
        req_tx.set_tx(invalid_tx.write_to_bytes().unwrap());
        let resp_tx = nearmint.check_tx(&req_tx);
        assert_eq!(resp_tx.code, 1);
    }
}
