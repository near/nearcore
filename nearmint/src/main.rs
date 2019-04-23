use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;

use abci::*;
use clap::{App, Arg};
use env_logger::Builder;
use log::info;

use node_runtime::adapter::{query_client, RuntimeAdapter};

use node_runtime::chain_spec::ChainSpec;
use node_runtime::state_viewer::{AccountViewCallResult, TrieViewer};
use node_runtime::{ApplyState, Runtime};
use primitives::traits::ToBytes;
use primitives::transaction::{SignedTransaction, TransactionStatus};
use primitives::types::{AccountId, AuthorityStake, MerkleHash};
use primitives::crypto::signature::PublicKey;
use primitives::crypto::signer::InMemorySigner;
use protobuf::parse_from_bytes;
use storage::{create_storage, GenericStorage, ShardChainStorage, Trie, TrieUpdate};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";
const DEFAULT_BASE_PATH: &str = "";
const STORAGE_PATH: &str = "storage";

/// Connector of NEAR Core with Tendermint.
struct NearMint {
    chain_spec: ChainSpec,
    runtime: Runtime,
    trie: Arc<Trie>,
    trie_viewer: TrieViewer,
    storage: Arc<RwLock<ShardChainStorage>>,
    root: MerkleHash,
    state_update: Option<TrieUpdate>,
    apply_state: Option<ApplyState>,
    authority_proposals: Vec<AuthorityStake>,
    height: u64,
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
    pub fn new(base_path: &Path, chain_spec: ChainSpec) -> Self {
        let (_, storage) = create_storage(&get_storage_path(base_path), 1);
        let storage = storage[0].clone();

        let trie = Arc::new(Trie::new(storage.clone()));
        let runtime = Runtime {};
        let trie_viewer = TrieViewer {};

        // Compute genesis from current spec.
        let state_update = TrieUpdate::new(trie.clone(), MerkleHash::default());
        let (genesis_root, db_changes) = runtime.apply_genesis_state(
            state_update,
            &chain_spec.accounts,
            &chain_spec.genesis_wasm,
            &chain_spec.initial_authorities,
        );

        storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .blockchain_storage_mut()
            .set_genesis_hash(genesis_root)
            .expect("Failed to set genesis hash");

        let maybe_best_hash = storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .blockchain_storage_mut()
            .best_block_hash()
            .map(|x| x.cloned());
        let (root, height) = if let Ok(Some(best_hash)) = maybe_best_hash {
            (
                best_hash,
                storage
                    .write()
                    .expect(POISONED_LOCK_ERR)
                    .blockchain_storage_mut()
                    .best_block_index()
                    .unwrap()
                    .unwrap(),
            )
        } else {
            // Apply genesis.
            trie.apply_changes(db_changes).expect("Failed to commit genesis state");
            (genesis_root, 0)
        };

        NearMint {
            chain_spec,
            runtime,
            trie,
            trie_viewer,
            storage,
            root,
            apply_state: None,
            state_update: None,
            authority_proposals: Vec::default(),
            height,
        }
    }
}

fn convert_tx(data: &[u8]) -> Result<SignedTransaction, String> {
    parse_from_bytes::<near_protos::signed_transaction::SignedTransaction>(&data)
        .map_err(|e| format!("Protobuf error: {}", e))
        .and_then(TryInto::try_into)
}

impl RuntimeAdapter for NearMint {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String> {
        let mut state_update = TrieUpdate::new(self.trie.clone(), self.root);
        self.trie_viewer.view_account(&mut state_update, account_id)
    }

    fn call_function(
        &self,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
    ) -> Result<Vec<u8>, String> {
        let state_update = TrieUpdate::new(self.trie.clone(), self.root);
        self.trie_viewer.call_function(
            state_update,
            self.height,
            contract_id,
            method_name,
            args,
            logs,
        )
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
        match query_client(self, &req.path, &req.data, req.height as u64, req.prove) {
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
            Ok(tx) => info!("Check tx: {:?}", tx),
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
        for (_, public_key, _, amount) in self.chain_spec.initial_authorities.iter() {
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
                    self.runtime,
                    state_update,
                    apply_state.block_index,
                    &tx,
                    &mut incoming_receipts,
                    &mut self.authority_proposals,
                );
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
                                self.runtime,
                                state_update,
                                0,
                                apply_state.block_index,
                                receipt,
                                &mut new_receipts,
                            );
                            logs.extend(receipt_result.logs);
                            if let Some(result) = receipt_result.result {
                                resp.data = result;
                            }
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
            let (new_root, db_changes) = state_update.finalize();
            self.trie.apply_changes(db_changes).expect("Failed to commit to database");
            if let Some(apply_state) = &self.apply_state {
                let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
                guard
                    .blockchain_storage_mut()
                    .set_best_block_index(apply_state.block_index)
                    .expect("FAILED");
                guard.blockchain_storage_mut().set_best_block_hash(new_root).expect("FAILED");
            }
            self.state_update = None;
            self.apply_state = None;
            self.root = new_root;
        }
        resp.data = self.root.as_ref().to_vec();
        resp
    }
}

fn main() {
    // Parse command line arguments.
    let matches = App::new("Nearmint")
        .args(&[
            Arg::with_name("base_path")
                .short("d")
                .long("base-path")
                .value_name("PATH")
                .help("Specify a base path for persisted files.")
                .default_value(DEFAULT_BASE_PATH)
                .takes_value(true),
            Arg::with_name("chain_spec_file")
                .short("c")
                .long("chain-spec-file")
                .value_name("CHAIN_SPEC")
                .help("Specify a file location to read a custom chain spec.")
                .takes_value(true),
            Arg::with_name("devnet")
                .long("devnet")
                .help("Run with DevNet validator configuration (single alice.near validator)")
                .takes_value(false),
            Arg::with_name("abci_address")
                .short("a")
                .long("abci-address")
                .value_name("ABCI_Address")
                .help("Specify ip address and port for Tendermint ABCI")
                .default_value("127.0.0.1:26658")
                .takes_value(true),
        ])
        .get_matches();
    let base_path = matches.value_of("base_path").map(PathBuf::from).unwrap();
    let chain_spec = if matches.is_present("devnet") {
        ChainSpec::default_devnet()
    } else {
        let chain_spec_path = matches.value_of("chain_spec_file").map(PathBuf::from);
        ChainSpec::from_file_or_default(&chain_spec_path, ChainSpec::default_poa())
    };
    let addr = matches.value_of("abci_address").map(|address| address.parse().unwrap()).unwrap();

    // Setup logging.
    let mut builder = Builder::from_default_env();
    builder.default_format_timestamp_nanos(true);
    builder.filter(None, log::LevelFilter::Info);
    builder.try_init().unwrap();

    // Fire it up!
    abci::run(addr, NearMint::new(&base_path, chain_spec));
}

#[cfg(test)]
mod tests {
    use node_runtime::adapter::RuntimeAdapter;
    use node_runtime::chain_spec::ChainSpec;
    use primitives::hash::CryptoHash;
    use primitives::transaction::TransactionBody;

    use protobuf::Message;

    use tempdir::TempDir;

    use super::*;

    #[test]
    fn test_apply_block() {
        let tmp_dir = TempDir::new("apply_block").unwrap();
        let chain_spec = ChainSpec::default_devnet();
        let mut nearmint = NearMint::new(tmp_dir.path(), chain_spec);
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
        let signer = InMemorySigner::from_seed("alice.near", "alice.near");
        let tx: near_protos::signed_transaction::SignedTransaction =
            TransactionBody::send_money(1, "alice.near", "bob.near", 10).sign(&signer).into();
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

        let result = nearmint.view_account(&"alice.near".to_string()).unwrap();
        assert_eq!(result.amount, 999999999990);
        let result = nearmint.view_account(&"bob.near".to_string()).unwrap();
        assert_eq!(result.amount, 1000000000010);
        assert_eq!(nearmint.height, 1);

        let mut req_query = RequestQuery::new();
        req_query.path = "account/alice.near".to_string();
        let resp_query = nearmint.query(&req_query);
        assert_eq!(resp_query.code, 0);
        let resp: AccountViewCallResult = serde_json::from_slice(&resp_query.value).unwrap();
        assert_eq!(resp.amount, 999999999990);
    }
}
