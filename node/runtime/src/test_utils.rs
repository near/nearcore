use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use primitives::aggregate_signature::{BlsSecretKey};
use primitives::types::{MerkleHash, GroupSignature, AccountingInfo, AccountId};
use primitives::traits::{ToBytes};
use primitives::signature::{get_key_pair, PublicKey, SecretKey, sign};
use primitives::signer::InMemorySigner;
use primitives::hash::{hash, CryptoHash};
use primitives::test_utils::get_key_pair_from_seed;
use storage::{Trie, TrieUpdate};
use storage::test_utils::create_trie;
use primitives::transaction::{
    SignedTransaction, ReceiptTransaction, TransactionBody,
    SendMoneyTransaction, DeployContractTransaction, FunctionCallTransaction,
    CreateAccountTransaction, ReceiptBody, Callback, AsyncCall, CallbackInfo,
    CallbackResult, AddKeyTransaction, DeleteKeyTransaction,
    AddBlsKeyTransaction
};
use primitives::chain::{SignedShardBlockHeader, ShardBlockHeader, ReceiptBlock};

use configs::ChainSpec;
use crate::state_viewer::TrieViewer;

use super::{
    ApplyResult, ApplyState, Runtime, set, callback_id_to_bytes, get, account_id_to_bytes,
    COL_ACCOUNT, Account
};

pub fn alice_account() -> AccountId {
    "alice.near".to_string()
}
pub fn bob_account() -> AccountId {
    "bob.near".to_string()
}
pub fn eve_account() -> AccountId {
    "eve.near".to_string()
}

pub fn default_code_hash() -> CryptoHash {
    let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm");
    hash(genesis_wasm)
}

pub fn generate_test_chain_spec() -> (ChainSpec, InMemorySigner, SecretKey) {
    use rand::{SeedableRng, XorShiftRng};

    let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
    let account_id = "alice.near";
    let mut rng = XorShiftRng::from_seed([11111, 22222, 33333, 44444]);
    let secret_key = BlsSecretKey::generate_from_rng(&mut rng);
    let public_key = secret_key.get_public_key();
    let authority = public_key.to_readable();
    let signer = InMemorySigner {
        account_id: account_id.to_string(),
        public_key,
        secret_key,
    };
    let (public_key, secret_key) = get_key_pair_from_seed("alice.near");
    (ChainSpec {
        accounts: vec![
            ("alice.near".to_string(), public_key.to_readable(), 100, 10),
            ("bob.near".to_string(), get_key_pair_from_seed("bob.near").0.to_readable(), 0, 10),
            ("system".to_string(), get_key_pair_from_seed("system").0.to_readable(), 0, 0),
        ],
        initial_authorities: vec![(account_id.to_string(), authority, 50)],
        genesis_wasm,
        beacon_chain_epoch_length: 2,
        beacon_chain_num_seats_per_slot: 10,
        boot_nodes: vec![],
    }, signer, secret_key)
}

pub fn get_runtime_and_trie_from_chain_spec(chain_spec: &ChainSpec) -> (Runtime, Arc<Trie>, MerkleHash) {
    let trie = create_trie();
    let runtime = Runtime {};
    let trie_update = TrieUpdate::new(trie.clone(), MerkleHash::default());
    let (genesis_root, db_changes) = runtime.apply_genesis_state(
        trie_update,
        &chain_spec.accounts,
        &chain_spec.genesis_wasm,
        &chain_spec.initial_authorities
    );
    trie.apply_changes(db_changes).unwrap();
    (runtime, trie, genesis_root)
}

pub fn get_runtime_and_trie() -> (Runtime, Arc<Trie>, MerkleHash) {
    let (chain_spec, _, _) = generate_test_chain_spec();
    get_runtime_and_trie_from_chain_spec(&chain_spec)
}

pub fn get_test_trie_viewer() -> (TrieViewer, TrieUpdate) {
    let (_, trie, root) = get_runtime_and_trie();
    let trie_viewer = TrieViewer {};
    let state_update = TrieUpdate::new(trie, root);
    (trie_viewer, state_update)
}

pub fn encode_int(val: i32) -> [u8; 4] {
    let mut tmp = [0u8; 4];
    LittleEndian::write_i32(&mut tmp, val);
    tmp
}

pub fn to_receipt_block(receipts: Vec<ReceiptTransaction>) -> ReceiptBlock {
    let header = SignedShardBlockHeader {
        body: ShardBlockHeader {
            parent_hash: CryptoHash::default(),
            shard_id: 0,
            index: 0,
            merkle_root_state: CryptoHash::default(),
            receipt_merkle_root: CryptoHash::default(),
        },
        hash: CryptoHash::default(),
        signature: GroupSignature::default(),
    };
    ReceiptBlock {
        header,
        path: vec![],
        receipts
    }
}

impl Runtime {
    pub fn apply_all_vec(
        &mut self,
        trie: Arc<Trie>,
        apply_state: ApplyState,
        prev_receipts: Vec<ReceiptBlock>,
        transactions: Vec<SignedTransaction>,
    ) -> Vec<ApplyResult> {
        let mut cur_apply_state = apply_state;
        let mut receipts = prev_receipts;
        let mut txs = transactions;
        let mut results = vec![];
        loop {
            let state_update = TrieUpdate::new(trie.clone(), cur_apply_state.root);
            let mut apply_result = self.apply(state_update, &cur_apply_state, &receipts, &txs);
            results.push(apply_result.clone());
            if apply_result.new_receipts.is_empty() {
                return results;
            }
            trie.apply_changes(apply_result.db_changes).unwrap();
            cur_apply_state = ApplyState {
                root: apply_result.root,
                shard_id: cur_apply_state.shard_id,
                block_index: cur_apply_state.block_index,
                parent_block_hash: cur_apply_state.parent_block_hash,
            };
            receipts = vec![to_receipt_block(apply_result.new_receipts.drain().flat_map(|(_, v)| v).collect())];
            txs = vec![];
        }
    }

    pub fn apply_all(
        &mut self,
        trie: Arc<Trie>,
        apply_state: ApplyState,
        transactions: Vec<SignedTransaction>,
    ) -> ApplyResult {
        self.apply_all_vec(trie, apply_state, vec![], transactions).pop().unwrap()
    }
}

pub struct User {
    runtime: Runtime,
    account_id: String,
    nonce: u64,
    trie: Arc<Trie>,
    pub pub_key: PublicKey,
    secret_key: SecretKey,
    pub bls_secret_key: BlsSecretKey,
}

impl User {
    pub fn new(
        runtime: Runtime,
        account_id: &str,
        trie: Arc<Trie>,
        root: MerkleHash
    ) -> (Self, MerkleHash) {
        let (pub_key, secret_key) = get_key_pair();
        let mut state_update = TrieUpdate::new(trie.clone(), root);
        let mut account: Account = get(
            &mut state_update,
            &account_id_to_bytes(COL_ACCOUNT, &account_id.to_string())
        ).unwrap();
        let bls_secret_key = BlsSecretKey::generate();
        account.public_keys.push(pub_key);
        account.bls_public_key = bls_secret_key.get_public_key();
        set(
            &mut state_update,
            &account_id_to_bytes(COL_ACCOUNT, &account_id.to_string()),
            &account
        );
        let (new_root, transaction) = state_update.finalize();
        trie.apply_changes(transaction).unwrap();

        (User {
            runtime,
            account_id: account_id.to_string(),
            nonce: 1,
            trie,
            pub_key,
            secret_key,
            bls_secret_key,
        }, new_root)
    }

    pub fn send_tx(
        &mut self,
        root: CryptoHash,
        tx_body: TransactionBody
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let hash = tx_body.get_hash();
        let signature = sign(hash.as_ref(), &self.secret_key);
        let transaction = SignedTransaction::new(signature, tx_body);
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let apply_results = self.runtime.apply_all_vec(
            self.trie.clone(), apply_state, vec![], vec![transaction]
        );
        let last_apply_result = apply_results[apply_results.len() - 1].clone();
        self.trie.apply_changes(last_apply_result.db_changes).unwrap();
        (last_apply_result.root, apply_results)
    }

    pub fn send_money(
        &mut self,
        root: MerkleHash,
        destination: &str,
        amount: u64
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let tx_body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: self.nonce,
            originator: self.account_id.clone(),
            receiver: destination.to_string(),
            amount,
        });
        self.nonce += 1;
        self.send_tx(root, tx_body)
    }

    pub fn create_account(
        &mut self,
        root: MerkleHash,
        account_id: &str,
        amount: u64
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let (pub_key, _) = get_key_pair();
        self.create_account_with_key(root, account_id, amount, pub_key)
    }

    pub fn create_account_with_key(
        &mut self,
        root: MerkleHash,
        account_id: &str,
        amount: u64,
        pub_key: PublicKey
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let tx_body = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: self.nonce,
            originator: self.account_id.clone(),
            new_account_id: account_id.to_string(),
            amount,
            public_key: pub_key.0[..].to_vec()
        });
        self.nonce += 1;
        self.send_tx(root, tx_body)
    }

    pub fn deploy_contract(
        &mut self,
        root: MerkleHash,
        contract_id: &str,
        wasm_binary: &[u8]
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let tx_body = TransactionBody::DeployContract(DeployContractTransaction {
            nonce: self.nonce,
            contract_id: contract_id.to_string(),
            wasm_byte_array: wasm_binary.to_vec(),
        });
        self.nonce += 1;
        self.send_tx(root, tx_body)
    }

    pub fn call_function(
        &mut self,
        root: MerkleHash,
        contract_id: &str,
        method_name: &str,
        args: Vec<u8>,
        amount: u64
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let tx_body = TransactionBody::FunctionCall(FunctionCallTransaction {
                nonce: self.nonce,
                originator: self.account_id.clone(),
                contract_id: contract_id.to_string(),
                method_name: method_name.as_bytes().to_vec(),
                args,
                amount,
        });
        self.nonce += 1;
        self.send_tx(root, tx_body)
    }

    pub fn add_key(
        &mut self,
        root: MerkleHash,
        key: PublicKey
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let tx_body = TransactionBody::AddKey(AddKeyTransaction {
            nonce: self.nonce,
            originator: self.account_id.clone(),
            new_key: key.0[..].to_vec()
        });
        self.nonce += 1;
        self.send_tx(root, tx_body)
    }

    pub fn delete_key(
        &mut self,
        root: MerkleHash,
        key: PublicKey,
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let tx_body = TransactionBody::DeleteKey(DeleteKeyTransaction {
            nonce: self.nonce,
            originator: self.account_id.clone(),
            cur_key: key.0[..].to_vec()
        });
        self.nonce += 1;
        self.send_tx(root, tx_body)
    }

    pub fn add_bls_key(
        &mut self,
        root: MerkleHash,
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let new_key = self.bls_secret_key.get_public_key().to_bytes();
        let proof_of_possession = self.bls_secret_key.get_proof_of_possession().to_bytes();
        let tx_body = TransactionBody::AddBlsKey(AddBlsKeyTransaction {
            nonce: self.nonce,
            originator: self.account_id.clone(),
            new_key,
            proof_of_possession,
        });
        self.nonce += 1;
        self.send_tx(root, tx_body)
    }

    fn send_receipt(
        &mut self,
        root: MerkleHash,
        receipt: ReceiptTransaction,
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let apply_state = ApplyState {
            root,
            shard_id: 0,
            parent_block_hash: CryptoHash::default(),
            block_index: 0
        };
        let apply_results = self.runtime.apply_all_vec(
            self.trie.clone(), apply_state, vec![to_receipt_block(vec![receipt])], vec![]
        );
        let last_apply_result = apply_results[apply_results.len() - 1].clone();
        self.trie.apply_changes(last_apply_result.db_changes).unwrap();
        (last_apply_result.root, apply_results)
    }

    pub fn async_call(
        &mut self,
        root: MerkleHash,
        dst: &str,
        method: &str,
        args: Vec<u8>,
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let nonce = hash(&[1, 2, 3]);
        let receipt = ReceiptTransaction::new(
            self.account_id.clone(),
            dst.to_string(),
            nonce,
            ReceiptBody::NewCall(AsyncCall::new(
                method.as_bytes().to_vec(),
                args,
                0,
                0,
                AccountingInfo {
                    originator: self.account_id.clone(),
                    contract_id: None,
                },
            ))
        );
        self.send_receipt(root, receipt)
    }

    pub fn callback(
        &mut self,
        root: MerkleHash,
        dst: &str,
        method: &str,
        args: Vec<u8>,
        id: Vec<u8>,
    ) -> (MerkleHash, Vec<ApplyResult>) {
        let mut callback = Callback::new(
            method.as_bytes().to_vec(),
            args,
            0,
            AccountingInfo {
                originator: self.account_id.clone(),
                contract_id: None,
            },
        );
        callback.results.resize(1, None);
        let mut state_update = TrieUpdate::new(self.trie.clone(), root);
        set(
            &mut state_update,
            &callback_id_to_bytes(&id.clone()),
            &callback
        );
        let (new_root, transaction) = state_update.finalize();
        self.trie.apply_changes(transaction).unwrap();
        let receipt = ReceiptTransaction::new(
            self.account_id.clone(),
            dst.to_string(),
            hash(&[1, 2, 3]),
            ReceiptBody::Callback(CallbackResult::new(
                CallbackInfo::new(id.clone(), 0, self.account_id.clone()),
                None,
            ))
        );
        self.send_receipt(new_root, receipt)
    }
}

pub fn setup_test_contract(wasm_binary: &[u8]) -> (User, CryptoHash) {
    let (runtime, trie, genesis_root) = get_runtime_and_trie();
    let (mut user, root) = User::new(runtime, "alice.near", trie.clone(), genesis_root);
    let (root_with_account, _) = user.create_account(root, "test_contract", 0);
    assert_ne!(root_with_account, root);
    let (mut user, root) = User::new(user.runtime, "test_contract", user.trie, root_with_account);
    let (root_with_contract_code, _) = user.deploy_contract(
        root, "test_contract", wasm_binary
    );
    assert_ne!(root_with_contract_code, root);
    User::new(user.runtime, "alice.near", user.trie, root_with_contract_code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setup_test_contract() {
        setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    }
}
