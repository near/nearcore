//! State viewer functions to list and filter accounts that have contracts
//! deployed.

use near_primitives::hash::CryptoHash;
use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_contract_code_key;
use near_primitives::types::AccountId;
use near_store::Trie;
use std::sync::Arc;

/// Output type for contract account queries with all relevant data around a
/// single contract.
pub(crate) struct ContractAccount {
    pub(crate) account_id: AccountId,
    pub(crate) source_wasm: Arc<[u8]>,
}

impl ContractAccount {
    pub(crate) fn from_contract_trie_node(
        trie_key: &[u8],
        value_hash: CryptoHash,
        trie: &Trie,
    ) -> anyhow::Result<Self> {
        let account_id = parse_account_id_from_contract_code_key(trie_key)?;
        let source_wasm = trie.storage.retrieve_raw_bytes(&value_hash)?;
        Ok(Self { account_id, source_wasm })
    }
}

impl std::fmt::Display for ContractAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:<64} {:>9}", self.account_id, self.source_wasm.len())
    }
}
