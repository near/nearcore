//! State viewer functions to list and filter accounts that have contracts
//! deployed.

use borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::transaction::{Action, ExecutionOutcomeWithProof};
use near_primitives::trie_key::trie_key_parsers::parse_account_id_from_contract_code_key;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::AccountId;
use near_store::{DBCol, NibbleSlice, StorageError, Store, Trie, TrieTraversalItem};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

type Result<T> = std::result::Result<T, ContractAccountError>;

/// Output type for a complete contract account query.
pub(crate) struct ContractAccountSummary {
    pub(crate) contracts: BTreeMap<AccountId, ContractInfo>,
    pub(crate) errors: Vec<ContractAccountError>,
}

/// Output type containing info about a single account and its contract.
///
/// This output can be observed in streaming fashion, with less memory overhead
/// than returning all results in one bulk in `ContractAccountSummary`. While
/// streaming, not all fields are available.
pub(crate) struct ContractAccount {
    pub(crate) account_id: AccountId,
    pub(crate) info: ContractInfo,
}

/// Output type containing info about a single contract.
#[derive(Default)]
pub(crate) struct ContractInfo {
    /// The WASM source code size in bytes.
    ///
    /// Available in iterator stream and in the summary.
    pub(crate) code_size: Option<usize>,
    /// Actions that have been observed to be triggered by the contract.
    ///
    /// Not available in iterator stream, only in the summary.
    pub(crate) actions: Option<BTreeSet<ActionType>>,
    /// Count incoming receipts.
    ///
    /// Not available in iterator stream, only in the summary.
    pub(crate) receipts_in: Option<usize>,
    /// Count outgoing receipts.
    ///
    /// Not available in iterator stream, only in the summary.
    pub(crate) receipts_out: Option<usize>,
}

/// Describe the desired output of a `ContractAccountIterator`.
///
/// The default filter displays nothing but the account names, for all accounts
/// in all shards.
///
/// Selecting specific accounts or skipping accounts with a lot of traffic can
/// speed up the command drastically.
///
/// By selecting only the required fields, the iterator can be more efficient.
/// For example, finding all invoked actions is very slow.
///
/// The output can either be streamed or collected into a BTreeMap. In the
/// latter case, you also want to be more conscious of memory consumption and
/// perhaps not include the full contract source code.
#[derive(Default, clap::Parser, Clone)]
pub(crate) struct ContractAccountFilter {
    /// Print the size of the source WASM.
    #[clap(long)]
    pub(crate) code_size: bool,
    /// Print the actions invoked from within each contract.
    ///
    /// Note: This will not include actions from an original transaction. It
    /// only looks at the actions inside receipts spawned by other receipts.
    /// This allows to look at on-chain actions only, which are more susceptible
    /// to gas cost changes.
    #[clap(long)]
    pub(crate) actions: bool,
    /// Print the number of action receipts received.
    #[clap(long)]
    pub(crate) receipts_in: bool,
    /// Print the number of action receipts sent.
    #[clap(long)]
    pub(crate) receipts_out: bool,

    /// Only produce output for the selected account Ids.
    #[clap(long, use_value_delimiter = true)]
    select_accounts: Option<Vec<AccountId>>,
    /// Do not look up details for the accounts listed here.
    ///
    /// This can be useful to avoid spending a long time reading through
    /// receipts that are for some of the largest accounts.
    #[clap(long, use_value_delimiter = true)]
    skip_accounts: Option<Vec<AccountId>>,
}

pub(crate) struct ContractAccountIterator {
    /// Trie nodes that point to the contracts.
    contract_nodes: VecDeque<TrieTraversalItem>,
    /// Selects fields to look up.
    filter: ContractAccountFilter,
    trie: Trie,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ContractAccountError {
    #[error("could not parse key {1:?}")]
    InvalidKey(#[source] std::io::Error, Vec<u8>),
    #[error("could not parse receipt value for receipt {1}")]
    InvalidReceipt(#[source] std::io::Error, CryptoHash),
    #[error("failed loading outgoing receipt {0}")]
    MissingOutgoingReceipt(CryptoHash),
    #[error("failed loading contract code for account {1}")]
    NoCode(#[source] StorageError, AccountId),
    #[error("failed parsing a value in col {1}")]
    UnparsableValue(#[source] std::io::Error, DBCol),
}

/// List of supported actions to filter for.
///
/// When filtering for an action, only those contracts will be listed that have
/// executed that action from within a recorded function call.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(u8)]
pub(crate) enum ActionType {
    CreateAccount,
    DeployContract,
    FunctionCall,
    Transfer,
    Stake,
    AddKey,
    DeleteKey,
    DeleteAccount,
    DataReceipt,
    Delegate,
}

impl ContractAccount {
    /// Iterate over all contracts stored in the given trie, in lexicographic
    /// order of the account IDs.
    pub(crate) fn in_trie(
        trie: Trie,
        filter: ContractAccountFilter,
    ) -> anyhow::Result<ContractAccountIterator> {
        ContractAccountIterator::new(trie, filter)
    }

    /// Iterate multiple tries at the same time.
    ///
    /// Returns an error if any of the tries fail to create an iterator.
    pub(crate) fn in_tries(
        tries: Vec<Trie>,
        filter: &ContractAccountFilter,
    ) -> anyhow::Result<impl Iterator<Item = Result<ContractAccount>>> {
        let mut iters = vec![];
        for trie in tries {
            let trie_iter = ContractAccountIterator::new(trie, filter.clone())?;
            iters.push(trie_iter);
        }
        let chained_iters = iters.into_iter().flatten();
        Ok(chained_iters)
    }

    /// Reads all the fields that can be retrieved efficiently in streaming manner.
    ///
    /// This will not populate fields, such as the actions field, that require
    /// to iterate entire columns. Calling `summary` populates the remaining fields.
    fn read_streaming_fields(
        account_id: AccountId,
        value_hash: CryptoHash,
        trie: &Trie,
        filter: &ContractAccountFilter,
    ) -> Result<Self> {
        let code = if filter.code_size {
            Some(
                trie.storage
                    .retrieve_raw_bytes(&value_hash)
                    .map_err(|err| ContractAccountError::NoCode(err, account_id.clone()))?,
            )
        } else {
            None
        };
        Ok(Self {
            account_id,
            info: ContractInfo { code_size: code.map(|bytes| bytes.len()), ..Default::default() },
        })
    }
}

impl ContractAccountIterator {
    pub(crate) fn new(trie: Trie, filter: ContractAccountFilter) -> anyhow::Result<Self> {
        let mut trie_iter = trie.iter()?;
        // TODO(#8376): Consider changing the interface to TrieKey to make this easier.
        // `TrieKey::ContractCode` requires a valid `AccountId`, we use "xx"
        let key = TrieKey::ContractCode { account_id: "xx".parse()? }.to_vec();
        let (prefix, suffix) = key.split_at(key.len() - 2);
        assert_eq!(suffix, "xx".as_bytes());

        // `visit_nodes_interval` wants nibbles stored in `Vec<u8>` as input
        let nibbles_before: Vec<u8> = NibbleSlice::new(prefix).iter().collect();
        let nibbles_after = {
            let mut tmp = nibbles_before.clone();
            *tmp.last_mut().unwrap() += 1;
            tmp
        };

        // finally, use trie iterator to find all contract nodes
        let vec_of_nodes = trie_iter.visit_nodes_interval(&nibbles_before, &nibbles_after)?;
        let contract_nodes = VecDeque::from(vec_of_nodes);
        Ok(Self { contract_nodes, filter, trie })
    }
}

/// Helper trait for blanket implementation, making the iterator composable.
pub(crate) trait Summary {
    fn summary(self, store: &Store, filter: &ContractAccountFilter) -> ContractAccountSummary;
}

impl<T: Iterator<Item = Result<ContractAccount>>> Summary for T {
    /// Eagerly evaluate everything selected by the filter, including fields
    /// that are not available in the streaming iterator.
    fn summary(self, store: &Store, filter: &ContractAccountFilter) -> ContractAccountSummary {
        let mut errors = vec![];
        let mut contracts = BTreeMap::new();

        for result in self {
            match result {
                Ok(mut contract) => {
                    // initialize values, we want zero values in the output when nothing is found
                    contract.info.receipts_in = Some(0);
                    contract.info.receipts_out = Some(0);
                    contract.info.actions = Some(BTreeSet::new());
                    contracts.insert(contract.account_id, contract.info);
                }
                Err(e) => {
                    // Print the error in stderr so that it is immediately
                    // visible in the terminal when something goes wrong but
                    // without spoiling stdout.
                    eprintln!("skipping contract due to {e}");
                    // Also store the error in the summary.
                    errors.push(e);
                }
            }
        }

        eprintln!("Done collecting {} contracts.", contracts.len());

        let iterate_receipts = filter.actions || filter.receipts_in || filter.receipts_out;
        if iterate_receipts {
            eprintln!("Iterating all receipts in the database. This might take a while...");
            let mut receipts_done = 0;
            for pair in store.iter(near_store::DBCol::Receipts) {
                if let Err(e) =
                    try_find_actions_spawned_by_receipt(pair, &mut contracts, store, filter)
                {
                    eprintln!("skipping receipt due to {e}");
                    errors.push(e);
                }
                receipts_done += 1;
                // give some feedback on progress to the user
                if receipts_done % 100_000 == 0 {
                    eprintln!("Processed {receipts_done} receipts");
                }
            }
        }
        ContractAccountSummary { contracts, errors }
    }
}

/// Given a receipt, search for all actions in its outgoing receipts.
///
/// Technically, this involves looking up the execution outcome(s) of the
/// receipt and subsequently looking up each receipt in the outgoing receipt id
/// list. Actions found in any of those receipts will be added to the accounts
/// list of actions.
///
/// If any of the receipts are missing, this will return an error and stop
/// processing receipts that would come later. Changes made to the action set by
/// already processed receipts will not be reverted.
fn try_find_actions_spawned_by_receipt(
    raw_kv_pair: std::io::Result<(Box<[u8]>, Box<[u8]>)>,
    accounts: &mut BTreeMap<AccountId, ContractInfo>,
    store: &Store,
    filter: &ContractAccountFilter,
) -> Result<()> {
    let (raw_key, raw_value) =
        raw_kv_pair.map_err(|e| ContractAccountError::UnparsableValue(e, DBCol::Receipts))?;
    // key: receipt id (CryptoHash)
    let key = CryptoHash::deserialize(&mut raw_value.as_ref())
        .map_err(|e| ContractAccountError::InvalidKey(e, raw_key.to_vec()))?;
    // value: Receipt
    let receipt = Receipt::deserialize(&mut raw_value.as_ref())
        .map_err(|e| ContractAccountError::InvalidReceipt(e, key))?;

    // Skip refunds.
    if receipt.receiver_id.is_system() {
        return Ok(());
    }

    // Note: We could use the entry API here to avoid the double hash, but we
    // would have to clone the key string. It's unclear which is better, I will
    // avoid the entry API because normal contains/get_mut seems simpler.
    if accounts.contains_key(&receipt.receiver_id) {
        // yes, this is a contract in our map (skip/select filtering has already been applied when constructing the map)
        let entry = accounts.get_mut(&receipt.receiver_id).unwrap();
        if filter.receipts_in {
            *entry.receipts_in.get_or_insert(0) += 1;
        }
        // next, check the execution results (one for each block in case of forks)
        for pair in store.iter_prefix_ser::<ExecutionOutcomeWithProof>(
            DBCol::TransactionResultForBlock,
            &raw_key,
        ) {
            let (_key, outcome) = pair.map_err(|e| {
                ContractAccountError::UnparsableValue(e, DBCol::TransactionResultForBlock)
            })?;
            if filter.receipts_out {
                *entry.receipts_out.get_or_insert(0) += outcome.outcome.receipt_ids.len();
            }
            if filter.actions {
                for outgoing_receipt_id in &outcome.outcome.receipt_ids {
                    let maybe_outgoing_receipt: Option<Receipt> = store
                        .get_ser(near_store::DBCol::Receipts, outgoing_receipt_id.as_bytes())
                        .map_err(|e| ContractAccountError::UnparsableValue(e, DBCol::Receipts))?;
                    let outgoing_receipt = maybe_outgoing_receipt.ok_or_else(|| {
                        ContractAccountError::MissingOutgoingReceipt(*outgoing_receipt_id)
                    })?;
                    match outgoing_receipt.receipt {
                        ReceiptEnum::Action(action_receipt) => {
                            for action in &action_receipt.actions {
                                let action_type = match action {
                                    Action::CreateAccount(_) => ActionType::CreateAccount,
                                    Action::DeployContract(_) => ActionType::DeployContract,
                                    Action::FunctionCall(_) => ActionType::FunctionCall,
                                    Action::Transfer(_) => ActionType::Transfer,
                                    Action::Stake(_) => ActionType::Stake,
                                    Action::AddKey(_) => ActionType::AddKey,
                                    Action::DeleteKey(_) => ActionType::DeleteKey,
                                    Action::DeleteAccount(_) => ActionType::DeleteAccount,
                                    Action::Delegate(_) => ActionType::Delegate,
                                };
                                entry
                                    .actions
                                    .get_or_insert_with(Default::default)
                                    .insert(action_type);
                            }
                        }
                        ReceiptEnum::Data(_) => {
                            entry
                                .actions
                                .get_or_insert_with(Default::default)
                                .insert(ActionType::DataReceipt);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

impl Iterator for ContractAccountIterator {
    type Item = Result<ContractAccount>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.contract_nodes.pop_front() {
            // only look at nodes with a value, ignoring intermediate nodes
            // without values
            if let TrieTraversalItem { hash, key: Some(trie_key) } = item {
                let account_id = parse_account_id_from_contract_code_key(&trie_key)
                    .map_err(|err| ContractAccountError::InvalidKey(err, trie_key.to_vec()));
                let Ok(account_id) = account_id else { return Some(Err(account_id.unwrap_err())) };

                if !self.filter.include_account(&account_id) {
                    continue;
                }

                // Here we can only look at fields that can be efficiently computed in one go.
                let contract = ContractAccount::read_streaming_fields(
                    account_id,
                    hash,
                    &self.trie,
                    &self.filter,
                );
                return Some(contract);
            }
        }
        None
    }
}

impl std::fmt::Display for ContractAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt_account_id_and_info(&self.account_id, &self.info, f)
    }
}

impl std::fmt::Display for ContractAccountSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (account_id, info) in &self.contracts {
            fmt_account_id_and_info(account_id, info, f)?;
            writeln!(f)?;
        }
        let num_errors = self.errors.len();
        if num_errors > 0 {
            writeln!(f, "And {num_errors} errors:")?;
            for err in &self.errors {
                writeln!(f, "{err}")?;
            }
        } else {
            writeln!(f)?;
            writeln!(f, "Finished without errors!")?;
        }

        Ok(())
    }
}

fn fmt_account_id_and_info(
    account_id: &AccountId,
    info: &ContractInfo,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    write!(f, "{:<64}", account_id)?;
    if let Some(size) = info.code_size {
        write!(f, " {:>9}", size)?;
    }
    if let Some(receipt_in) = info.receipts_in {
        write!(f, " {receipt_in:>10}")?;
    }
    if let Some(receipt_out) = info.receipts_out {
        write!(f, " {receipt_out:>10}")?;
    }
    if let Some(action_set) = &info.actions {
        write!(f, " ")?;
        for (i, action) in action_set.iter().enumerate() {
            if i != 0 {
                write!(f, ",")?;
            }
            write!(f, "{action:?}")?;
        }
    }
    Ok(())
}

impl ContractAccountFilter {
    pub(crate) fn write_header(&self, out: &mut impl std::io::Write) -> std::io::Result<()> {
        write!(out, "{:<64}", "ACCOUNT_ID",)?;
        if self.code_size {
            write!(out, " {:>9}", "SIZE[B]")?;
        }
        if self.receipts_in {
            write!(out, " {:>10}", "RCPTS_IN",)?;
        }
        if self.receipts_out {
            write!(out, " {:>10}", "RCPTS_OUT",)?;
        }
        if self.actions {
            write!(out, " ACTIONS")?;
        }
        writeln!(out)
    }

    fn include_account(&self, account: &AccountId) -> bool {
        if let Some(include) = &self.select_accounts {
            include.contains(account)
        } else if let Some(exclude) = &self.skip_accounts {
            !exclude.contains(account)
        } else {
            true
        }
    }

    // If any of the fields are no computable on-the-fly / streaming, then we
    // cannot stream.
    pub(crate) fn can_stream(&self) -> bool {
        !(self.actions || self.receipts_in || self.receipts_out)
    }
}

#[cfg(test)]
mod tests {
    use super::{ContractAccount, ContractAccountFilter, Summary};
    use borsh::BorshSerialize;
    use near_crypto::{InMemorySigner, Signer};
    use near_primitives::hash::CryptoHash;
    use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum};
    use near_primitives::transaction::{
        Action, CreateAccountAction, DeployContractAction, ExecutionMetadata, ExecutionOutcome,
        ExecutionOutcomeWithProof, ExecutionStatus, FunctionCallAction, TransferAction,
    };
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::AccountId;
    use near_store::test_utils::{
        create_test_store, test_populate_store, test_populate_store_rc, test_populate_trie,
    };
    use near_store::{DBCol, ShardTries, ShardUId, Store, Trie};
    use std::fmt::Write;

    #[test]
    fn test_three_contract_sizes() {
        let initial = vec![
            contract_tuple("caroline.near", 3),
            contract_tuple("alice.near", 1),
            contract_tuple("alice.nearx", 2),
            // data right before contracts in trie order
            account_tuple("xeno.near", 1),
            // data right after contracts in trie order
            access_key_tuple("alan.near", 1),
        ];
        let trie = create_trie(initial);

        let filter = ContractAccountFilter { code_size: true, ..Default::default() };
        let contract_accounts: Vec<_> =
            ContractAccount::in_trie(trie, filter).expect("failed creating iterator").collect();
        assert_eq!(3, contract_accounts.len(), "wrong number of contracts returned by iterator");

        // expect reordering toe lexicographic order
        let contract1 = contract_accounts[0].as_ref().expect("returned error instead of contract");
        let contract2 = contract_accounts[1].as_ref().expect("returned error instead of contract");
        let contract3 = contract_accounts[2].as_ref().expect("returned error instead of contract");
        assert_eq!(contract1.account_id.as_str(), "alice.near");
        assert_eq!(contract2.account_id.as_str(), "alice.nearx");
        assert_eq!(contract3.account_id.as_str(), "caroline.near");
        assert_eq!(contract1.info.code_size, Some(1));
        assert_eq!(contract2.info.code_size, Some(2));
        assert_eq!(contract3.info.code_size, Some(3));
    }

    /// Check basic summary output and make sure the output looks right.
    #[test]
    fn test_simple_summary() {
        let trie_data = vec![contract_tuple("alice.near", 100), contract_tuple("bob.near", 200)];
        let (store, trie) = create_store_and_trie(&[], &[], trie_data);

        let filter = full_filter();
        let summary = ContractAccount::in_tries(vec![trie], &filter)
            .expect("iterator creation")
            .summary(&store, &filter);

        let mut buf = vec![];
        filter.write_header(&mut buf).unwrap();
        let mut output = String::from_utf8(buf).unwrap();
        write!(&mut output, "{summary}").unwrap();
        insta::assert_snapshot!(output, @r###"
        ACCOUNT_ID                                                         SIZE[B]   RCPTS_IN  RCPTS_OUT ACTIONS
        alice.near                                                             100          0          0 
        bob.near                                                               200          0          0 

        Finished without errors!
        "###);
    }

    /// Check summary output that contains a "full" output.
    #[test]
    fn test_summary() {
        // To have an interesting output we need a receipt, for which the
        // execution outcome spawns another receipt which has actions in it.

        // This is our original receipt, converted from a transaction, calling
        // bob.near::foo().
        let fn_call_receipt_id = CryptoHash::hash_borsh(1);
        let fn_call_receipt = create_receipt_with_actions(
            "alice.near",
            "bob.near",
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "foo".to_owned(),
                args: vec![],
                gas: 1000,
                deposit: 0,
            })],
        );

        // This is the receipt spawned, with the actions triggered by the
        // function call. It is sent by Bob and contains several actions. (And
        // executed by Alice but this should not matter for the tests here.)
        let outgoing_receipt_id = CryptoHash::hash_borsh(2);
        let outgoing_receipt = create_receipt_with_actions(
            "bob.near",
            "alice.near",
            vec![
                Action::Transfer(TransferAction { deposit: 20 }),
                Action::CreateAccount(CreateAccountAction {}),
                Action::DeployContract(DeployContractAction { code: vec![] }),
            ],
        );

        // And the outcome that links the two receipt above together.
        let fn_call_outcome = create_execution_outcome(vec![outgoing_receipt_id]);

        // Now prepare data to be inserted to DB, separating ref counted data.
        let store_data = [store_tripple(
            DBCol::TransactionResultForBlock,
            &fn_call_receipt_id,
            &fn_call_outcome,
        )];
        let store_data_rc = [
            store_tripple(DBCol::Receipts, &fn_call_receipt_id, &fn_call_receipt),
            store_tripple(DBCol::Receipts, &outgoing_receipt_id, &outgoing_receipt),
        ];

        let trie_data = vec![contract_tuple("alice.near", 100), contract_tuple("bob.near", 200)];
        let (store, trie) = create_store_and_trie(&store_data, &store_data_rc, trie_data);

        let filter = full_filter();
        let summary = ContractAccount::in_tries(vec![trie], &filter)
            .expect("iterator creation")
            .summary(&store, &filter);

        let mut buf = vec![];
        filter.write_header(&mut buf).unwrap();
        let mut output = String::from_utf8(buf).unwrap();
        write!(&mut output, "{summary}").unwrap();
        insta::assert_snapshot!(output, @r###"
        ACCOUNT_ID                                                         SIZE[B]   RCPTS_IN  RCPTS_OUT ACTIONS
        alice.near                                                             100          1          0 
        bob.near                                                               200          1          1 CreateAccount,DeployContract,Transfer

        Finished without errors!
        "###);
    }

    /// Create an in-memory trie with the key-value pairs.
    fn create_trie(initial: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Trie {
        create_store_and_trie(&[], &[], initial).1
    }

    /// Create an in-memory store + trie with key-value pairs for each.
    fn create_store_and_trie(
        store_data: &[(DBCol, Vec<u8>, Vec<u8>)],
        store_data_rc: &[(DBCol, Vec<u8>, Vec<u8>)],
        trie_data: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> (Store, Trie) {
        let store = create_test_store();
        test_populate_store(&store, store_data);
        test_populate_store_rc(&store, store_data_rc);
        let tries = ShardTries::test_shard_version(store.clone(), 0, 1);
        let root =
            test_populate_trie(&tries, &Trie::EMPTY_ROOT, ShardUId::single_shard(), trie_data);
        let trie = tries.get_trie_for_shard(ShardUId::single_shard(), root);
        (store, trie)
    }

    /// Create a test contract key-value pair to insert in the test trie, with specified amount of bytes.
    fn contract_tuple(account: &str, num_bytes: u8) -> (Vec<u8>, Option<Vec<u8>>) {
        (
            TrieKey::ContractCode { account_id: account.parse().unwrap() }.to_vec(),
            Some(vec![num_bytes; num_bytes as usize]),
        )
    }

    /// Create a test account key-value pair to insert in the test trie.
    fn account_tuple(account: &str, num: u8) -> (Vec<u8>, Option<Vec<u8>>) {
        (TrieKey::Account { account_id: account.parse().unwrap() }.to_vec(), Some(vec![num, num]))
    }

    /// Create a test access key key-value pair to insert in the test trie.
    fn access_key_tuple(account: &str, num: u8) -> (Vec<u8>, Option<Vec<u8>>) {
        (
            TrieKey::AccessKey {
                account_id: account.parse().unwrap(),
                public_key: near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519),
            }
            .to_vec(),
            Some(vec![num, num, num, num]),
        )
    }

    /// Creates an `ExecutionOutcomeWithProof` with the given outgoing receipts.
    ///
    /// Note that details such as the proof are invalid.
    fn create_execution_outcome(receipt_ids: Vec<CryptoHash>) -> ExecutionOutcomeWithProof {
        ExecutionOutcomeWithProof {
            proof: vec![],
            outcome: ExecutionOutcome {
                logs: vec![],
                receipt_ids,
                gas_burnt: 100,
                compute_usage: Some(200),
                tokens_burnt: 2000,
                executor_id: "someone.near".parse().unwrap(),
                status: ExecutionStatus::SuccessValue(vec![]),
                metadata: ExecutionMetadata::default(),
            },
        }
    }

    /// Convenience fn to create a triple to insert to the store.
    fn store_tripple(
        col: DBCol,
        key: &impl BorshSerialize,
        value: &impl BorshSerialize,
    ) -> (DBCol, Vec<u8>, Vec<u8>) {
        (
            col,
            borsh::ser::BorshSerialize::try_to_vec(key).unwrap(),
            borsh::ser::BorshSerialize::try_to_vec(value).unwrap(),
        )
    }

    /// A filter that collects all data.
    fn full_filter() -> ContractAccountFilter {
        ContractAccountFilter {
            code_size: true,
            actions: true,
            receipts_in: true,
            receipts_out: true,
            select_accounts: None,
            skip_accounts: None,
        }
    }

    /// Create a test receipt from sender to receiver with the given actions.
    fn create_receipt_with_actions(sender: &str, receiver: &str, actions: Vec<Action>) -> Receipt {
        let sender_id: AccountId = sender.parse().unwrap();
        let signer =
            InMemorySigner::from_seed(sender_id.clone(), near_crypto::KeyType::ED25519, "seed");
        Receipt {
            predecessor_id: sender_id.clone(),
            receiver_id: receiver.parse().unwrap(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: sender_id,
                signer_public_key: signer.public_key(),
                gas_price: 2,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions,
            }),
        }
    }
}
