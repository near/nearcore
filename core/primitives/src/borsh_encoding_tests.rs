//! Golden-bytes tests for the borsh encoding of core protocol types. They pin
//! the encoding so that a dependency upgrade cannot change it silently.
//!
//! Every value is deterministic: ed25519 keys come from fixed seeds, nothing
//! reads a clock, and constructors that branch on the protocol version get a
//! released version, whose features never move.

use crate::account::{
    AccessKey, AccessKeyPermission, Account, AccountContract, FunctionCallPermission, GasKeyInfo,
};
use crate::action::delegate::{
    DelegateAction, DelegateActionV2, NonDelegateAction, SignedDelegateAction,
    VersionedDelegateActionPayload, VersionedSignedDelegateAction,
};
use crate::action::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, DeployGlobalContractAction, DeterministicStateInitAction,
    FunctionCallAction, GlobalContractDeployMode, GlobalContractIdentifier, StakeAction,
    TransferAction, TransferToGasKeyAction, UniversalStateInitAction, UseGlobalContractAction,
    WithdrawFromGasKeyAction,
};
use crate::block_header::Approval;
use crate::congestion_info::CongestionInfo;
use crate::deterministic_account_id::{
    DeterministicAccountStateInit, DeterministicAccountStateInitV1,
};
use crate::epoch_info::EpochInfo;
use crate::genesis::{genesis_block, genesis_chunks};
use crate::hash::hash;
use crate::receipt::{
    ActionReceipt, ActionReceiptV2, DataReceipt, DataReceiver, GlobalContractDistributionReceipt,
    Receipt, ReceiptEnum, ReceiptV0, StateStoredReceipt, StateStoredReceiptMetadata,
};
use crate::shard_layout::ShardLayout;
use crate::sharding::ShardChunkHeader;
use crate::stateless_validation::chunk_endorsement::ChunkEndorsement;
use crate::test_utils::create_test_signer;
use crate::transaction::{NonceMode, Transaction, TransactionNonce, TransactionV0, TransactionV1};
use crate::types::validator_stake::ValidatorStake;
use crate::types::{
    AccountId, Balance, BlockHeight, EpochId, Gas, ProtocolVersion, ShardId, ValidatorKickoutReason,
};
use crate::universal_state_init::{UniversalStateInit, UniversalStateInitV1};
use crate::validator_mandates::{ValidatorMandates, ValidatorMandatesConfig};
use borsh::{BorshDeserialize, BorshSerialize};
use insta::assert_snapshot;
use near_crypto::{
    ED25519PublicKey, InMemorySigner, KeyType, ML_DSA_65_PUBLIC_KEY_LENGTH,
    ML_DSA_65_SIGNATURE_LENGTH, MlDsa65PublicKey, PublicKey, Secp256K1PublicKey, Signature, Signer,
};
use near_time::Utc;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;

/// A released protocol version. Released features are never renumbered, so
/// stabilizing a nightly feature cannot change the pinned bytes.
const PINNED_PROTOCOL_VERSION: ProtocolVersion = 86;

/// Lowercase hex, 32 bytes per line, so a snapshot diff points at the bytes
/// that changed.
fn hex_lines(bytes: &[u8]) -> String {
    bytes.chunks(32).map(hex::encode).collect::<Vec<_>>().join("\n")
}

/// Pins the encoding of `value` in snapshot `name`, then checks that
/// `object_length` agrees with it and that the bytes decode back to `value`
/// and re-encode to the same bytes.
fn assert_encoding<T>(name: &str, value: &T)
where
    T: BorshSerialize + BorshDeserialize + PartialEq + Debug,
{
    let bytes = borsh::to_vec(value).unwrap();
    assert_snapshot!(name, hex_lines(&bytes));
    assert_eq!(borsh::object_length(value).unwrap(), bytes.len());
    let decoded = T::try_from_slice(&bytes).unwrap();
    assert_eq!(&decoded, value);
    assert_eq!(borsh::to_vec(&decoded).unwrap(), bytes);
}

fn account_id(name: &str) -> AccountId {
    name.parse().unwrap()
}

fn signer(name: &str) -> Signer {
    InMemorySigner::test_signer(&account_id(name))
}

fn public_key(name: &str) -> PublicKey {
    signer(name).public_key()
}

fn function_call(method_name: &str) -> FunctionCallAction {
    FunctionCallAction {
        method_name: method_name.to_string(),
        args: br#"{"amount":"10"}"#.to_vec(),
        gas: Gas::from_teragas(30),
        deposit: Balance::from_yoctonear(1),
    }
}

fn nested_actions() -> Vec<NonDelegateAction> {
    let actions = [
        Action::Transfer(TransferAction { deposit: Balance::from_near(2) }),
        Action::FunctionCall(Box::new(function_call("ft_transfer"))),
    ];
    actions.into_iter().map(|action| action.try_into().unwrap()).collect()
}

/// One action of every `Action` variant, in discriminant order.
fn all_actions() -> Vec<Action> {
    let relayer = signer("relayer.near");
    let delegate = SignedDelegateAction::sign(
        &relayer,
        DelegateAction {
            sender_id: account_id("relayer.near"),
            receiver_id: account_id("token.near"),
            actions: nested_actions(),
            nonce: 17,
            max_block_height: 1_000,
            public_key: relayer.public_key(),
        },
    );
    let delegate_v2 = VersionedSignedDelegateAction::sign(
        &relayer,
        VersionedDelegateActionPayload::V2(DelegateActionV2 {
            sender_id: account_id("relayer.near"),
            receiver_id: account_id("token.near"),
            actions: nested_actions(),
            nonce: TransactionNonce::from_nonce_and_index(18, 2),
            max_block_height: 1_001,
            public_key: relayer.public_key(),
        }),
    );
    let data = BTreeMap::from([(b"zeta".to_vec(), b"1".to_vec()), (b"alpha".to_vec(), vec![])]);
    let universal_state_init = UniversalStateInit::V1(UniversalStateInitV1 {
        code: Some(GlobalContractIdentifier::AccountId(account_id("wallet.near"))),
        data: data.clone(),
        access_keys: BTreeSet::from([public_key("owner.near").into()]),
    });
    vec![
        Action::CreateAccount(CreateAccountAction {}),
        Action::DeployContract(DeployContractAction { code: b"\0asm-code".to_vec() }),
        Action::FunctionCall(Box::new(function_call("ft_transfer_call"))),
        Action::Transfer(TransferAction { deposit: Balance::from_near(5) }),
        Action::Stake(Box::new(StakeAction {
            stake: Balance::from_near(50_000),
            public_key: public_key("validator.near"),
        })),
        Action::AddKey(Box::new(AddKeyAction {
            public_key: public_key("app.near"),
            access_key: AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(Balance::from_millinear(250)),
                    receiver_id: "app.near".to_string(),
                    method_names: vec!["vote".to_string(), "withdraw".to_string()],
                }),
            },
        })),
        Action::DeleteKey(Box::new(DeleteKeyAction { public_key: public_key("old.near") })),
        Action::DeleteAccount(DeleteAccountAction { beneficiary_id: account_id("heir.near") }),
        Action::Delegate(Box::new(delegate)),
        Action::DeployGlobalContract(DeployGlobalContractAction {
            code: b"\0asm-global".to_vec().into(),
            deploy_mode: GlobalContractDeployMode::CodeHash,
        }),
        Action::UseGlobalContract(Box::new(UseGlobalContractAction {
            contract_identifier: GlobalContractIdentifier::CodeHash(hash(b"\0asm-global")),
        })),
        Action::DeterministicStateInit(Box::new(DeterministicStateInitAction {
            state_init: DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
                code: GlobalContractIdentifier::CodeHash(hash(b"\0asm-global")),
                data,
            }),
            deposit: Balance::from_near(1),
        })),
        Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: public_key("gas.near"),
            deposit: Balance::from_near(3),
        })),
        Action::WithdrawFromGasKey(Box::new(WithdrawFromGasKeyAction {
            public_key: public_key("gas.near"),
            amount: Balance::from_millinear(500),
        })),
        Action::DelegateV2(Box::new(delegate_v2)),
        Action::UniversalStateInit(Box::new(UniversalStateInitAction {
            state_init: universal_state_init.to_raw(),
            deposit: Balance::from_near(1),
        })),
    ]
}

#[test]
fn signed_transaction_v0() {
    let actions = all_actions();
    // No wildcard arm, so a new `Action` variant stops this compiling. Add it
    // to `all_actions` and extend the range.
    let tags: Vec<u8> = actions
        .iter()
        .map(|action| match action {
            Action::CreateAccount(_) => 0,
            Action::DeployContract(_) => 1,
            Action::FunctionCall(_) => 2,
            Action::Transfer(_) => 3,
            Action::Stake(_) => 4,
            Action::AddKey(_) => 5,
            Action::DeleteKey(_) => 6,
            Action::DeleteAccount(_) => 7,
            Action::Delegate(_) => 8,
            Action::DeployGlobalContract(_) => 9,
            Action::UseGlobalContract(_) => 10,
            Action::DeterministicStateInit(_) => 11,
            Action::TransferToGasKey(_) => 12,
            Action::WithdrawFromGasKey(_) => 13,
            Action::DelegateV2(_) => 14,
            Action::UniversalStateInit(_) => 15,
        })
        .collect();
    assert_eq!(tags, (0..=15).collect::<Vec<_>>());

    let alice = signer("alice.near");
    let transaction = Transaction::V0(TransactionV0 {
        signer_id: account_id("alice.near"),
        public_key: alice.public_key(),
        nonce: 42,
        receiver_id: account_id("bob.near"),
        block_hash: hash(b"block"),
        actions,
    });
    assert_encoding("signed_transaction_v0", &transaction.sign(&alice));
}

#[test]
fn signed_transaction_v1() {
    let alice = signer("alice.near");
    let transaction = Transaction::V1(TransactionV1 {
        signer_id: account_id("alice.near"),
        public_key: alice.public_key(),
        nonce: TransactionNonce::from_nonce_and_index(43, 7),
        receiver_id: account_id("bob.near"),
        block_hash: hash(b"block"),
        actions: vec![
            Action::Transfer(TransferAction { deposit: Balance::from_near(1) }),
            Action::FunctionCall(Box::new(function_call("ft_transfer"))),
        ],
        nonce_mode: NonceMode::Strict,
    });
    assert_encoding("signed_transaction_v1", &transaction.sign(&alice));
}

fn receipt(receipt_id: &[u8], receipt: ReceiptEnum) -> Receipt {
    Receipt::V0(ReceiptV0 {
        predecessor_id: account_id("alice.near"),
        receiver_id: account_id("bob.near"),
        receipt_id: hash(receipt_id),
        receipt,
    })
}

fn action_receipt() -> ActionReceipt {
    ActionReceipt {
        signer_id: account_id("alice.near"),
        signer_public_key: public_key("alice.near"),
        gas_price: Balance::from_yoctonear(100_000_000),
        output_data_receivers: vec![DataReceiver {
            data_id: hash(b"data"),
            receiver_id: account_id("carol.near"),
        }],
        input_data_ids: vec![hash(b"input")],
        actions: vec![Action::FunctionCall(Box::new(function_call("on_transfer")))],
    }
}

fn action_receipt_v2() -> ActionReceiptV2 {
    let ActionReceipt {
        signer_id,
        signer_public_key,
        gas_price,
        output_data_receivers,
        input_data_ids,
        actions,
    } = action_receipt();
    ActionReceiptV2 {
        signer_id,
        refund_to: Some(account_id("refund.near")),
        signer_public_key,
        gas_price,
        output_data_receivers,
        input_data_ids,
        actions,
    }
}

fn data_receipt() -> DataReceipt {
    DataReceipt { data_id: hash(b"data"), data: Some(b"result".to_vec()) }
}

#[test]
fn receipts() {
    let receipts = vec![
        receipt(b"action", ReceiptEnum::Action(action_receipt())),
        receipt(b"action_v2", ReceiptEnum::ActionV2(action_receipt_v2())),
        receipt(b"data", ReceiptEnum::Data(data_receipt())),
    ];
    assert_encoding("receipts", &receipts);
}

#[test]
fn receipts_other_variants() {
    let distribution = GlobalContractDistributionReceipt::new(
        GlobalContractIdentifier::AccountId(account_id("wallet.near")),
        ShardId::new(3),
        vec![ShardId::new(1), ShardId::new(4)],
        b"\0asm-global".to_vec().into(),
        9,
    );
    let receipts = vec![
        receipt(b"promise_yield", ReceiptEnum::PromiseYield(action_receipt())),
        receipt(b"promise_resume", ReceiptEnum::PromiseResume(data_receipt())),
        receipt(b"distribution", ReceiptEnum::GlobalContractDistribution(distribution)),
        receipt(b"promise_yield_v2", ReceiptEnum::PromiseYieldV2(action_receipt_v2())),
    ];
    assert_encoding("receipts_other_variants", &receipts);
}

/// The form delayed and buffered receipts take in the trie.
#[test]
fn state_stored_receipt() {
    let stored = StateStoredReceipt::new_owned(
        receipt(b"delayed", ReceiptEnum::Action(action_receipt())),
        StateStoredReceiptMetadata {
            congestion_gas: Gas::from_teragas(300),
            congestion_size: 1_234,
        },
    );
    assert_encoding("state_stored_receipt", &stored);
}

const GENESIS_HEIGHT: BlockHeight = 100;

fn genesis_chunk_headers() -> Vec<ShardChunkHeader> {
    let shard_ids = [ShardId::new(0), ShardId::new(1)];
    let chunks = genesis_chunks(
        vec![hash(b"state root")],
        vec![CongestionInfo::default(); shard_ids.len()],
        &shard_ids,
        Gas::from_teragas(1_000),
        GENESIS_HEIGHT,
        PINNED_PROTOCOL_VERSION,
    );
    chunks.into_iter().map(|chunk| chunk.take_header()).collect()
}

#[test]
fn genesis_block_with_chunks() {
    let validator_stakes = vec![
        ValidatorStake::new(
            account_id("alice.near"),
            public_key("alice.near"),
            Balance::from_near(100),
        ),
        ValidatorStake::new(account_id("bob.near"), public_key("bob.near"), Balance::from_near(50)),
    ];
    let block = genesis_block(
        PINNED_PROTOCOL_VERSION,
        genesis_chunk_headers(),
        Utc::from_unix_timestamp(1_700_000_000).unwrap(),
        GENESIS_HEIGHT,
        Balance::from_yoctonear(100_000_000),
        Balance::from_near(1_000_000_000),
        &validator_stakes,
    );
    assert_encoding("genesis_block", &block);
}

/// Its only variant has tag 1, not 0, so this pins that `use_discriminant` is
/// honored.
#[test]
fn chunk_endorsement() {
    let endorsement = ChunkEndorsement::new(
        EpochId(hash(b"epoch")),
        &genesis_chunk_headers()[1],
        &create_test_signer("alice.near"),
    );
    assert_encoding("chunk_endorsement", &endorsement);
}

#[test]
fn epoch_info_v5() {
    let names = ["alice.near", "bob.near", "carol.near"];
    let stakes = [Balance::from_near(300), Balance::from_near(200), Balance::from_near(100)];
    let validators: Vec<_> = names
        .iter()
        .zip(stakes)
        .map(|(name, stake)| ValidatorStake::new(account_id(name), public_key(name), stake))
        .collect();
    // HashMap order is random per process. Borsh sorts the keys, so the bytes are stable.
    let validator_to_index = HashMap::from([
        (account_id("carol.near"), 2),
        (account_id("alice.near"), 0),
        (account_id("bob.near"), 1),
    ]);
    let stake_change = BTreeMap::from([
        (account_id("bob.near"), Balance::from_near(200)),
        (account_id("alice.near"), Balance::from_near(300)),
    ]);
    let validator_reward = HashMap::from([
        (account_id("bob.near"), Balance::from_near(2)),
        (account_id("carol.near"), Balance::from_near(1)),
        (account_id("alice.near"), Balance::from_near(3)),
    ]);
    let validator_kickout = HashMap::from([
        (
            account_id("ivan.near"),
            ValidatorKickoutReason::ProtocolVersionTooOld { version: 85, network_version: 86 },
        ),
        (
            account_id("dave.near"),
            ValidatorKickoutReason::NotEnoughBlocks { produced: 1, expected: 10 },
        ),
        (
            account_id("heidi.near"),
            ValidatorKickoutReason::NotEnoughChunkEndorsements { produced: 3, expected: 30 },
        ),
        (
            account_id("eve.near"),
            ValidatorKickoutReason::NotEnoughChunks { produced: 2, expected: 20 },
        ),
        (
            account_id("grace.near"),
            ValidatorKickoutReason::NotEnoughStake {
                stake: Balance::from_near(5),
                threshold: Balance::from_near(10),
            },
        ),
        (account_id("frank.near"), ValidatorKickoutReason::Unstaked),
        (account_id("judy.near"), ValidatorKickoutReason::DidNotGetASeat),
    ]);
    let shard_layout = ShardLayout::v3(
        vec![account_id("aurora"), account_id("sweat")],
        vec![ShardId::new(1), ShardId::new(3), ShardId::new(4)],
        BTreeMap::from([
            (ShardId::new(2), vec![ShardId::new(3), ShardId::new(4)]),
            (ShardId::new(0), vec![ShardId::new(1), ShardId::new(2)]),
        ]),
        ShardId::new(2),
    );
    let validator_mandates =
        ValidatorMandates::new(ValidatorMandatesConfig::new(2, 3), &validators);
    let epoch_info = EpochInfo::new(
        7,
        validators,
        validator_to_index,
        vec![0, 1, 2],
        vec![vec![0, 1], vec![1, 2], vec![2, 0]],
        stake_change,
        validator_reward,
        validator_kickout,
        Balance::from_near(10),
        Balance::from_near(50),
        PINNED_PROTOCOL_VERSION,
        [7; 32],
        validator_mandates,
        shard_layout,
        Some(5),
    );
    assert!(matches!(epoch_info, EpochInfo::V5(_)));
    assert_encoding("epoch_info_v5", &epoch_info);
}

#[test]
fn accounts_and_access_keys() {
    let accounts = vec![
        Account::new(
            Balance::from_near(10),
            Balance::from_near(1),
            AccountContract::Local(hash(b"\0asm-code")),
            182,
        ),
        Account::new(
            Balance::from_near(20),
            Balance::ZERO,
            AccountContract::GlobalByAccount(account_id("wallet.near")),
            200,
        ),
        Account::new_uninitialized(Balance::from_near(30), 100, 99_000_000),
    ];
    assert_encoding("accounts", &accounts);

    let function_call_permission = FunctionCallPermission {
        allowance: None,
        receiver_id: "app.near".to_string(),
        method_names: vec!["vote".to_string()],
    };
    let gas_key_info = GasKeyInfo { balance: Balance::from_near(1), num_nonces: 4 };
    let access_keys = vec![
        AccessKey { nonce: 1, permission: AccessKeyPermission::FullAccess },
        AccessKey {
            nonce: 2,
            permission: AccessKeyPermission::FunctionCall(function_call_permission.clone()),
        },
        AccessKey {
            nonce: 3,
            permission: AccessKeyPermission::GasKeyFunctionCall(
                gas_key_info.clone(),
                function_call_permission,
            ),
        },
        AccessKey { nonce: 4, permission: AccessKeyPermission::GasKeyFullAccess(gas_key_info) },
    ];
    assert_encoding("access_keys", &access_keys);
}

/// The shape of block approvals and chunk endorsements.
#[test]
fn optional_signatures() {
    let parent_hash = hash(b"parent");
    let approval = |name: &str| {
        Some(Box::new(Approval::new(parent_hash, 10, 11, &create_test_signer(name)).signature))
    };
    let approvals: Vec<Option<Box<Signature>>> =
        vec![approval("alice.near"), None, approval("carol.near"), None];
    assert_encoding("optional_signatures", &approvals);
}

/// Fixed key and signature bytes. Decoding never checks that they are valid,
/// and ML-DSA signing may be randomized. Every byte stays below 0x20, which
/// the ed25519 signature decoder requires of the last byte.
fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 32) as u8).collect()
}

#[test]
fn keys_and_signatures() {
    let public_keys = vec![
        PublicKey::ED25519(ED25519PublicKey::try_from(&patterned(32)[..]).unwrap()),
        PublicKey::SECP256K1(Secp256K1PublicKey::try_from(&patterned(64)[..]).unwrap()),
        PublicKey::MLDSA65(
            MlDsa65PublicKey::try_from(&patterned(ML_DSA_65_PUBLIC_KEY_LENGTH)[..]).unwrap(),
        ),
    ];
    let signatures = vec![
        Signature::from_parts(KeyType::ED25519, &patterned(64)).unwrap(),
        Signature::from_parts(KeyType::SECP256K1, &patterned(65)).unwrap(),
        Signature::from_parts(KeyType::MLDSA65, &patterned(ML_DSA_65_SIGNATURE_LENGTH)).unwrap(),
    ];
    assert_encoding("keys_and_signatures", &(public_keys, signatures));
}
