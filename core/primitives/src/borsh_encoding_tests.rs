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
    ActionReceipt, ActionReceiptV2, DataReceipt, DataReceiver, Receipt, ReceiptEnum, ReceiptV0,
};
use crate::shard_layout::ShardLayout;
use crate::transaction::{
    NonceMode, SignedTransaction, Transaction, TransactionNonce, TransactionV0, TransactionV1,
};
use crate::types::validator_stake::ValidatorStake;
use crate::types::{AccountId, Balance, Gas, ProtocolVersion, ShardId, ValidatorKickoutReason};
use crate::universal_state_init::{UniversalStateInit, UniversalStateInitV1};
use crate::validator_mandates::{ValidatorMandates, ValidatorMandatesConfig};
use crate::validator_signer::InMemoryValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use insta::assert_snapshot;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signature, Signer};
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

/// Pins the encoding of `value` in snapshot `name`, then checks that the bytes
/// decode back to `value` and re-encode to the same bytes.
fn assert_encoding<T>(name: &str, value: &T)
where
    T: BorshSerialize + BorshDeserialize + PartialEq + Debug,
{
    let bytes = borsh::to_vec(value).unwrap();
    assert_snapshot!(name, hex_lines(&bytes));
    let decoded = T::try_from_slice(&bytes).unwrap();
    assert_eq!(&decoded, value);
    assert_eq!(borsh::to_vec(&decoded).unwrap(), bytes);
}

fn account_id(name: &str) -> AccountId {
    name.parse().unwrap()
}

fn signer(name: &str) -> Signer {
    InMemorySigner::from_seed(account_id(name), KeyType::ED25519, name)
}

fn public_key(name: &str) -> PublicKey {
    signer(name).public_key()
}

fn sign_transaction(signer: &Signer, transaction: Transaction) -> SignedTransaction {
    let (hash, _) = transaction.get_hash_and_size();
    SignedTransaction::new(signer.sign(hash.as_ref()), transaction)
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
    let alice = signer("alice.near");
    let transaction = Transaction::V0(TransactionV0 {
        signer_id: account_id("alice.near"),
        public_key: alice.public_key(),
        nonce: 42,
        receiver_id: account_id("bob.near"),
        block_hash: hash(b"block"),
        actions: all_actions(),
    });
    assert_encoding("signed_transaction_v0", &sign_transaction(&alice, transaction));
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
    assert_encoding("signed_transaction_v1", &sign_transaction(&alice, transaction));
}

#[test]
fn receipts() {
    let receipt = |receipt_id: &[u8], receipt| {
        Receipt::V0(ReceiptV0 {
            predecessor_id: account_id("alice.near"),
            receiver_id: account_id("bob.near"),
            receipt_id: hash(receipt_id),
            receipt,
        })
    };
    let output_data_receivers =
        vec![DataReceiver { data_id: hash(b"data"), receiver_id: account_id("carol.near") }];
    let actions = vec![Action::FunctionCall(Box::new(function_call("on_transfer")))];
    let receipts = vec![
        receipt(
            b"action",
            ReceiptEnum::Action(ActionReceipt {
                signer_id: account_id("alice.near"),
                signer_public_key: public_key("alice.near"),
                gas_price: Balance::from_yoctonear(100_000_000),
                output_data_receivers: output_data_receivers.clone(),
                input_data_ids: vec![hash(b"input")],
                actions: actions.clone(),
            }),
        ),
        receipt(
            b"action_v2",
            ReceiptEnum::ActionV2(ActionReceiptV2 {
                signer_id: account_id("alice.near"),
                refund_to: Some(account_id("refund.near")),
                signer_public_key: public_key("alice.near"),
                gas_price: Balance::from_yoctonear(100_000_000),
                output_data_receivers,
                input_data_ids: vec![hash(b"input")],
                actions,
            }),
        ),
        receipt(
            b"data",
            ReceiptEnum::Data(DataReceipt {
                data_id: hash(b"data"),
                data: Some(b"result".to_vec()),
            }),
        ),
    ];
    assert_encoding("receipts", &receipts);
}

#[test]
fn genesis_block_with_chunks() {
    let height = 100;
    let shard_ids = [ShardId::new(0), ShardId::new(1)];
    let chunks = genesis_chunks(
        vec![hash(b"state root")],
        vec![CongestionInfo::default(); shard_ids.len()],
        &shard_ids,
        Gas::from_teragas(1_000),
        height,
        PINNED_PROTOCOL_VERSION,
    );
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
        chunks.into_iter().map(|chunk| chunk.take_header()).collect(),
        Utc::from_unix_timestamp(1_700_000_000).unwrap(),
        height,
        Balance::from_yoctonear(100_000_000),
        Balance::from_near(1_000_000_000),
        &validator_stakes,
    );
    assert_encoding("genesis_block", &block);
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
    // HashMap keys are inserted out of order: borsh sorts them on encoding.
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
        let signer = InMemoryValidatorSigner::from_seed(account_id(name), KeyType::ED25519, name);
        Some(Box::new(Approval::new(parent_hash, 10, 11, &signer).signature))
    };
    let approvals: Vec<Option<Box<Signature>>> =
        vec![approval("alice.near"), None, approval("carol.near"), None];
    assert_encoding("optional_signatures", &approvals);
}
