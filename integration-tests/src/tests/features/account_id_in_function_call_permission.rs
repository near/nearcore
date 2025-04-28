use near_chain_configs::Genesis;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, Signer};
use near_parameters::RuntimeConfigStore;
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::errors::{ActionsValidationError, InvalidTxError};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, AddKeyAction, Transaction, TransactionV0};

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;

#[test]
fn test_invalid_account_id() {
    // Prepare TestEnv with a contract at the old protocol version.
    let env = {
        let epoch_length = 5;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        TestEnv::builder(&genesis.config)
            .nightshade_runtimes_with_runtime_config_store(
                &genesis,
                vec![RuntimeConfigStore::new(None)],
            )
            .build()
    };

    let signer: Signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx = TransactionV0 {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: signer.public_key(),
            access_key: AccessKey {
                nonce: 1,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: "#".to_string(),
                    method_names: vec![],
                }),
            },
        }))],
        nonce: 0,
        block_hash: CryptoHash::default(),
    };

    // Transaction fails due to invalid account id.
    {
        let tip = env.clients[0].chain.head().unwrap();
        let signed_transaction =
            Transaction::V0(TransactionV0 { nonce: 11, block_hash: tip.last_block_hash, ..tx })
                .sign(&signer);
        assert_eq!(
            env.rpc_handlers[0].process_tx(signed_transaction, false, false),
            ProcessTxResponse::InvalidTx(InvalidTxError::ActionsValidation(
                ActionsValidationError::InvalidAccountId { account_id: "#".to_string() }
            ))
        )
    };
}

#[test]
fn test_very_long_account_id() {
    let env = {
        let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        TestEnv::builder(&genesis.config)
            .nightshade_runtimes_with_runtime_config_store(
                &genesis,
                vec![RuntimeConfigStore::new(None)],
            )
            .build()
    };

    let tip = env.clients[0].chain.head().unwrap();
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx = Transaction::V0(TransactionV0 {
        signer_id: "test0".parse().unwrap(),
        receiver_id: "test0".parse().unwrap(),
        public_key: signer.public_key(),
        actions: vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: signer.public_key(),
            access_key: AccessKey {
                nonce: 1,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: "A".repeat(1024),
                    method_names: vec![],
                }),
            },
        }))],
        nonce: 0,
        block_hash: tip.last_block_hash,
    })
    .sign(&signer);

    assert_eq!(
        env.rpc_handlers[0].process_tx(tx, false, false),
        ProcessTxResponse::InvalidTx(InvalidTxError::ActionsValidation(
            ActionsValidationError::InvalidAccountId { account_id: "A".repeat(128) }
        ))
    )
}
