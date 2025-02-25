use near_chain_configs::Genesis;
use near_primitives::errors::{ActionError, ActionErrorKind};
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::views::FinalExecutionStatus;
use near_primitives_core::version::PROTOCOL_VERSION;

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use crate::utils::process_blocks::create_account;

#[test]
fn test_create_top_level_accounts() {
    let epoch_length: BlockHeight = 5;
    let account: AccountId = "test0".parse().unwrap();
    let mut genesis = Genesis::test(vec![account.clone()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = PROTOCOL_VERSION;
    let runtime_config = near_parameters::RuntimeConfigStore::new(None);
    let mut env = TestEnv::builder(&genesis.config)
        .nightshade_runtimes_with_runtime_config_store(&genesis, vec![runtime_config])
        .build();

    // These accounts cannot be created because they are top level accounts that are not implicit.
    // Note that implicit accounts have to be 64 or 42 (if starts with '0x') characters long.
    let top_level_accounts = [
        "0x06012c8cf97bead5deae237070f9587f8e7a266da",
        "0a5e97870f263700f46aa00d967821199b9bc5a120",
        "0x000000000000000000000000000000000000000",
        "alice",
        // cspell:disable-next-line
        "thisisaveryverylongtoplevelaccount",
    ];
    for (index, id) in top_level_accounts.iter().enumerate() {
        let new_account_id = id.parse::<AccountId>().unwrap();
        let tx_hash = create_account(
            &mut env,
            account.clone(),
            new_account_id.clone(),
            epoch_length,
            1 + index as u64 * epoch_length,
            PROTOCOL_VERSION,
        );
        let transaction_result =
            env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
        assert_eq!(
            transaction_result.status,
            FinalExecutionStatus::Failure(
                ActionError {
                    index: Some(0),
                    kind: ActionErrorKind::CreateAccountOnlyByRegistrar {
                        account_id: new_account_id,
                        registrar_account_id: "registrar".parse().unwrap(),
                        predecessor_id: account.clone()
                    }
                }
                .into()
            )
        );
    }
}
