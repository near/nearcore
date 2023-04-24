use crate::tests::client::process_blocks::set_block_protocol_version;
use crate::tests::client::utils::TestEnvNightshadeSetupExt;
use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::migration_data::MigrationData;
use near_primitives::types::BlockHeight;
use near_primitives::version::ProtocolFeature;
use nearcore::config::GenesisExt;
use nearcore::migrations::load_migration_data;
use std::collections::HashSet;

const EPOCH_LENGTH: u64 = 5;
const HEIGHT_TIMEOUT: u64 = 10;

fn run_test(
    chain_id: &str,
    low_height_with_no_chunk: BlockHeight,
    high_height_with_no_chunk: BlockHeight,
    should_be_restored: bool,
) {
    init_test_logger();

    let protocol_version =
        ProtocolFeature::RestoreReceiptsAfterFixApplyChunks.protocol_version() - 1;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.chain_id = String::from(chain_id);
    genesis.config.epoch_length = EPOCH_LENGTH;
    genesis.config.protocol_version = protocol_version;
    let chain_genesis = ChainGenesis::new(&genesis);
    // TODO #4305: get directly from NightshadeRuntime
    let migration_data = load_migration_data(&genesis.config.chain_id);

    let mut env = TestEnv::builder(chain_genesis)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let get_restored_receipt_hashes = |migration_data: &MigrationData| -> HashSet<CryptoHash> {
        HashSet::from_iter(
            migration_data
                .restored_receipts
                .get(&0u64)
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|receipt| receipt.receipt_id),
        )
    };

    let mut receipt_hashes_to_restore = get_restored_receipt_hashes(&migration_data);
    let mut height: BlockHeight = 1;
    let mut last_update_height: BlockHeight = 0;

    // Simulate several blocks to guarantee that they are produced successfully.
    // Stop block production if all receipts were restored. Or, if some receipts are still not
    // applied, upgrade already happened, and no new receipt was applied in some last blocks,
    // consider the process stuck to avoid any possibility of infinite loop.
    while height < 15
        || (!receipt_hashes_to_restore.is_empty() && height - last_update_height < HEIGHT_TIMEOUT)
    {
        let mut block = env.clients[0].produce_block(height).unwrap().unwrap();
        if low_height_with_no_chunk <= height && height < high_height_with_no_chunk {
            let prev_block = env.clients[0].chain.get_block_by_height(height - 1).unwrap().clone();
            testlib::process_blocks::set_no_chunk_in_block(&mut block, &prev_block);
        }
        set_block_protocol_version(
            &mut block,
            "test0".parse().unwrap(),
            ProtocolFeature::RestoreReceiptsAfterFixApplyChunks.protocol_version(),
        );

        env.process_block(0, block, Provenance::PRODUCED);

        let last_block = env.clients[0].chain.get_block_by_height(height).unwrap().clone();
        let protocol_version = env.clients[0]
            .epoch_manager
            .get_epoch_protocol_version(last_block.header().epoch_id())
            .unwrap();

        for receipt_id in receipt_hashes_to_restore.clone().iter() {
            if env.clients[0].chain.get_execution_outcome(receipt_id).is_ok() {
                assert!(
                    protocol_version
                        >= ProtocolFeature::RestoreReceiptsAfterFixApplyChunks.protocol_version(),
                    "Restored receipt {} was executed before protocol upgrade",
                    receipt_id
                );
                receipt_hashes_to_restore.remove(receipt_id);
                last_update_height = height;
            };
        }

        // Update last updated height anyway if upgrade did not happen
        if protocol_version < ProtocolFeature::RestoreReceiptsAfterFixApplyChunks.protocol_version()
        {
            last_update_height = height;
        }
        height += 1;
    }

    if should_be_restored {
        assert!(
            receipt_hashes_to_restore.is_empty(),
            "Some of receipts were not executed, hashes: {:?}",
            receipt_hashes_to_restore
        );
    } else {
        assert_eq!(
            receipt_hashes_to_restore,
            get_restored_receipt_hashes(&migration_data),
            "If accidentally there are no chunks in first epoch with new protocol version, receipts should not be introduced"
        );
    }
}

#[test]
fn test_no_chunks_missing() {
    // If there are no chunks missing, all receipts should be applied
    run_test("mainnet", 1, 0, true);
}

#[test]
fn test_first_chunk_in_epoch_missing() {
    // If the first chunk in the first epoch with needed protocol version is missing,
    // all receipts should still be applied
    run_test("mainnet", 8, 12, true);
}

#[test]
fn test_all_chunks_in_epoch_missing() {
    // If all chunks are missing in the first epoch, no receipts should be applied
    run_test("mainnet", 11, 11 + EPOCH_LENGTH, false);
}

#[test]
fn test_run_for_testnet() {
    // Run the same process for chain other than mainnet to ensure that blocks are produced
    // successfully during the protocol upgrade.
    run_test("testnet", 1, 0, true);
}
