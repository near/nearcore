//! History: these tests replace three pytests from October 2019.
//! `sanity/staking2.py` (#1384) sent random stake txs and checked locked
//! balances. `sanity/staking_repro1.py` (#1384) replayed a sequence that
//! stopped a node on the staking invariant check. The likely fix is #1415,
//! completed by #1432, which keeps the last chunk's proposal locked at the
//! epoch boundary (`last_proposal` in `update_validator_accounts`).
//! `sanity/staking_repro2.py` (#1423) replayed a wrong kickout when an account
//! unstakes and then stakes in the same epoch, fixed by #1425.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_validator_ids;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_chain_configs::test_utils::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, StakeAction};
use near_primitives::num_rational::Rational32;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, AccountInfo, Balance, BlockHeight};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::array;

const EPOCH_LENGTH: u64 = 18;
/// Endorsement delay of the SPICE late-stake tests. It is larger than the
/// number of blocks between the late chunk and the epoch end, so a block of
/// the next epoch certifies the chunk.
const EXECUTION_DELAY: u64 = 4;
const GENESIS_STAKES: [Balance; 3] = [TESTING_INIT_STAKE, TESTING_INIT_STAKE, Balance::ZERO];

#[test]
fn test_staking_random_stakes() {
    // Setup: in each round, one or two of the three accounts stake a random
    // amount and the others unstake. 21 rounds give 11 final rounds over 11
    // epochs.
    let mut rng = StdRng::seed_from_u64(0);
    let rounds: Vec<[Balance; 3]> = (0..21).map(|_| random_stakes(&mut rng)).collect();
    run_stake_rounds(&rounds);
}

#[test]
fn test_staking_repro1_sequence() {
    run_stake_rounds(&sequence_then_random_rounds(&REPRO1_STAKES));
}

#[test]
fn test_staking_repro2_sequence() {
    run_stake_rounds(&sequence_then_random_rounds(&REPRO2_STAKES));
}

#[test]
fn test_staking_stake_in_last_block_of_epoch() {
    test_staking_late_stake_impl(0, 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_staking_stake_in_last_block_of_epoch_delayed_execution() {
    test_staking_late_stake_impl(EXECUTION_DELAY, 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_staking_stake_in_second_to_last_block_of_epoch_delayed_execution() {
    test_staking_late_stake_impl(EXECUTION_DELAY, 2);
}

/// Sends one round of stake txs per epoch, included `blocks_before_epoch_end`
/// blocks before the end of the epoch.
fn test_staking_late_stake_impl(execution_delay: u64, blocks_before_epoch_end: u64) {
    let (mut env, accounts) = setup_env(execution_delay);
    let first_epoch_start = env.rpc_node().head().height;
    let mut rng = StdRng::seed_from_u64(1);
    let num_rounds = 10;

    // Setup: the proposals of each round count in the next epoch. Without
    // SPICE, the chunk header that carries them is in the first block of the
    // next epoch. With SPICE, a block of the next epoch certifies their chunk.
    // At the end of epoch E, the stake return keeps the largest stake of
    // epochs E, E+1 and E+2 and also the last proposal, here the round sent
    // late in E (`last_proposal` in `update_validator_accounts`). The epoch
    // infos of E, E+1 and E+2 hold the rounds sent late in E-3, E-2 and E-1.
    // Thus the locked balance is the largest of the last four rounds, counting
    // the genesis stakes as the first round.
    let mut stake_rounds = vec![GENESIS_STAKES];
    for round in 0..num_rounds {
        let epoch_start = first_epoch_start + round * EPOCH_LENGTH;
        // The stake return of the boundary runs in the epoch's first block.
        env.rpc_runner().run_until_head_height(epoch_start);
        check_locked(&mut env, &accounts, max_stakes(&stake_rounds, 4));

        // Txs submitted at head `target_height - 2` go into the chunk for
        // `target_height`. The assert below checks it.
        let target_height = epoch_start + EPOCH_LENGTH - blocks_before_epoch_end;
        env.rpc_runner().run_until_head_height(target_height - 2);
        // The test's endorsement delay applies only to endorsements sent over
        // the network, not to a validator's own. A validator with 2/3 of the
        // stake would certify the late chunk without delay. Every account
        // stakes 70M to 100M NEAR, so the largest share is at most
        // 100 / (100 + 70 + 70), about 42%. The genesis epochs have two
        // validators at 50% each.
        let stakes = array::from_fn(|_| random_stake(&mut rng));
        let inclusion_height = send_stakes(&mut env, &accounts, &stakes);
        assert_eq!(inclusion_height, target_height, "stake txs not in the target block");
        if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
            let inclusion_epoch = env.rpc_node().head().epoch_id;
            env.rpc_runner().run_until_certified(inclusion_height);
            assert_ne!(
                env.rpc_node().head().epoch_id,
                inclusion_epoch,
                "stake txs certified in their own epoch"
            );
        }
        stake_rounds.push(stakes);
    }

    // Check: the rule above holds after the last epoch boundary too.
    let final_check_height = first_epoch_start + num_rounds * EPOCH_LENGTH;
    env.rpc_runner().run_until_head_height(final_check_height);
    check_locked(&mut env, &accounts, max_stakes(&stake_rounds, 4));
}

/// The sequence, then six random rounds. The random rounds replace the
/// sequence's stakes in the last-three-epochs window, so the checks also show
/// how the sequence's last proposals took effect.
fn sequence_then_random_rounds(sequence: &[[u128; 3]]) -> Vec<[Balance; 3]> {
    let mut rng = StdRng::seed_from_u64(2);
    let random_rounds = (0..6).map(|_| random_stakes(&mut rng));
    sequence.iter().map(|stakes| stakes.map(Balance::from_yoctonear)).chain(random_rounds).collect()
}

/// One or two of the three accounts get a random stake; the others get zero.
fn random_stakes(rng: &mut StdRng) -> [Balance; 3] {
    let mut stakes = [Balance::ZERO; 3];
    for _ in 0..2 {
        stakes[rng.gen_range(0..3)] = random_stake(rng);
    }
    stakes
}

/// A random stake between 70M and 100M NEAR.
fn random_stake(rng: &mut StdRng) -> Balance {
    Balance::from_near(rng.gen_range(70_000_000..=100_000_000))
}

/// Rounds alternate "final" and "replaced", starting with a final round. A
/// replaced round is sent at `replaced_proposal_offset` of an epoch and a final
/// round at `final_proposal_offset` of the same epoch. The epoch manager keeps
/// only an account's last proposal in an epoch, so the final round replaces the
/// other. A stake increase locks tokens when its tx executes, so a replaced
/// stake is locked until the end of its epoch. Before each round, and after the
/// last one, each account's locked balance must be the largest of its last
/// three final stakes and its replaced stake in the current epoch, counting the
/// genesis stakes as the first final round.
fn run_stake_rounds(rounds: &[[Balance; 3]]) {
    let (mut env, accounts) = setup_env(0);
    let first_epoch_start = env.rpc_node().head().height;
    let replaced_proposal_offset = 6;
    let final_proposal_offset = 12;

    let mut final_stakes = vec![GENESIS_STAKES];
    let mut replaced_stakes = [Balance::ZERO; 3];
    for (round, stakes) in (0u64..).zip(rounds) {
        let is_final = round % 2 == 0;
        let epoch_index = (round + 1) / 2;
        let offset = if is_final { final_proposal_offset } else { replaced_proposal_offset };
        env.rpc_runner()
            .run_until_head_height(first_epoch_start + epoch_index * EPOCH_LENGTH + offset);

        check_locked(&mut env, &accounts, expected_locked(&final_stakes, replaced_stakes));

        send_stakes(&mut env, &accounts, stakes);
        if is_final {
            final_stakes.push(*stakes);
            replaced_stakes = [Balance::ZERO; 3];
        } else {
            replaced_stakes = *stakes;
        }
    }

    // Check: the rule above holds after the last epoch boundary too.
    // The last round is in epoch `rounds.len() / 2`.
    let last_epoch_index = rounds.len() as u64 / 2 + 1;
    env.rpc_runner().run_until_head_height(
        first_epoch_start + last_epoch_index * EPOCH_LENGTH + replaced_proposal_offset,
    );
    check_locked(&mut env, &accounts, expected_locked(&final_stakes, replaced_stakes));
}

/// Starts validator0 and validator1 as genesis validators and validator2 as a
/// non-validator node. There are 3 block and chunk producer seats, so every
/// account that stakes enough becomes a validator. Returns with the head at the
/// first block of an epoch.
fn setup_env(execution_delay: u64) -> (TestLoopEnv, Vec<AccountId>) {
    init_test_logger();

    let accounts = create_validator_ids(3);
    let validators: Vec<AccountInfo> = accounts[..2]
        .iter()
        .map(|account| AccountInfo {
            account_id: account.clone(),
            public_key: create_test_signer(account.as_str()).public_key(),
            amount: TESTING_INIT_STAKE,
        })
        .collect();
    let mut env = TestLoopBuilder::new()
        .epoch_length(EPOCH_LENGTH)
        .validators_spec(ValidatorsSpec::raw(validators, 3, 3, 0))
        .add_non_validator_client(&accounts[2])
        .add_user_accounts(&accounts, TESTING_INIT_BALANCE)
        // Without rewards, locked balances are exactly the expected stakes.
        .max_inflation_rate(Rational32::new(0, 1))
        .enable_rpc()
        .delay_warmup()
        .build();
    env.delay_endorsements_propagation(execution_delay);
    let mut env = env.warmup();

    env.rpc_runner().run_until_new_epoch();
    (env, accounts)
}

/// Submits one stake tx per account to the RPC node. Returns the height of
/// the block that includes them.
fn send_stakes(
    env: &mut TestLoopEnv,
    accounts: &[AccountId],
    stakes: &[Balance; 3],
) -> BlockHeight {
    let tx_hashes: Vec<_> = accounts
        .iter()
        .zip(stakes)
        .map(|(account, stake)| {
            let rpc = env.rpc_node();
            let tx = rpc.tx_from_actions(
                account,
                account,
                vec![Action::Stake(Box::new(StakeAction {
                    stake: *stake,
                    public_key: create_test_signer(account.as_str()).public_key(),
                }))],
            );
            rpc.submit_tx(tx)
        })
        .collect();
    env.rpc_runner().run_until_included(&tx_hashes)
}

/// The largest of each account's last three final stakes and its replaced
/// stake.
fn expected_locked(final_stakes: &[[Balance; 3]], replaced_stakes: [Balance; 3]) -> [Balance; 3] {
    let final_max = max_stakes(final_stakes, 3);
    array::from_fn(|i| final_max[i].max(replaced_stakes[i]))
}

/// The largest stake of each account in the last `window` rounds.
fn max_stakes(stake_rounds: &[[Balance; 3]], window: usize) -> [Balance; 3] {
    let recent = &stake_rounds[stake_rounds.len().saturating_sub(window)..];
    array::from_fn(|i| recent.iter().map(|stakes| stakes[i]).max().unwrap())
}

/// Waits until the RPC node has executed its head block, then checks each
/// account's locked balance.
fn check_locked(env: &mut TestLoopEnv, accounts: &[AccountId], expected: [Balance; 3]) {
    let head_height = env.rpc_node().head().height;
    env.rpc_runner().run_until_executed_height(head_height);
    for (account, expected) in accounts.iter().zip(expected) {
        let locked = env.rpc_node().view_account_query(account).unwrap().locked;
        assert_eq!(locked, expected, "locked balance of {account}");
    }
}

const REPRO1_STAKES: [[u128; 3]; 31] = [
    [0, 73_215_476_000_000_000_000_000_000_000_000, 73_203_310_000_000_000_000_000_000_000_000],
    [0, 95_711_839_000_000_000_000_000_000_000_000, 75_774_073_000_000_000_000_000_000_000_000],
    [0, 95_711_839_000_000_000_000_000_000_000_000, 75_774_073_000_000_000_000_000_000_000_000],
    [0, 84_382_393_000_000_000_000_000_000_000_000, 98_749_818_000_000_000_000_000_000_000_000],
    [0, 84_382_393_000_000_000_000_000_000_000_000, 98_749_818_000_000_000_000_000_000_000_000],
    [0, 0, 90_224_511_000_000_000_000_000_000_000_000],
    [0, 0, 90_224_511_000_000_000_000_000_000_000_000],
    [86_432_337_000_000_000_000_000_000_000_000, 76_422_536_000_000_000_000_000_000_000_000, 0],
    [86_432_337_000_000_000_000_000_000_000_000, 76_422_536_000_000_000_000_000_000_000_000, 0],
    [75_063_341_000_000_000_000_000_000_000_000, 0, 80_786_582_000_000_000_000_000_000_000_000],
    [75_063_341_000_000_000_000_000_000_000_000, 0, 80_786_582_000_000_000_000_000_000_000_000],
    [86_164_122_000_000_000_000_000_000_000_000, 0, 73_964_803_000_000_000_000_000_000_000_000],
    [86_164_122_000_000_000_000_000_000_000_000, 0, 73_964_803_000_000_000_000_000_000_000_000],
    [70_076_530_000_000_000_000_000_000_000_000, 0, 0],
    [70_076_530_000_000_000_000_000_000_000_000, 0, 0],
    [0, 78_553_537_000_000_000_000_000_000_000_000, 0],
    [0, 78_553_537_000_000_000_000_000_000_000_000, 0],
    [85_364_825_000_000_000_000_000_000_000_000, 0, 81_322_978_000_000_000_000_000_000_000_000],
    [85_364_825_000_000_000_000_000_000_000_000, 0, 81_322_978_000_000_000_000_000_000_000_000],
    [98_064_272_000_000_000_000_000_000_000_000, 0, 0],
    [98_064_272_000_000_000_000_000_000_000_000, 0, 0],
    [0, 0, 78_616_653_000_000_000_000_000_000_000_000],
    [0, 0, 78_616_653_000_000_000_000_000_000_000_000],
    [99_017_960_000_000_000_000_000_000_000_000, 78_066_788_000_000_000_000_000_000_000_000, 0],
    [99_017_960_000_000_000_000_000_000_000_000, 78_066_788_000_000_000_000_000_000_000_000, 0],
    [83_587_257_000_000_000_000_000_000_000_000, 0, 0],
    [83_587_257_000_000_000_000_000_000_000_000, 0, 0],
    [81_625_375_000_000_000_000_000_000_000_000, 0, 90_569_985_000_000_000_000_000_000_000_000],
    [81_625_375_000_000_000_000_000_000_000_000, 0, 90_569_985_000_000_000_000_000_000_000_000],
    [76_171_853_000_000_000_000_000_000_000_000, 70_811_780_000_000_000_000_000_000_000_000, 0],
    [76_171_853_000_000_000_000_000_000_000_000, 70_811_780_000_000_000_000_000_000_000_000, 0],
];

const REPRO2_STAKES: [[u128; 3]; 5] = [
    [0, 89_807_699_575_087_731_503_000_000_000_000, 99_394_381_434_196_724_661_000_000_000_000],
    [0, 78_422_984_975_403_936_749_000_000_000_000, 86_824_329_382_096_781_900_000_000_000_000],
    [96_998_170_601_935_325_415_000_000_000_000, 0, 96_905_754_147_675_144_777_000_000_000_000],
    [0, 90_761_562_581_683_912_450_000_000_000_000, 85_484_232_564_481_713_683_000_000_000_000],
    [0, 88_770_343_317_484_058_757_000_000_000_000, 94_113_873_883_235_846_136_000_000_000_000],
];
