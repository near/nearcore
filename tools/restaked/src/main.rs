use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clap::{App, Arg};
use log::{error, info, LevelFilter};

use near_crypto::{InMemorySigner, KeyFile};
use near_primitives::views::CurrentEpochValidatorInfo;
use neard::config::{Config, BLOCK_PRODUCER_KICKOUT_THRESHOLD, CONFIG_FILENAME};
use neard::get_default_home;
// TODO(1905): Move out RPC interface for transacting into separate production crate.
use testlib::user::{rpc_user::RpcUser, User};

const DEFAULT_WAIT_PERIOD_SEC: &str = "60";
const DEFAULT_RPC_URL: &str = "http://localhost:3030";

/// Returns true if given validator might get kicked out.
fn maybe_kicked_out(validator_info: &CurrentEpochValidatorInfo) -> bool {
    validator_info.num_produced_blocks * 100
        < validator_info.num_expected_blocks * u64::from(BLOCK_PRODUCER_KICKOUT_THRESHOLD)
}

fn main() {
    env_logger::Builder::new().filter(None, LevelFilter::Info).init();
    let default_home = get_default_home();
    let matches = App::new("Key-pairs generator")
        .about(
            "Continuously checking the node and executes staking transaction if node is kicked out",
        )
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("wait-period")
                .long("wait-period")
                .default_value(DEFAULT_WAIT_PERIOD_SEC)
                .help("Waiting period between checking if node is kicked out (in seconds)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("rpc-url")
                .long("rpc-url")
                .default_value(DEFAULT_RPC_URL)
                .help("Url of RPC for the node to monitor")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("stake-amount")
                .long("stake-amount")
                .default_value("0")
                .help("Stake amount in NEAR, if 0 is used it restakes last seen staked amount")
                .takes_value(true),
        )
        .get_matches();

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    let wait_period = matches
        .value_of("wait-period")
        .map(|s| s.parse().expect("Wait period must be a number"))
        .unwrap();
    let rpc_url = matches.value_of("rpc-url").unwrap();
    let stake_amount = matches
        .value_of("stake-amount")
        .map(|s| s.parse().expect("Stake amount must be a number"))
        .unwrap();

    let config = Config::from_file(&home_dir.join(CONFIG_FILENAME));
    let key_file = KeyFile::from_file(&home_dir.join(&config.validator_key_file));
    // Support configuring if there is another key.
    let signer = InMemorySigner::from_file(&home_dir.join(&config.validator_key_file));
    let account_id = signer.account_id.clone();
    let mut last_stake_amount = stake_amount;

    assert_eq!(
        signer.account_id, key_file.account_id,
        "Only can stake for the same account as given signer key"
    );

    let user = RpcUser::new(rpc_url, account_id.clone(), Arc::new(signer));
    loop {
        let validators = user.validators(None).unwrap();
        // Check:
        //  - don't already have a proposal
        //  - too many missing blocks in current validators
        //  - missing in next validators
        if validators.current_proposals.iter().any(|proposal| proposal.account_id() == &account_id)
        {
            continue;
        }
        let mut restake = false;
        validators
            .current_validators
            .iter()
            .filter(|validator_info| validator_info.account_id == account_id)
            .last()
            .map(|validator_info| {
                last_stake_amount = validator_info.stake;
                if maybe_kicked_out(validator_info) {
                    restake = true;
                }
            });
        restake |= !validators
            .next_validators
            .iter()
            .any(|validator_info| validator_info.account_id == account_id);
        if restake {
            // Already kicked out or getting kicked out.
            let amount = if stake_amount == 0 { last_stake_amount } else { stake_amount };
            info!("Sending staking transaction {} -> {}", key_file.account_id, amount);
            if let Err(err) =
                user.stake(key_file.account_id.clone(), key_file.public_key.clone(), amount)
            {
                error!("Failed to send staking transaction: {}", err);
            }
        }
        std::thread::sleep(Duration::from_secs(wait_period));
    }
}
