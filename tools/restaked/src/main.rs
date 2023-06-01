use clap::{Arg, Command};
use near_crypto::{InMemorySigner, KeyFile};
use near_o11y::tracing::{error, info};
use near_primitives::views::CurrentEpochValidatorInfo;
use nearcore::config::{Config, BLOCK_PRODUCER_KICKOUT_THRESHOLD, CONFIG_FILENAME};
use nearcore::get_default_home;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
// TODO(1905): Move out RPC interface for transacting into separate production crate.
use integration_tests::user::{rpc_user::RpcUser, User};

const DEFAULT_WAIT_PERIOD_SEC: &str = "60";
const DEFAULT_RPC_URL: &str = "http://localhost:3030";

/// Returns true if given validator might get kicked out.
fn maybe_kicked_out(validator_info: &CurrentEpochValidatorInfo) -> bool {
    validator_info.num_produced_blocks * 100
        < validator_info.num_expected_blocks * u64::from(BLOCK_PRODUCER_KICKOUT_THRESHOLD)
}

fn main() {
    let env_filter = near_o11y::EnvFilterBuilder::from_env().verbose(Some("")).finish().unwrap();
    let _subscriber = near_o11y::default_subscriber(env_filter, &Default::default()).global();

    let matches = Command::new("Key-pairs generator")
        .about(
            "Continuously checking the node and executes staking transaction if node is kicked out",
        )
        .arg(
            Arg::new("home")
                .long("home")
                .default_value(get_default_home().into_os_string())
                .value_parser(clap::value_parser!(PathBuf))
                .help("Directory for config and data (default \"~/.near\")")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("wait-period")
                .long("wait-period")
                .default_value(DEFAULT_WAIT_PERIOD_SEC)
                .help("Waiting period between checking if node is kicked out (in seconds)")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("rpc-url")
                .long("rpc-url")
                .default_value(DEFAULT_RPC_URL)
                .help("Url of RPC for the node to monitor")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("stake-amount")
                .long("stake-amount")
                .default_value("0")
                .help("Stake amount in NEAR, if 0 is used it restakes last seen staked amount")
                .action(clap::ArgAction::Set),
        )
        .get_matches();

    let home_dir = matches.get_one::<PathBuf>("home").unwrap();
    let wait_period = matches
        .get_one::<String>("wait-period")
        .map(|s| s.parse().expect("Wait period must be a number"))
        .unwrap();
    let rpc_url = matches.get_one::<String>("rpc-url").unwrap();
    let stake_amount = matches
        .get_one::<String>("stake-amount")
        .map(|s| s.parse().expect("Stake amount must be a number"))
        .unwrap();

    let config = Config::from_file(&home_dir.join(CONFIG_FILENAME)).expect("can't load config");

    let key_path = home_dir.join(&config.validator_key_file);
    let key_file = KeyFile::from_file(&key_path)
        .unwrap_or_else(|e| panic!("Failed to open key file at {:?}: {:#}", &key_path, e));
    // Support configuring if there is another key.
    let signer = InMemorySigner::from_file(&key_path).unwrap_or_else(|e| {
        panic!("Failed to initialize signer from key file at {:?}: {:#}", key_path, e)
    });
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
            info!(
                target: "restaked",
                "Sending staking transaction {} -> {}", key_file.account_id, amount
            );
            if let Err(err) =
                user.stake(key_file.account_id.clone(), key_file.public_key.clone(), amount)
            {
                error!(target: "restaked", "Failed to send staking transaction: {}", err);
            }
        }
        std::thread::sleep(Duration::from_secs(wait_period));
    }
}
