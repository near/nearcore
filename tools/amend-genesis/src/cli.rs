use near_primitives::types::NumBlocks;
use near_primitives::types::{BlockHeightDelta, NumSeats};
use near_primitives::version::ProtocolVersion;
use num_rational::Rational32;
use std::path::PathBuf;

/// Amend a genesis/records file created by `dump-state`.
#[derive(clap::Parser)]
pub struct AmendGenesisCommand {
    /// path to the input genesis file
    #[clap(long)]
    genesis_file_in: PathBuf,
    /// path to the output genesis file
    #[clap(long)]
    genesis_file_out: PathBuf,
    /// path to the input records file. Note that right now this must be provided, and
    /// this command will not work with a genesis file that itself contains the records
    #[clap(long)]
    records_file_in: PathBuf,
    /// path to the output records file
    #[clap(long)]
    records_file_out: PathBuf,
    /// path to a JSON list of AccountInfos representing the validators to put in the
    /// output genesis state. These are JSON maps of the form
    /// {
    ///   "account_id": <ACCOUNT_ID>,
    ///   "public_key": <PUBLIC_KEY>,
    ///   "amount": <STAKE>,
    /// }
    #[clap(long)]
    validators: PathBuf,
    /// path to extra records to add to the output state. Right now only Accounts and AccessKey
    /// records are supported, and any added accounts must have zero `code_hash`
    #[clap(long)]
    extra_records: Option<PathBuf>,
    /// chain ID to set on the output genesis
    #[clap(long)]
    chain_id: Option<String>,
    /// protocol version to set on the output genesis
    #[clap(long)]
    protocol_version: Option<ProtocolVersion>,
    /// num_seats to set in the output genesis file
    #[clap(long)]
    num_seats: Option<NumSeats>,
    /// epoch length to set in the output genesis file
    #[clap(long)]
    epoch_length: Option<BlockHeightDelta>,
    /// transaction_validity_period to set in the output genesis file
    #[clap(long)]
    transaction_validity_period: Option<NumBlocks>,
    /// block_producer_kickout_threshold to set in the output genesis file
    #[clap(long)]
    block_producer_kickout_threshold: Option<u8>,
    /// chunk_producer_kickout_threshold to set in the output genesis file
    #[clap(long)]
    chunk_producer_kickout_threshold: Option<u8>,
    /// protocol_reward_rate to set in the output genesis file. Give a ratio here (e.g. "1/10")
    #[clap(long)]
    protocol_reward_rate: Option<Rational32>,
    /// optional file that should contain a JSON-serialized shard layout
    #[clap(long)]
    shard_layout_file: Option<PathBuf>,
    /// runtime fees config `num_bytes_account` value. Used to initialize the `storage_usage` field
    /// on accounts in the output state
    #[clap(long)]
    num_bytes_account: Option<u64>,
    /// runtime fees config `num_extra_bytes_record` value. Used to initialize the `storage_usage` field
    /// on accounts in the output state
    #[clap(long)]
    num_extra_bytes_record: Option<u64>,
}

impl AmendGenesisCommand {
    pub fn run(self) -> anyhow::Result<()> {
        let genesis_changes = crate::GenesisChanges {
            chain_id: self.chain_id,
            protocol_version: self.protocol_version,
            num_seats: self.num_seats,
            epoch_length: self.epoch_length,
            transaction_validity_period: self.transaction_validity_period,
            protocol_reward_rate: self.protocol_reward_rate,
            block_producer_kickout_threshold: self.block_producer_kickout_threshold,
            chunk_producer_kickout_threshold: self.chunk_producer_kickout_threshold,
        };
        crate::amend_genesis(
            &self.genesis_file_in,
            &self.genesis_file_out,
            &self.records_file_in,
            &self.records_file_out,
            self.extra_records.as_deref(),
            &self.validators,
            self.shard_layout_file.as_deref(),
            &genesis_changes,
            self.num_bytes_account.unwrap_or(100),
            self.num_extra_bytes_record.unwrap_or(40),
        )
    }
}
