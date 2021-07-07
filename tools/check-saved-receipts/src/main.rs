use std::collections::{HashSet, HashMap};
use std::io::Result;
use std::iter::FromIterator;
use std::path::Path;

use clap::{App, Arg};

use near_chain::{ChainStore, ChainStoreAccess};
use near_primitives::receipt::Receipt;
use near_store::create_store;
use near_primitives::types::ShardId;
use neard::{get_default_home, get_store_path, load_config};

lazy_static_include::lazy_static_include_bytes! {
    /// File with receipts which were lost because of a bug in apply_chunks to the runtime config.
    /// Follows the ReceiptResult format which is HashMap<ShardId, Vec<Receipt>>.
    /// See https://github.com/near/nearcore/pull/4248/ for more details.
    MAINNET_RESTORED_RECEIPTS => "../../../mainnet_restored_receipts.json",
}

fn main() -> Result<()> {
    eprintln!("check-saved-receipts started");

    let default_home = get_default_home();
    let matches = App::new("check-saved-receipts")
        .arg(
            Arg::new("home")
                .default_value(&default_home)
                .about("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .get_matches();

    let home_dir = matches.value_of("home").map(Path::new).unwrap();
    let path = get_store_path(&home_dir);
    eprintln!("{}", path);
    let store = create_store(&path);
    let mut chain_store = ChainStore::new(store.clone(), 9820210);

    eprintln!("11111");
    let bytes = include_bytes!("../../../mainnet_restored_receipts.json");
    let restored_receipts: HashMap<ShardId, Vec<Receipt>> = serde_json::from_slice(bytes)
        .expect("File with receipts restored after apply_chunks fix have to be correct");
    eprintln!("22222");
    let receipts = restored_receipts.get(&0u64).unwrap();
    for receipt in receipts {
        eprintln!("{}", receipt.get_hash());
        chain_store.get_receipt(&receipt.get_hash()).unwrap().unwrap();
    }

    Ok(())
}
