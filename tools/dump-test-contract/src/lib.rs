use std::io::Write;

use clap::Parser;
use near_test_contracts::{
    backwards_compatible_rs_contract, congestion_control_test_contract, estimator_contract,
    ft_contract, fuzzing_contract, nightly_rs_contract, rs_contract, smallest_rs_contract,
    trivial_contract, ts_contract,
};

#[derive(Parser)]
pub struct DumpTestContractCommand {
    #[arg(short, long)]
    contract_name: String,
}

impl DumpTestContractCommand {
    pub fn run(&self) -> anyhow::Result<()> {
        let Some((name, extension)) = self.contract_name.rsplit_once(".") else {
            panic!("argument expected in `filename.wasm` form");
        };

        if extension != "wasm" {
            panic!("unsupported filetype `{extension}`");
        }

        let code = match &*name {
            "trivial" => trivial_contract(),
            "rs_contract" => rs_contract(),
            "nightly_rs_contract" => nightly_rs_contract(),
            "backwards_compatible_rs_contract" => backwards_compatible_rs_contract(),
            "ts_contract" => ts_contract(),
            "fuzzing_contract" => fuzzing_contract(),
            "ft_contract" => ft_contract(),
            "smallest_rs_contract" => smallest_rs_contract(),
            "estimator_contract" => estimator_contract(),
            "congestion_control_test_contract" => congestion_control_test_contract(),
            _ => panic!("unknown contract {name}"),
        };

        std::io::stdout().write_all(&code).expect("while writing code to stdout");

        Ok(())
    }
}
