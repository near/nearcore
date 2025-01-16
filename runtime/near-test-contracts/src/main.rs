use std::io::Write as _;

use near_test_contracts::*;

fn main() {
    let contract_name = std::env::args()
        .nth(1)
        .expect("expecting one argument with the name of the contract to return");
    let Some((name, extension)) = contract_name.rsplit_once(".") else {
        panic!("argument expected in `filename.ext` form");
    };
    let code = match &*name {
        "trivial" => trivial_contract(),
        "rs_contract" => rs_contract(),
        "backwards_compatible_rs_contract" => backwards_compatible_rs_contract(),
        "nightly_rs_contract" => nightly_rs_contract(),
        "ts_contract" => ts_contract(),
        "fuzzing_contract" => fuzzing_contract(),
        "ft_contract" => ft_contract(),
        "smallest_rs_contract" => smallest_rs_contract(),
        "estimator_contract" => estimator_contract(),
        "congestion_control_test_contract" => congestion_control_test_contract(),
        _ => panic!("unknown contract {name}"),
    };
    if extension == "wasm" {
        std::io::stdout().write_all(&code).expect("while writing code to stdout");
    } else {
        panic!("unsupported filetype `{extension}`");
    }
}
