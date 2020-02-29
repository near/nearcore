#!/bin/bash
near_home=${NEAR_HOME:-${HOME}/.near}
echo "Dump state from ${near_home}"
cargo run -p state-viewer -- dump_state
echo
output_hash=$(cat ${near_home}/output_hash)
echo "Result Genesis hash is: ${output_hash}"
echo "Moving result genesis config, records and genesis_hash into near/res"
mv ${near_home}/output_records_${output_hash}.json near/res/testnet_genesis_records_${output_hash}.json
mv ${near_home}/output_hash near/res/testnet_genesis_hash
mv ${near_home}/output_config.json near/res/testnet_genesis_config.json
