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

echo "Uploading testnet genesis records into S3"
aws s3 cp --acl public-read near/res/testnet_genesis_records_${output_hash}.json s3://testnet.nearprotocol.com/
echo
echo "Uploaded to: https://s3-us-west-1.amazonaws.com/testnet.nearprotocol.com/testnet_genesis_records_${output_hash}.json"