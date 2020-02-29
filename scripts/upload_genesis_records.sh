#!/bin/bash
testnet_genesis_hash=$(cat near/res/testnet_genesis_hash)
echo "Uploading testnet genesis records into S3"
aws s3 cp --acl public-read near/res/testnet_genesis_records_${testnet_genesis_hash}.json s3://testnet.nearprotocol.com/
echo
echo "Uploaded to: https://s3-us-west-1.amazonaws.com/testnet.nearprotocol.com/testnet_genesis_records_${testnet_genesis_hash}.json"