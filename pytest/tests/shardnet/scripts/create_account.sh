#!/bin/bash
account_id=$(cat /home/ubuntu/.near/shardnet/validator_key.json  | grep account_id | awk -F'"' '{print $4}')
mkdir -p /home/ubuntu/.near-credentials/shardnet/
echo "{\"account_id\":\"$account_id\",\"public_key\":\"$1\",\"private_key\":\"$2\"}" > /home/ubuntu/.near-credentials/shardnet/near.json
pk=$(cat ~/.near/shardnet/validator_key.json  | grep public_key | awk -F'"' '{print $4}')
NEAR_ENV=shardnet near --nodeUrl=http://127.0.0.1:3030 create-account $account_id --masterAccount near --initialBalance 1000000 --publicKey $pk
