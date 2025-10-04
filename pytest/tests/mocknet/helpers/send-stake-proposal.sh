#!/usr/bin/env bash
# This script is used to send a stake proposal to the mocknet cluster.

# Read validator data from validators.json
# TODO: consider querying near-cli to get validator balance, it should be
# simpler.
validators_file="$HOME/.near/neard-runner/validators.json"

if [ ! -f "$validators_file" ]; then
    echo "Error: validators.json not found at $validators_file"
    exit 1
fi

# Read our validator key to get our account_id
our_account_id=$(jq -r '.account_id' ~/.near/validator_key.json)
our_public_key=$(jq -r '.public_key' ~/.near/validator_key.json)

# Find our validator in the validators.json file
validator_data=$(jq -r --arg account_id "$our_account_id" '.[] | select(.account_id == $account_id)' "$validators_file")

if [ -z "$validator_data" ]; then
    echo "Error: Could not find validator with account_id $our_account_id in validators.json"
    exit 1
fi

# Extract amount and convert from yoctonear to NEAR (integer only)
amount_yoctonear=$(echo "$validator_data" | jq -r '.amount')
amount_near=$(echo "$amount_yoctonear / 1000000000000000000000000" | bc | cut -d. -f1)

echo "Found validator: $our_account_id"
echo "Public key: $our_public_key"
echo "Amount: $amount_near NEAR (from $amount_yoctonear yoctonear)"

/home/ubuntu/.cargo/bin/near-validator staking stake-proposal \
    $our_account_id $our_public_key \
    "$amount_near NEAR" \
    network-config mocknet sign-with-keychain send
