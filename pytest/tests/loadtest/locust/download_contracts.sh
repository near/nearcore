#!/bin/bash
#
# Retrieves the WASM contracts from respective URLs or directories for all Locust workloads
# and stores them in the "res" folder.

SCRIPT_DIR="${0%/*}"

# Directory to place the wasm files in.
TARGET_CONTRACTS_DIR="${SCRIPT_DIR}/res"

# Directory where some of the contracts are located.
# TODO: Consider storing the contracts in a single place.
SOURCE_CONTRACTS_DIR="${SCRIPT_DIR}/../../../../runtime/near-test-contracts/res"

wget https://raw.githubusercontent.com/NearSocial/social-db/master/res/social_db_release.wasm -O ${TARGET_CONTRACTS_DIR}/social_db.wasm
wget https://raw.githubusercontent.com/sweatco/sweat-near/main/res/sweat.wasm -O ${TARGET_CONTRACTS_DIR}/sweat.wasm
wget https://raw.githubusercontent.com/sweatco/sweat-near/main/res/sweat_claim.wasm -O ${TARGET_CONTRACTS_DIR}/sweat_claim.wasm
ln -s ${SOURCE_CONTRACTS_DIR}/fungible_token.wasm ${TARGET_CONTRACTS_DIR}/fungible_token.wasm
ln -s ${SOURCE_CONTRACTS_DIR}/backwards_compatible_rs_contract.wasm ${TARGET_CONTRACTS_DIR}/congestion.wasm

# The `inscription.near` contract has been downloaded using:
# ./neard view-state dump-code --account-id inscription.near --output inscription.wasm
# from mainnet at block height 123138647
