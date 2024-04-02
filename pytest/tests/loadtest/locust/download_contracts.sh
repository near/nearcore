#!/bin/bash
#
# Downloads the WASM contracts necessary for all workloads and stores them in "res" folder.

wget https://raw.githubusercontent.com/NearSocial/social-db/master/res/social_db_release.wasm -O res/social_db.wasm
wget https://raw.githubusercontent.com/sweatco/sweat-near/main/res/sweat.wasm -O res/sweat.wasm
cp ../../../../runtime/near-test-contracts/res/fungible_token.wasm res/fungible_token.wasm
cp ../../../../runtime/near-test-contracts/res/backwards_compatible_rs_contract.wasm res/congestion.wasm
