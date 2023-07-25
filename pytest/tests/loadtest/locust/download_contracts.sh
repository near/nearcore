#!/bin/bash
#
# Downloads the WASM contracts necessary for all workloads and stores them in "res" folder.

cd res
wget https://raw.githubusercontent.com/NearSocial/social-db/master/res/social_db_release.wasm -O social_db.wasm
wget https://raw.githubusercontent.com/sweatco/sweat-near/main/res/sweat.wasm -O sweat.wasm
ln -s ../../../../../runtime/near-test-contracts/res/fungible_token.wasm fungible_token.wasm
ln -s ../../../../../runtime/near-test-contracts/res/backwards_compatible_rs_contract.wasm congestion.wasm
