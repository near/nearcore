#!/bin/bash
#
# Downloads the WASM contracts necessary for all workloads and stores them in "res" folder.

cd res
wget https://raw.githubusercontent.com/NearSocial/social-db/master/res/social_db_release.wasm -O social_db.wasm
ln -s ../../../../../runtime/near-test-contracts/res/fungible_token.wasm fungible_token.wasm
ln -s ../../../../../runtime/near-test-contracts/res/backwards_compatible_rs_contract.wasm congestion.wasm
