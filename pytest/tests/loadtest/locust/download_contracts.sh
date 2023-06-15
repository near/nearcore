#!/bin/bash
#
# Downloads the WASM contracts necessary for all workloads and stores them in "res" folder.

cd res
wget https://raw.githubusercontent.com/NearSocial/social-db/master/res/social_db_release.wasm -O social_db_release.wasm
wget https://raw.githubusercontent.com/near/nearcore/master/runtime/near-test-contracts/res/fungible_token.wasm -O fungible_token.wasm
cp ../../../../../runtime/near-test-contracts/res/test_contract_rs.wasm .
