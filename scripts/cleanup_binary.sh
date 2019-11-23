#!/bin/sh

for file in `find target/debug/deps/ \
  ! -name 'test_cases_runtime-*' \
  ! -name 'test_cases_testnet_rpc-*' \
  ! -name 'test_catchup-*' \
  ! -name 'test_errors-*' \
  ! -name 'test_rejoin-*' \
  ! -name 'test_simple-*' \
  ! -name 'test_tps_regression-*' \
  ! -name 'near' \
  ! -name 'near-*' \
  ! -name '*.so' \
  ! -name 'loadtester-*' \
  `
do
  if [ -f $file ] && [ -x $file ]; then
    rm "$file"
  fi
done