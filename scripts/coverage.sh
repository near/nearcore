#!/bin/sh

if [[ -z "${GITLAB_CI}" ]]; then
  echo "This script only works in CI"
  exit 1
fi

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
    # codecov script cannot follow symlinks, so place here and mv it to target
    mkdir -p "target2/cov/$(basename $file)"
    kcov --include-pattern=nearcore --verify "target2/cov/$(basename $file)" "$file"
  fi
done

rm target # In CI, removes the symlink.
mv target2 target
# codecov sometimes incorrectly merge report, use kcov to merge before upload
kcov --merge target/coverage target/cov/*
rm -rf target/cov
curl -s https://codecov.io/bash | bash
echo "Uploaded code coverage"
