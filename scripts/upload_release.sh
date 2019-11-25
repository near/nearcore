#!/bin/bash

if [[ -z "${GITLAB_CI}" ]]; then
  echo "This script only works in CI"
  exit 1
fi

# file name example: near-staging-a449f839907f060e75f4448e8f30c7bf7f7a951b
near_binary_file=near-$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)-$(git rev-parse HEAD)
cp target/release/near ${near_binary_file}
gsutil cp ${near_binary_file} gs://nearprotocol_nearcore_release