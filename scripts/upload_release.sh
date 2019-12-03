#!/bin/bash

if [[ -z "${GITLAB_CI}" ]]; then
  echo "This script only works in CI"
  exit 1
fi

# file name example: near-staging-a449f839907f060e75f4448e8f30c7bf7f7a951b
near_binary_file=near-${CI_COMMIT_REF_NAME}-$(git rev-parse HEAD)
near_latest=near-${CI_COMMIT_REF_NAME}-latest
cp target/release/near ${near_binary_file}-release
cp target/debug/near ${near_binary_file}-debug
/snap/bin/gsutil cp ${near_binary_file}-release gs://nearprotocol_nearcore_release
/snap/bin/gsutil cp gs://nearprotocol_nearcore_release/${near_binary_file}-release gs://nearprotocol_nearcore_release/${near_latest}-release
/snap/bin/gsutil cp ${near_binary_file}-debug gs://nearprotocol_nearcore_release
/snap/bin/gsutil cp gs://nearprotocol_nearcore_release/${near_binary_file}-debug gs://nearprotocol_nearcore_release/${near_latest}-debug
