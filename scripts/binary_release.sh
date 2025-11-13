#!/bin/bash
set -xeo pipefail

release="${1:-release}"

case "$release" in
  release|nightly-release|perf-release|assertions-release|test-features-release)
    ;;
  *)  
    echo "Unsupported release type '$release'. Please provide no argument for normal release or provide nightly-release for nightly."
    exit 1
    ;;
esac

BRANCH=$(git branch --show-current)

# in case of Release triggered run, branch is empty
if [ -z "$BRANCH" ]; then
  REF=$(git describe --tags | head -n1)
  BRANCH=$(git branch -r --contains=$REF | head -n1 | cut -c3- | cut -d / -f 2)
fi

COMMIT=$(git rev-parse HEAD)

RELEASE_TAG=""
if [ "${GITHUB_EVENT_NAME}" = "release" ] && [ -n "${GITHUB_REF_NAME}" ]; then
  RELEASE_TAG="${GITHUB_REF_NAME}"
fi

os=$(uname)
arch=$(uname -m)
os_and_arch=${os}-${arch}

# cspell:words czvf
function tar_binary {
  mkdir -p $1/${os_and_arch}
  cp target/release/$1 $1/${os_and_arch}/
  tar -C $1 -czvf $1.tar.gz ${os_and_arch}
}

make $release

function upload_binary {
  if [ "$release" = "release" ]
  then
    tar_binary $1
    tar_file=$1.tar.gz

    upload_targets=()
    if [ -n "${BRANCH}" ]; then
      upload_targets+=("${BRANCH}")
    fi
    if [ -n "${RELEASE_TAG}" ] && [ "${BRANCH}" != "${RELEASE_TAG}" ]; then
      upload_targets+=("${RELEASE_TAG}")
    fi

    if [ ${#upload_targets[@]} -eq 0 ]; then
      echo "Unable to determine upload target for release artifacts" >&2
      exit 1
    fi

    for target in "${upload_targets[@]}"; do
      aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${target}/$1
      aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${target}/${COMMIT}/$1
      aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${target}/${COMMIT}/stable/$1

      aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${target}/${tar_file}
      aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${target}/${COMMIT}/${tar_file}
      aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${target}/${COMMIT}/stable/${tar_file}
    done

  else
    folder="${release%-release}"
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/${folder}/$1
  fi
}

upload_binary neard

# disabled until we clarify why we need this binary in S3
# if [ "$release" != "assertions-release" ]
# then
#   upload_binary store-validator
# fi

# near-sandbox is used by near-workspaces which is an SDK for end-to-end contracts testing that automatically 
# spins up localnet using near-sandbox (neard with extra features useful for testing - state patching, time travel). 
# There are JS and Rust SDKs and it wouldn’t be efficient to build nearcore from scratch on the 
# user machine and CI, so it relies on the prebuilt binaries.
# example PR https://github.com/near/near-sandbox/pull/81/files
if [ "$release" = "release" ]
then
  upload_binary near-sandbox
fi
