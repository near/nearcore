#!/bin/bash
set -xeo pipefail

release="${1:-release}"

case "$release" in
  release|nightly-release|perf-release|assertions-release)
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

os=$(uname)
arch=$(uname -m)
os_and_arch=${os}-${arch}

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
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${BRANCH}/$1
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${BRANCH}/${COMMIT}/$1
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${BRANCH}/${COMMIT}/stable/$1

    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/$1
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/$1
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/stable/$1

    aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${tar_file}
    aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/${tar_file}
    aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/stable/${tar_file}

  else
    folder="${release%-release}"
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${BRANCH}/${COMMIT}/${folder}/$1
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
