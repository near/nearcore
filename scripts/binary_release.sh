#!/bin/bash
set -eo pipefail

release="${1:-release}"

case "$release" in
  release|nightly-release|perf-release|assertions-release)
    ;;
  *)  
    echo "Unsupported release type '$release'. Please provide no argument for normal release or provide nightly-release for nightly."
    exit 1
    ;;
esac

branch=$(git rev-parse --abbrev-ref HEAD)
commit=$(git rev-parse HEAD)

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
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/$1
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/$1
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/stable/$1

    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/$1
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/$1
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/stable/$1

    aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${tar_file}
    aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/${tar_file}
    aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/stable/${tar_file}

  else
    folder="${release%-release}"
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/${folder}/$1
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/${folder}/$1
  fi
}

upload_binary neard

# disabled until we clarify why we need this binary in S3
# if [ "$release" != "assertions-release" ]
# then
#   upload_binary store-validator
# fi

if [ "$release" = "release" ]
then
  upload_binary near-sandbox
fi
