#!/bin/bash
set -eo pipefail

release="release"
if [ ! -z "$1" ];
then
	release=$1
fi

if [ "$release" != "release" ] && [ "$release" != "nightly-release" ]
then
	echo "Please provide no argument for normal release or provide nightly-release for nightly"
	exit 1
fi

branch=${BUILDKITE_BRANCH:-${GITHUB_REF##*/}}
commit=${BUILDKITE_COMMIT:-${GITHUB_SHA}}
if [[ ${commit} == "HEAD" ]]; then
    commit=$(git rev-parse HEAD)
fi
os=$(uname)

make $release

function upload_binary {
	if [ "$release" == "release" ]
	then
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/$1
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/$1
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/stable/$1
	else
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/nightly/$1
	fi
}

upload_binary near
upload_binary keypair-generator
upload_binary genesis-csv-to-json
upload_binary near-vm-runner-standalone
upload_binary state-viewer
upload_binary store-validator
