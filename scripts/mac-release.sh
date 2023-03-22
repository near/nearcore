#!/usr/bin/env bash
set -eo pipefail

release="release"
if [ ! -z "$1" ];
then
	release=$1
fi

if [ "$release" != "release" ] && [ "$release" != "nightly-release" ] && [ "$release" != "perf-release" ]
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
arch=$(uname -m)
os_and_arch=${os}-${arch}

function tar_binary {
  mkdir -p $1/${os_and_arch}
  cp target/release/$1 $1/${os_and_arch}/
  tar -C $1 -czvf $1.tar.gz ${os_and_arch}
}

make $release

function upload_binary {
	if [ "$release" == "release" ]
	then
		tar_binary $1
		tar_file=$1.tar.gz

		if [ "$arch" == "x86_64" ]
		then
			aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/$1
			aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/$1
			aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/stable/$1
		fi

		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/$1
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/$1
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/stable/$1

		aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${tar_file}
		aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/${tar_file}
		aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/stable/${tar_file}
	elif [ "$release" == "perf-release" ]
	then
		if [ "$arch" == "x86_64" ]
		then
			aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/perf/$1
		fi
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/perf/$1
	else
		if [ "$arch" == "x86_64" ]
		then
			aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/nightly/$1
		fi
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${branch}/${commit}/nightly/$1
	fi
}

upload_binary neard
upload_binary store-validator

if [ "$release" == "release" ]
then
  upload_binary near-sandbox
fi
