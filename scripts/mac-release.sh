#!/usr/bin/env bash
set -xeo pipefail

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

# cspell:words czvf
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
			aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${BRANCH}/$1
			aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${BRANCH}/${COMMIT}/$1
			aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${BRANCH}/${COMMIT}/stable/$1
		fi

		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/$1
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/$1
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/stable/$1

		aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${tar_file}
		aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/${tar_file}
		aws s3 cp --acl public-read ${tar_file} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/stable/${tar_file}
	elif [ "$release" == "perf-release" ]
	then
		if [ "$arch" == "x86_64" ]
		then
			aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${BRANCH}/${COMMIT}/perf/$1
		fi
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/perf/$1
	else
		if [ "$arch" == "x86_64" ]
		then
			aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${BRANCH}/${COMMIT}/nightly/$1
		fi
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/nightly/$1
	fi
}

upload_binary neard

if [ "$release" == "release" ]
then
  upload_binary near-sandbox
fi
