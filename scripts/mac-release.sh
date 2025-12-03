#!/usr/bin/env bash
set -xeo pipefail

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

BRANCH=$(git branch --show-current)

# in case of Release triggered run, branch is empty
if [ -z "$BRANCH" ]; then
  REF=$(git describe --tags | head -n1)
  BRANCH=$(git branch -r --contains=$REF | head -n1 | cut -c3- | cut -d / -f 2)
fi

COMMIT=$(git rev-parse HEAD)

RELEASE_TAG=""
# the release commit is assigned multiple tags. Only use the tag from the git run.
if [ "${GITHUB_EVENT_NAME}" = "release" ] && [ -n "${GITHUB_REF_NAME}" ]; then
  RELEASE_TAG="${GITHUB_REF_NAME}"
fi

os=$(uname)
arch=$(uname -m)
os_and_arch=${os}-${arch}

# cspell:words czvf
function tar_binary {
  if [ "${DRY_RUN}" = "true" ]; then
    echo "DRY RUN MODE: Would run 'tar -C $1 -czvf $1.tar.gz ${os_and_arch}'"
    touch $1.tar.gz
    return
  fi
  mkdir -p $1/${os_and_arch}
  cp target/release/$1 $1/${os_and_arch}/
  tar -C $1 -czvf $1.tar.gz ${os_and_arch}
}

if [ "${DRY_RUN}" = "true" ]; then
  echo "DRY RUN MODE: Would run 'make $release'"
else
  make $release
fi

function upload_binary {
	DRY_RUN_FLAG=""
    if [ "${DRY_RUN}" = "true" ]; then
        DRY_RUN_FLAG="--dryrun"
    fi
    if [ "$release" == "release" ]
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

		sources=("target/release/$1" "${tar_file}")
        destinations=("$1" "${tar_file}")

		for target in "${upload_targets[@]}"; do  
			for i in "${!sources[@]}"; do
				src=${sources[$i]}
				dst=${destinations[$i]}
				
				aws s3 cp --acl public-read ${src} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${dst} ${DRY_RUN_FLAG}
				aws s3 cp --acl public-read ${src} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/${dst} ${DRY_RUN_FLAG}
				aws s3 cp --acl public-read ${src} s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/stable/${dst} ${DRY_RUN_FLAG}
			done
		done
	else
	    folder="${release%-release}"
		aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os_and_arch}/${BRANCH}/${COMMIT}/${folder}/$1 ${DRY_RUN_FLAG}
	fi
}

upload_binary neard

if [ "$release" == "release" ]
then
  upload_binary near-sandbox
fi
