#!/bin/bash
set -eo pipefail

release="stable"
if [ ! -z "$1" ]
then
	release=$1
fi

if [ "$release" != "stable" ] && [ "$release" != "nightly" ]
then
	echo "Please provide no argument for normal release or provide nightly."
	exit 1
fi

branch=${BUILDKITE_BRANCH//:/_}
branch=${branch//\//_}
commit=${BUILDKITE_COMMIT}
if [[ ${commit} == "HEAD" ]]; then
    commit=$(git rev-parse HEAD)
fi

image_name="nearcore"
if [[ ${release} == "stable" ]];
then
	make
else
	make docker-nearcore-nightly
	image_name="nearcore-nightly"
fi

docker tag $image_name nearprotocol/${image_name}:${branch}-${commit}
docker tag $image_name nearprotocol/${image_name}:${branch}

set -x
docker push nearprotocol/${image_name}:${branch}-${commit}
docker push nearprotocol/${image_name}:${branch}
if [[ ${branch} == "master" ]];
then
	docker tag $image_name nearprotocol/${image_name}:latest
	docker push nearprotocol/${image_name}:latest
fi
