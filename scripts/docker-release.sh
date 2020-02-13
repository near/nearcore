#!/bin/bash
function make_sure {
    read -p "$1. Are you sure [y/n]? " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]
    then
        exit 1
    fi
}

branch=$(git rev-parse --abbrev-ref HEAD)
if [ "${branch}" != "master" ]; then
    make_sure "Not in master branch"
fi

if [ -n "$(git status -s)" ]; then
    make_sure "There's untracked files or uncommitted changes"
fi

make
docker tag nearcore nearprotocol/nearcore:latest
docker push nearprotocol/nearcore:latest