#!/bin/bash

ensure_repo () {
  local name=$1;
  if [ $# -eq 2 ]; then
     name=$2;
  fi

  if [[ -e $name ]]; then
    # shellcheck disable=SC2164
    cd "$name";
    git pull
  else
    git clone --depth=1 https://github.com/near/"$1" "$name";
  fi
}

if [ "${1}" != "--source-only" ]; then
    cd test-binaries || exit 1;
    ensure_repo near-sdk-rs;
    ensure_repo core-contracts;
fi
