#!/usr/bin/env bash

set -e

npm --version
# if that's the issue use
# yarn config set ignore-engines true
npm current node -v
yarn --version
python3 --version
cargo --version


if [[ $(yarn --version) != "1."* ]]; then
  echo "You version of yarn is too old $(yarn --version) < 1.0"
  echo "Install with npm install --global yarn"
  exit 1
fi

