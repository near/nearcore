#!/bin/bash
set -ex

cd tests/hello
rm -rf node_modules
rm package-lock.json
npm install
npm run build
