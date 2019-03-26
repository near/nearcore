#!/bin/bash
set -ex

cd tests/hello
rm -rf node_modules
npm install
npm run build
