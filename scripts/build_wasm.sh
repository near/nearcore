#!/bin/bash
set -ex
 
rm -rf node_modules yarn.lock
yarn add near-hello
cp node_modules/near-hello/dist/main.wasm tests/hello.wasm
