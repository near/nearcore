#!/bin/bash
PATH=$(npm bin):$PATH
cp node_modules/near-runtime-ts/near.ts ./assembly
mkdir -p ./assembly/json
cp -R node_modules/assemblyscript-json/assembly/{encoder,decoder}.ts ./assembly/json
asc -O3 --baseDir assembly model.ts --nearFile model.near.ts
asc -O3 --baseDir assembly main.ts --nearFile main.near.ts
asc -O3 --baseDir assembly main.near.ts --binaryFile ../../hello.wasm

