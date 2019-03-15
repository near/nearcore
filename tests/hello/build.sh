#!/bin/sh
PATH=$(npm bin):$PATH
asc -O3 --baseDir assembly model.ts --nearFile model.near.ts
asc -O3 --baseDir assembly main.ts --nearFile main.near.ts
asc -O3 --baseDir assembly main.near.ts --binaryFile ../../hello.wasm

