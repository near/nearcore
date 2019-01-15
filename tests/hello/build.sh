#!/bin/sh
PATH=$(npm bin):$PATH
asc --baseDir assembly main.ts --nearFile main.near.ts
asc --baseDir assembly main.near.ts --binaryFile ../../hello.wasm

