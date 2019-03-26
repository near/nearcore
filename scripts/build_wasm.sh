#!/bin/bash
set -ex

cd tests/hello && npm install && npm run build && cd ../..
