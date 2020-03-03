#!/bin/bash
set -euo pipefail

make
docker tag nearcore nearprotocol/nearcore:master
docker push nearprotocol/nearcore:master