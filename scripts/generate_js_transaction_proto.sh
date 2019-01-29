#!/usr/bin/env bash
PARENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null && pwd )"
${PARENT_DIR}/nearlib/node_modules/.bin/pbjs \
	-t static-module \
	-w commonjs \
	-o ${PARENT_DIR}/nearlib/protos.js \
	${PARENT_DIR}/protos/protos/signed_transaction.proto
