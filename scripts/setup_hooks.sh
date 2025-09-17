#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
HOOK_DIR=$(git rev-parse --show-toplevel)/.git/hooks
ln -s -f "${DIR}/pre-commit" "${HOOK_DIR}/pre-commit"
