#!/bin/sh

set -eu

if [ -d .github ]; then
    : pass
elif [ -d ../.github ]; then
    cd ..
else
    echo >&2 'Unable to find repository root directory'
    echo >&2 'Please run the script from it as ‘sh scripts/check_formatting.sh’'
    exit 2
fi

verbose_run() {
    echo "+ $*" >&2
    "$@"
}

error=false

echo 'Checking Rust formatting...'
if ! verbose_run cargo fmt -- --check; then
    error=true
    echo >&2
    echo >&2 'Please format Rust code by running ‘cargo fmt’'
    echo >&2
fi

echo 'Checking Python formatting...'
# TODO(near-ops#645): Remove pip install once yapf is bundled in the image.
verbose_run python3 -m pip install -q --user yapf
if ! verbose_run python3 -m yapf -pdr pytest scripts; then
    error=true
    echo >&2
    echo >&2 'Please format Python code by running:'
    echo >&2 '    python3 -m pip install -U yapf'
    echo >&2 '    python3 -m yapf -pir pytest scripts'
    echo >&2
fi

if $error; then
    echo >&2 'Found formatting issues; please fix as per provided instructions'
    exit 1
fi
echo 'All good'
