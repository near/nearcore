#!/usr/bin/env bash

echo "Copying rustfmt.toml into ~/.config/rustfmt/"
mkdir -p "${HOME}"/.config/rustfmt
cp $(dirname "${0}")/../rustfmt.toml "${HOME}"/.config/rustfmt/

if [[ ! -f "${HOME}/.local/bin/pre-commit" ]]; then
    echo "installing pre-commit program"
    if [[ ! -z "$(command -v pip3)" ]]; then
        pip3 install pre-commit
    elif [[ ! -z "$(command -v pip)" ]]; then
        pip install pre-commit
    else
        echo "Please install pip3 or pip and rerun this script"
        exit 1
    fi
fi

"${HOME}"/.local/bin/pre-commit install
