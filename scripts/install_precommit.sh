#!/bin/sh

echo "Copying rustfmt.toml into ~/.config/rustfmt/"
mkdir -p "${HOME}"/.config/rustfmt
cp $(dirname "${0}")/../rustfmt.toml "${HOME}"/.config/rustfmt/


