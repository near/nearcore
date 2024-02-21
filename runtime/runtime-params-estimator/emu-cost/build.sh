#!/usr/bin/env bash
cd "${0%/*}"
# FIXME: does this need to use XDG? Well, whoever sets it to non-default value is on the hook for
# making it work.
if [[ ! -f "$HOME/.config/containers/policy.json" ]]; then
    mkdir -p "$HOME/.config/containers"
    cp policy.json "$HOME/.config/containers"
fi
exec podman --runtime=crun build -t rust-emu .
