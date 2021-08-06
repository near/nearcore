#!/usr/bin/env bash

set -eu

dry_run_arg=
if [ "$#" -ge 1 ]; then
	case "${1-}" in
	--dry-run)
		dry_run_arg=--dry-run
		;;
	*)
		echo "${0##*/}: $1: unknown argument" >&2
		exit 1
		;;
	esac
fi

pwd=$(pwd)

set -x

for pkg in near-vm-errors near-vm-logic near-vm-runner \
           near-vm-runner-standalone; do
	cd "$pwd/$pkg"
	cargo publish $dry_run_arg
	if [ -z "$dry_run_arg" ]; then
		# Sleep a bit to let the previous package upload to
		# crates.io.  Otherwise we fail publishing checks.
		sleep 30
	fi
done
