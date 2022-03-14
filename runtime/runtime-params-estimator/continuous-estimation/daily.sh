#!/bin/bash
# Script designed to be run once a day on a machine that continously estimates
# gas costs based on the nearcore master source code. It pulls changes and
# updates the website all in one go.

set -e

export REPO_UNDER_TEST="/home/jakob/estimator_nearcore/"
export WWW_PATH="/var/estimator/www/"

pushd "${REPO_UNDER_TEST}"
git fetch
# TODO: Only continue if force by check or FETCH_HEAD != latest estimation.
git checkout FETCH_HEAD
popd

# Run estimations and store results in sqlite DB
printf '%s %s\n' `date` "Starting all estimation";
bash run_estimation.sh
printf '%s %s\n' `date` "Estimation done";

# Update static website that displays results
bash update_www.sh

# TODO check alerts
# TODO push alerts