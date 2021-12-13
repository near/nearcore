#!/usr/bin/env bash
set -e

echo "waiting on health check; NEAR_PID = ${NEAR_PID:?undefined}" >&2

for _ in {1..500}; do
    echo -n '.' >&2
    kill -0 "$NEAR_PID" > /dev/null 2>&1 || exit 1
    if [[ "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:3030/status)" == "200" ]]; then
        exit 0
    fi
    sleep 5
done

echo 'ERROR: waiting timeout' >&2
exit 1
