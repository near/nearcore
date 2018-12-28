bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:3030/healthz)" != "200" ]]; do sleep 5; done'
