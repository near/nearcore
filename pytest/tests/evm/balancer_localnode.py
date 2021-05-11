from rc import bash, ok, running, kill
import sys
import json
import os
import atexit
import time

ok(bash('''
cd ..
cargo build -p neard --features protocol_feature_evm,nightly_protocol_features
rm -rf ~/.near/local
rm -rf ~/.near-credentials/local
mkdir -p ~/.near-credentials/local
target/debug/neard --home ~/.near/local init
''', stdout=sys.stdout, stderr=sys.stderr))

with open(os.path.expanduser('~/.near/local/validator_key.json')) as f:
    key_file = json.load(f)
    key_file['private_key'] = key_file['secret_key']
with open(os.path.expanduser('~/.near/local/validator_key.json'), 'w') as f:
    json.dump(key_file, f)


p = running('../target/debug/neard --home ~/.near/local run')
time.sleep(5)
atexit.register(lambda: kill(p))

ok(bash('''
cp ~/.near/local/validator_key.json ~/.near-credentials/local/test.near.json

rm -rf balancer-core
git clone https://github.com/near/balancer-core.git
cd balancer-core
git merge origin/hotfix/skip-flawed-test --no-ff --no-edit
npm i
npm i -g near-cli truffle

env NEAR_ENV=local near --masterAccount test.near --keyPath ~/.near/local/validator_key.json evm-dev-init test.near 10

env NEAR_MASTER_ACCOUNT=test.near truffle migrate --network near_local --reset
env NEAR_MASTER_ACCOUNT=test.near truffle test --network near_local
''', stdout=sys.stdout, stderr=sys.stderr))
