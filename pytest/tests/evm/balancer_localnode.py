from rc import bash, ok
import sys

from cluster import start_cluster
sys.path.append('lib')

ok(bash('''
cd ..
cargo build -p neard --features protocol_feature_evm,nightly_protocol_features
''', stdout=sys.stdout, stderr=sys.stderr))

nodes = start_cluster(1, 0, 1, None, [], {})

ok(bash('''
rm -rf balancer-core
git clone https://github.com/near/balancer-core.git
cd balancer-core
git checkout near-evm-nightly
npm i
npm i -g near-cli truffle

mkdir -p ~/.near-credentials/local
cp ~/.near/test0/validator_key.json ~/.near-credentials/local/test0.json

env NEAR_ENV=local near evm-dev-init test0 10

env NEAR_MASTER_ACCOUNT=test0 truffle migrate --network near_localnet --reset
env NEAR_MASTER_ACCOUNT=test0 truffle test --network near_localnet
''', stdout=sys.stdout, stderr=sys.stderr))
