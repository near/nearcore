from rc import bash, ok
import sys

ok(bash('''
git clone https://github.com/near/balancer-core.git
cd balancer-core
npm i
npm i -g near-cli

# test account exists, so do not create them again
# env NEAR_ENV=betanet near evm-dev-init balancer-core-test.betanet 10

mkdir -p ~/.near-credentials/betanet
cp ../tests/evm/keys/*.json ~/.near-credentials/betanet/

env NEAR_MASTER_ACCOUNT=balancer-core-test.betanet truffle migrate --network near_betanet --reset
env NEAR_MASTER_ACCOUNT=balancer-core-test.betanet truffle test --network near_betanet
''', stdout=sys.stdout, stderr=sys.stderr))
