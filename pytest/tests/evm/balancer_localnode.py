from rc import bash
import sys

p = bash('''
git clone https://github.com/near/balancer-core.git
cd balancer-core
npm i
npm i -g near-cli

# env NEAR_ENV=localnet near evm-dev-init balancer-core-test.near 10

# env NEAR_MASTER_ACCOUNT=balancer-core-test.near truffle migrate --network near_localnet --reset
# env NEAR_MASTER_ACCOUNT=balancer-core-test.near truffle test --network near_localnet
''')

print(p.stdout)
print(p.stderr, file=sys.stderr)