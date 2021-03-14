const { RainbowConfig } = require('rainbow-bridge-utils')

RainbowConfig.setParam('near-node-url', 'http://0.0.0.0:3040')
RainbowConfig.setParam('near-network-id', 'local')
RainbowConfig.setParam('near-master-account', 'test0')
RainbowConfig.setParam(
  'near-master-sk',
  'ed25519:3KyUucjyGk1L58AJBB6Rf6EZFqmpTSSKG7KKsptMvpJLDBiZmAkU4dR1HzNS6531yZ2cR5PxnTM7NLVvSfJjZPh7'
)
RainbowConfig.setParam('eth-node-url', 'ws://localhost:9545')
RainbowConfig.setParam(
  'eth-master-sk',
  '0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200'
)
RainbowConfig.setParam('near-client-validate-ethash', 'false')
RainbowConfig.saveConfig()
