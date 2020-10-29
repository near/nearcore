const { RainbowConfig } = require('rainbow-bridge-utils')

RainbowConfig.setParam('near-node-url', 'http://0.0.0.0:3040')
RainbowConfig.setParam('near-network-id', 'local')
RainbowConfig.setParam('near-master-account', 'test0')
RainbowConfig.setParam(
  'near-master-sk',
  'ed25519:3KyUucjyGk1L58AJBB6Rf6EZFqmpTSSKG7KKsptMvpJLDBiZmAkU4dR1HzNS6531yZ2cR5PxnTM7NLVvSfJjZPh7'
//'ed25519:3D4YudUQRE39Lc4JHghuB5WM8kbgDDa34mnrEP5DdTApVH81af7e2dWgNPEaiQfdJnZq1CNPp5im4Rg5b733oiMP'
)
RainbowConfig.saveConfig()
