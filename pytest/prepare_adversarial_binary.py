# Adversarial infrastructure will call to `prepare_adversarial_binary()` whenever
# it corrupts a node, so calling this script before running adversarial tests is
# not necessary. However, on a node that never ran any adversarial tests compiling
# an adversarial node can take several minutes, causing tests to timeout, so for
# tests that run via CI / nighly and have strict timeouts calling this script
# before-hand is necessary

from lib.adversary import prepare_adversarial_binary

prepare_adversarial_binary()

