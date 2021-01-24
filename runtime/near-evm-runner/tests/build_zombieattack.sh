#!/bin/sh -ex

docker run -v `pwd`:/sources ethereum/solc:0.5.17 -o /sources/build --abi --bin --overwrite /sources/contracts/ZombieAttack.sol
