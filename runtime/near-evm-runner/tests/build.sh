#!/bin/bash

contracts=(
    "SolTests"
    "SubContract"
    "Create2Factory"
    "SelfDestruct"
    "ConstructorRevert"
    "PrecompiledFunction"
)

truffle compile || exit 1
for contractName in "${contracts[@]}"
    do
        cat build/contracts/"$contractName".json | \
          jq .bytecode | \
          awk '{ print substr($1,4,length($1)-4) }' | \
          tr -d '\n' \
          > build/"$contractName".bin

        cat build/contracts/"$contractName".json | \
          jq .abi \
          > build/"$contractName".abi
    done
rm -rf build/contracts
