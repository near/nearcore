#!/bin/bash
set -e

export TMHOME=/srv/near

tendermint init
cp /near/config.toml ${TMHOME}/config/config.toml

if [[ -z ${PRIVATE_NETWORK} ]]
then
echo "Not running on a private network"
else
sed -i 's/addr_book_strict\ =\ true/addr_book_strict\ =\ false/g' ${TMHOME}/config/config.toml
fi

cat << EOF > ${TMHOME}/config/genesis.json
{
  "genesis_time": "2019-04-19T21:02:18.967617Z",
  "chain_id": "test-chain",
  "consensus_params": {
    "block": {
      "max_bytes": "22020096",
      "max_gas": "-1",
      "time_iota_ms": "1000"
    },
    "evidence": {
      "max_age": "100000"
    },
    "validator": {
      "pub_key_types": [
        "ed25519"
      ]
    }
  },
  "app_hash": ""
}
EOF

if [[ -z ${CHAIN_SPEC_PATH} ]]
then
	generate-test-spec -a ${TOTAL_NODES} -c ${TMHOME}/chain_spec.json -n ${NUM_ACCOUNTS}
	CHAIN_SPEC_PATH="${TMHOME}/chain_spec.json"
fi

if [[ -z ${KEYGEN_SEED} ]] 
then
	KEYGEN_SEED="near.${NODE_ID}"
fi

keystore keygen --tendermint --test-seed ${KEYGEN_SEED} -p ${TMHOME}/config/

if [[ -n ${NODE_KEY} ]]
then
    cat << EOF > ${TMHOME}/config/node_key.json
{"priv_key": {"type": "tendermint/PrivKeyEd25519", "value": "$NODE_KEY"}}
EOF
	cat ${TMHOME}/config/node_key.json
fi

echo "Chain spec ${CHAIN_SPEC_PATH} with ${TOTAL_NODES}"
echo "Keygen: ${KEYGEN_SEED}"
echo "Bootnode: ${BOOT_NODES}"

tendermint node --p2p.persistent_peers="${BOOT_NODES}" &

nearmint --abci-address 127.0.0.1:26658 --chain-spec-file=${CHAIN_SPEC_PATH} --base-path=${TMHOME}

