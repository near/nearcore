const fs = require('fs');

const configPath = '/Users/mike/.near/local/config.json';
const genesisPath = '/Users/mike/.near/local/genesis.json';
let config = JSON.parse(fs.readFileSync(configPath));
let genesis = JSON.parse(fs.readFileSync(genesisPath));

config.consensus.min_block_production_delay.nanos = 6660000;

genesis.runtime_config.wasm_config.limit_config.max_total_prepaid_gas = 10000000000000000000;
genesis.runtime_config.wasm_config.limit_config.max_gas_burnt = 10000000000000000000;
genesis.runtime_config.wasm_config.limit_config.max_gas_burnt_view = 10000000000000000000;

fs.writeFileSync(configPath, JSON.stringify(config));
fs.writeFileSync(genesisPath, JSON.stringify(genesis));

console.log('done hacking')
