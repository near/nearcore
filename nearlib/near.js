const NearClient = require('./nearclient');
const BrowserLocalStorageKeystore = require('./signing/browser_local_storage_key_store');
const SimpleKeyStoreSigner = require('./signing/simple_key_store_signer');
const LocalNodeConnection = require('./local_node_connection');

const MAX_STATUS_POLL_ATTEMPTS = 3;
const STATUS_POLL_PERIOD_MS = 250;

/**
 * Javascript library for interacting with near.
 */
class Near {
    /**
     * Constructs near with an instance of nearclient.
     * @constructor
     * @param {NearClient} nearClient
     */
    constructor(nearClient) {
        this.nearClient = nearClient;
    }

    /**
     * Generate a default configuration for nearlib
     * @param {string} nodeUrl url of the near node to connect to
     */
    static createDefaultConfig(nodeUrl = 'http://localhost:3030') {
        return new Near(new NearClient(
            new SimpleKeyStoreSigner(new BrowserLocalStorageKeystore()),
            new LocalNodeConnection(nodeUrl)
        ));
    }

    /**
     * Calls a view function. Returns the same value that the function returns.
     * @param {string} sender account id of the sender
     * @param {string} contractAccountId account id of the contract
     * @param {string} methodName method to call
     * @param {object} args arguments to pass to the method
     */
    async callViewFunction(sender, contractAccountId, methodName, args) {
        if (!args) {
            args = {};
        }
        const serializedArgs = Array.from(Buffer.from(JSON.stringify(args)));
        const response = await this.nearClient.request('call_view_function', {
            originator: sender,
            contract_account_id: contractAccountId,
            method_name: methodName,
            args: serializedArgs
        });
        const json = JSON.parse(Buffer.from(response.result).toString());
        return json.result;
    }

    /**
     * Schedules an asynchronous function call. Returns a hash which can be used to
     * check the status of the transaction later.
     * @param {number} amount amount of tokens to transfer as part of the operation
     * @param {string} sender account id of the sender
     * @param {string} contractAccountId account id of the contract
     * @param {string} methodName method to call
     * @param {object} args arguments to pass to the method
     */
    async scheduleFunctionCall(amount, sender, contractAccountId, methodName, args) {
        if (!args) {
            args = {};
        }
        const serializedArgs = Array.from(Buffer.from(JSON.stringify(args)));
        return await this.nearClient.submitTransaction('schedule_function_call', {
            amount: amount,
            originator: sender,
            contract_account_id: contractAccountId,
            method_name: methodName,
            args: serializedArgs
        });
    }

    /**
     *  Deploys a smart contract to the block chain
     * @param {string} sender account id of the sender
     * @param {string} contractAccountId account id of the contract
     * @param {Uint8Array} wasmArray wasm binary
     */
    async deployContract(sender, contractAccountId, wasmArray) {
        return await this.nearClient.submitTransaction('deploy_contract', {
            originator: sender,
            contract_account_id: contractAccountId,
            wasm_byte_array: wasmArray,
            public_key: '9AhWenZ3JddamBoyMqnTbp7yVbRuvqAv3zwfrWgfVRJE' // This parameter is not working properly yet. Use some fake value
        });
    }

    async getTransactionStatus(transaction_hash) {
        const transactionStatusResponse = await this.nearClient.request('get_transaction_result', {
            hash: transaction_hash,
        });
        return transactionStatusResponse;
    }

    async loadContract(contractAccountId, options) {
        // TODO: Introspection of contract methods + move this to account context to avoid options
        let contract = {};
        let near = this;
        options.viewMethods.forEach((methodName) => {
            contract[methodName] = async function (args) {
                args = args || {};
                return near.callViewFunction(options.sender, contractAccountId, methodName, args);
            };
        });
        options.changeMethods.forEach((methodName) => {
            contract[methodName] = async function (args) {
                args = args || {};
                const response = await near.scheduleFunctionCall(0, options.sender, contractAccountId, methodName, args);
                let status;
                for (let i = 0; i < MAX_STATUS_POLL_ATTEMPTS; i++) {
                    await sleep(STATUS_POLL_PERIOD_MS);
                    status = await near.getTransactionStatus(response.hash);
                    if (status.status == 'Completed') {
                        return status;
                    }
                }
                throw new Error(`Exceeded ${MAX_STATUS_POLL_ATTEMPTS} status check attempts ` +
                    `for transaction ${response.hash} with status: ${status.status}`);
            };
        });
        return contract;
    }
}

function sleep(time) {
    return new Promise(function (resolve) {
        setTimeout(resolve, time);
    });
}

module.exports = Near;

