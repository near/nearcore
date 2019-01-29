const bs58 = require('bs58');

const NearClient = require('./nearclient');
const BrowserLocalStorageKeystore = require('./signing/browser_local_storage_key_store');
const SimpleKeyStoreSigner = require('./signing/simple_key_store_signer');
const LocalNodeConnection = require('./local_node_connection');
const {
    DeployContractTransaction, FunctionCallTransaction, SignedTransaction
} = require('./protos');

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
    async scheduleFunctionCall(amount, originator, contractId, methodName, args) {
        if (!args) {
            args = {};
        }
        methodName = new Uint8Array(Buffer.from(methodName));
        args = new Uint8Array(Buffer.from(JSON.stringify(args)));
        const nonce = await this.nearClient.getNonce(originator);
        const functionCall = FunctionCallTransaction.create({
            originator,
            contractId,
            methodName,
            args,
        });
        // Integers with value of 0 must be omitted
        // https://github.com/dcodeIO/protobuf.js/issues/1138
        if (nonce !== 0) {
            functionCall.nonce = nonce;
        }
        if (amount !== 0) {
            functionCall.amount = amount;
        }

        const buffer = FunctionCallTransaction.encode(functionCall).finish();
        const signature = await this.nearClient.signer.signTransactionBody(
            buffer,
            originator,
        );

        const signedTransaction = SignedTransaction.create({
            functionCall,
            signature,
        });
        return await this.nearClient.submitTransaction(signedTransaction);
    }

    /**
     * Deploys a smart contract to the block chain
     * @param {string} sender account id of the sender
     * @param {string} contractAccountId account id of the contract
     * @param {Uint8Array} wasmArray wasm binary
     */
    async deployContract(originator, contractId, wasmByteArray) {
        const nonce = await this.nearClient.getNonce(originator);

        // This parameter is not working properly yet. Use some fake value
        var publicKey = '9AhWenZ3JddamBoyMqnTbp7yVbRuvqAv3zwfrWgfVRJE';
        publicKey = bs58.decode(publicKey);
        const deployContract = DeployContractTransaction.create({
            originator,
            contractId,
            wasmByteArray,
            publicKey, 
        });
        // Integers with value of 0 must be omitted
        // https://github.com/dcodeIO/protobuf.js/issues/1138
        if (nonce !== 0) {
            deployContract.nonce = nonce;
        }

        const buffer = DeployContractTransaction.encode(deployContract).finish();
        const signature = await this.nearClient.signer.signTransactionBody(
            buffer,
            originator,
        );

        const signedTransaction = SignedTransaction.create({
            deployContract,
            signature,
        });
        return await this.nearClient.submitTransaction(signedTransaction);
    }

     /**
     * Get a status of a single transaction identified by the transaction hash. 
     * @param {string} transactionHash unique identifier of the transaction
     */
    async getTransactionStatus(transactionHash) {
        const transactionStatusResponse = await this.nearClient.request('get_transaction_result', {
            hash: transactionHash,
        });
        return transactionStatusResponse;
    }

    /**
     * Load given contract and expose it's methods.
     *
     * Every method is taking named arguments as JS object, e.g.:
     * `{ paramName1: "val1", paramName2: 123 }`
     *
     * View method returns promise which is resolved to result when it's available.
     * State change method returns promise which is resolved when state change is succesful and rejected otherwise.
     *
     * Note that `options` param is only needed temporary while contract introspection capabilities are missing.
     *
     * @param {string} contractAccountId contract account name
     * @param {object} options object used to pass named parameters
     * @param {string} options.sender account name of user which is sending transactions
     * @param {string[]} options.viewMethods list of view methods to load (which don't change state)
     * @param {string[]} options.changeMethods list of methods to load that change state
     * @returns {object} object with methods corresponding to given contract methods.
     *
     */
    async loadContract(contractAccountId, options) {
        // TODO: Move this to account context to avoid options.sender
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

