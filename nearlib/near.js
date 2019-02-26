const createError = require('http-errors');

const NearClient = require('./nearclient');
const BrowserLocalStorageKeystore = require('./signing/browser_local_storage_key_store');
const SimpleKeyStoreSigner = require('./signing/simple_key_store_signer');
const LocalNodeConnection = require('./local_node_connection');
const {
    DeployContractTransaction, FunctionCallTransaction, SignedTransaction
} = require('./protos');

const MAX_STATUS_POLL_ATTEMPTS = 5;
const STATUS_POLL_PERIOD_MS = 1000;

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
     * @param {string} contractAccountId account id of the contract
     * @param {string} methodName method to call
     * @param {object} args arguments to pass to the method
     */
    async callViewFunction(contractAccountId, methodName, args) {
        if (!args) {
            args = {};
        }
        const serializedArgs = Array.from(Buffer.from(JSON.stringify(args)));
        const response = await this.nearClient.request('call_view_function', {
            contract_account_id: contractAccountId,
            method_name: methodName,
            args: serializedArgs
        });
        response.logs.forEach(line => {
            console.log(`[${contractAccountId}]: ${line}`);
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
     * @param {string} contractAccountId account id of the contract
     * @param {Uint8Array} wasmArray wasm binary
     */
    async deployContract(contractId, wasmByteArray) {
        const nonce = await this.nearClient.getNonce(contractId);

        const deployContract = DeployContractTransaction.create({
            contractId,
            wasmByteArray,
        });
        // Integers with value of 0 must be omitted
        // https://github.com/dcodeIO/protobuf.js/issues/1138
        if (nonce !== 0) {
            deployContract.nonce = nonce;
        }

        const buffer = DeployContractTransaction.encode(deployContract).finish();
        const signature = await this.nearClient.signer.signTransactionBody(
            buffer,
            contractId,
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
     * Wait until transaction is completed or failed.
     * Automatically sends logs from contract to `console.log`.
     *
     * {@link MAX_STATUS_POLL_ATTEMPTS} defines how many attempts are made.
     * {@link STATUS_POLL_PERIOD_MS} defines delay between subsequent {@link getTransactionStatus} calls.
     *
     * @param {string | object} transactionResponseOrHash hash of transaction or object returned from {@link submitTransaction}
     * @param {object} options object used to pass named parameters
     * @param {string} options.contractAccountId specifies contract ID for better logs and error messages
     */
    async waitForTransactionResult(transactionResponseOrHash, options = {}) {
        const transactionHash = transactionResponseOrHash.hasOwnProperty('hash') ? transactionResponseOrHash.hash : transactionResponseOrHash;
        const contractAccountId = options.contractAccountId || 'unknown contract';
        let result;
        for (let i = 0; i < MAX_STATUS_POLL_ATTEMPTS; i++) {
            await sleep(STATUS_POLL_PERIOD_MS);
            result = (await this.getTransactionStatus(transactionHash)).result;
            const flatLog = result.logs.reduce((acc, it) => acc.concat(it.lines), []);
            flatLog.forEach(line => {
                console.log(`[${contractAccountId}]: ${line}`);
            });
            if (result.status == 'Completed') {
                return result;
            }
            if (result.status == 'Failed') {
                const errorMessage = flatLog.find(it => it.startsWith('ABORT:')) || '';
                throw createError(400, `Transaction ${transactionHash} on ${contractAccountId} failed. ${errorMessage}`);
            }
        }
        throw createError(408, `Exceeded ${MAX_STATUS_POLL_ATTEMPTS} status check attempts ` +
            `for transaction ${transactionHash} on ${contractAccountId} with status: ${result.status}`);
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
                return near.callViewFunction(contractAccountId, methodName, args);
            };
        });
        options.changeMethods.forEach((methodName) => {
            contract[methodName] = async function (args) {
                args = args || {};
                const response = await near.scheduleFunctionCall(0, options.sender, contractAccountId, methodName, args);
                return near.waitForTransactionResult(response.hash, { contractAccountId });
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

