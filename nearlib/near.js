const createError = require('http-errors');

const NearClient = require('./nearclient');
const BrowserLocalStorageKeystore = require('./signing/browser_local_storage_key_store');
const SimpleKeyStoreSigner = require('./signing/simple_key_store_signer');
const LocalNodeConnection = require('./local_node_connection');
const {
    DeployContractTransaction, FunctionCallTransaction, SignedTransaction
} = require('./protos');

const MAX_STATUS_POLL_ATTEMPTS = 10;
const STATUS_POLL_PERIOD_MS = 2000;

/**
 * Javascript library for interacting with near.
 */
class Near {
    /**
     * Constructs near with an instance of nearclient.
     * @constructor
     * @param {NearClient} nearClient
     * @example
     * const nearClient = new nearlib.NearClient(
     *   walletAccount, 
     *   new nearlib.LocalNodeConnection(config.nodeUrl));
     * const near = new nearlib.Near(nearClient);
     */
    constructor(nearClient) {
        this.nearClient = nearClient;
    }

    /**
     * Generate a default configuration for nearlib
     * @param {string} nodeUrl url of the near node to connect to
     * @example
     * Near.createDefaultConfig();
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
     * @example
     * const viewFunctionResponse = await near.callViewFunction(
     *   contractAccountId, 
     *   methodName, 
     *   args);
     */
    async callViewFunction(contractAccountId, methodName, args) {
        return this.nearClient.callViewFunction(contractAccountId, methodName, args);
    }

    /**
     * Schedules an asynchronous function call. Returns a hash which can be used to
     * check the status of the transaction later.
     * @param {number} amount amount of tokens to transfer as part of the operation
     * @param {string} sender account id of the sender
     * @param {string} contractAccountId account id of the contract
     * @param {string} methodName method to call
     * @param {object} args arguments to pass to the method
     * @example
     * const scheduleResult = await near.scheduleFunctionCall(
     *     0,
     *     aliceAccountName,
     *     contractName,
     *     'setValue', // this is the function defined in a wasm file that we are calling
     *     setArgs);
     */
    async scheduleFunctionCall(amount, originator, contractId, methodName, args) {
        if (!args) {
            args = {};
        }
        methodName = new Uint8Array(Buffer.from(methodName));
        args = new Uint8Array(Buffer.from(JSON.stringify(args)));
        const nonce = await this.nearClient.getNonce(originator);
        const functionCall = FunctionCallTransaction.create({
            nonce,
            originator,
            contractId,
            methodName,
            args,
        });
        // Integers with value of 0 must be omitted
        // https://github.com/dcodeIO/protobuf.js/issues/1138
        if (amount !== 0) {
            functionCall.amount = amount;
        }

        const buffer = FunctionCallTransaction.encode(functionCall).finish();
        const signatureAndPublicKey = await this.nearClient.signer.signBuffer(
            buffer,
            originator,
        );

        const signedTransaction = SignedTransaction.create({
            functionCall,
            signature: signatureAndPublicKey.signature,
            publicKey: signatureAndPublicKey.publicKey,
        });
        return await this.nearClient.submitTransaction(signedTransaction);
    }

    /**
     * Deploys a smart contract to the block chain
     * @param {string} contractAccountId account id of the contract
     * @param {Uint8Array} wasmArray wasm binary
     * @example
     * const response =  await nearjs.deployContract(contractName, data);
     */
    async deployContract(contractId, wasmByteArray) {
        const nonce = await this.nearClient.getNonce(contractId);

        const deployContract = DeployContractTransaction.create({
            nonce,
            contractId,
            wasmByteArray,
        });

        const buffer = DeployContractTransaction.encode(deployContract).finish();
        const signatureAndPublicKey = await this.nearClient.signer.signBuffer(
            buffer,
            contractId,
        );

        const signedTransaction = SignedTransaction.create({
            deployContract,
            signature: signatureAndPublicKey.signature,
            publicKey: signatureAndPublicKey.publicKey,
        });
        return await this.nearClient.submitTransaction(signedTransaction);
    }

    /**
     * Get a status of a single transaction identified by the transaction hash.
     * @param {string} transactionHash unique identifier of the transaction
     * @example
     * // get the result of a transaction status call
     * const result = await this.getTransactionStatus(transactionHash)
     */
    async getTransactionStatus(transactionHash) {
        return this.nearClient.getTransactionStatus(transactionHash);
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
     * @example
     * const result = await this.waitForTransactionResult(transactionHash);
     */
    async waitForTransactionResult(transactionResponseOrHash, options = {}) {
        const transactionHash = transactionResponseOrHash.hasOwnProperty('hash') ? transactionResponseOrHash.hash : transactionResponseOrHash;
        const contractAccountId = options.contractAccountId || 'unknown contract';
        let alreadyDisplayedLogs = [];
        let result;
        for (let i = 0; i < MAX_STATUS_POLL_ATTEMPTS; i++) {
            await sleep(STATUS_POLL_PERIOD_MS);
            result = (await this.getTransactionStatus(transactionHash));
            let j;
            for (j = 0; j < alreadyDisplayedLogs.length && alreadyDisplayedLogs[j] == result.logs[j]; j++);
            if (j != alreadyDisplayedLogs.length) {
                console.warn('new logs:', result.logs, 'iconsistent with already displayed logs:', alreadyDisplayedLogs);
            }
            for (; j < result.logs.length; ++j) {
                const line = result.logs[j];
                console.log(`[${contractAccountId}]: ${line}`);
                alreadyDisplayedLogs.push(line);
            }
            if (result.status == 'Completed') {
                if (result.value) {
                    result.lastResult = JSON.parse(Buffer.from(result.value, 'base64').toString());
                }
                return result;
            }
            if (result.status == 'Failed') {
                const errorMessage = result.logs.find(it => it.startsWith('ABORT:')) || '';
                const hash = Buffer.from(transactionHash).toString('base64');
                throw createError(400, `Transaction ${hash} on ${contractAccountId} failed. ${errorMessage}`);
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
     * @example
     * // this example would be a counter app with a contract that contains the incrementCounter and decrementCounter methods
     * window.contract = await near.loadContract(config.contractName, {
     *   viewMethods: ["getCounter"],
     *   changeMethods: ["incrementCounter", "decrementCounter"],
     *   sender: nearlib.dev.myAccountId
     * });
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
