const BSON = require('bsonfy').BSON;
const NearClient = require('./nearclient');

/*
 * This is javascript library for interacting with blockchain.
 */
class Near {
    constructor(nearClient) {
        this.nearClient = nearClient;
    }

    /**
     * Calls a view function. Returns the same value that the function returns.
     */
    async callViewFunction(sender, contractAccountId, methodName, args) {
        if (!args) {
            args = {};
        }
        const serializedArgs =  Array.from(BSON.serialize(args));
        const response = await this.nearClient.request('call_view_function', {
            originator: sender,
            contract_account_id: contractAccountId,
            method_name: methodName,
            args: serializedArgs
        });
        const array = Uint8Array.from(response.result);
        const bson = BSON.deserialize(array);
        return bson.result;
    };

    /**
     * Schedules an asynchronous function call.
     */
    async scheduleFunctionCall(amount, sender, contractAccountId, methodName, args) {
        if (!args) {
            args = {};
        }
        const serializedArgs =  Array.from(BSON.serialize(args));
        const response = await this.nearClient.submitTransaction('schedule_function_call', {
            amount: amount,
            originator: sender,
            contract_account_id: contractAccountId,
            method_name: methodName,
            args: serializedArgs
        });
        return {}; // there is nothing returned from schedule function call right now
    };

    /**
     * Deploys a contract.
     */
    async deployContract(senderAccountId, contractAccountId, wasmArray, publicKey) {
        await this.nearClient.submitTransaction('deploy_contract', {
            originator: senderAccountId,
            contract_account_id: contractAccountId,
            wasm_byte_array: wasmArray,
            public_key: publicKey
        });
    }
};

module.exports = Near; 

