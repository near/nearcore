const BSON = require('bsonfy').BSON;
const NearBase = require('./nearbase');

/*
 * This is javascript library for interacting with blockchain.
 */
class Near extends NearBase {
    constructor(keyStore, nearConnection) {
        super(keyStore, nearConnection);
        this.nearConnection = nearConnection;
    }

    /**
     * Calls a view function. Returns the same value that the function returns.
     */
    async callViewFunction(sender, contractAccountId, methodName, args) {
        if (!args) {
            args = {};
        }
        const serializedArgs =  Array.from(BSON.serialize(args));
        const response = await this.nearConnection.request('call_view_function', {
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
     * Deploys a contract.
     */
    async deployContract(senderAccountId, contractAccountId, wasmArray, publicKey) {
        await this.submitTransaction('deploy_contract', {
            originator: senderAccountId,
            contract_account_id: contractAccountId,
            wasm_byte_array: wasmArray,
            public_key: publicKey
        });
    }
};

module.exports = Near; 

