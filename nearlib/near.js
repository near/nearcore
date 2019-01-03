const BSON = require('bsonfy').BSON;

/*
 * This is javascript library for interacting with blockchain.
 */
class Near {
    constructor(keyStore, nearConnection) {
        this.nearConnection = nearConnection;
        this.keyStore = keyStore;
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
};

module.exports = Near; 

