const bs58 = require('bs58');
const nacl = require("tweetnacl") 

/**
 * Client for communicating with near blockchain. 
 */
class NearClient {
    constructor (keyStore, nearConnection) {
        this.keyStore = keyStore;
        this.nearConnection = nearConnection;
    }

    async viewAccount (account_id) {
        const viewAccountResponse = await this.request('view_account', {
            account_id: account_id,
        });
        return viewAccountResponse;
    };

    async signTransaction (transaction, sender) {
        const stringifiedTxn = JSON.stringify(transaction);
        const encodedKey = await this.keyStore.getKey(sender);
        const key = bs58.decode(encodedKey.secret_key);
        const message = Buffer.from(stringifiedTxn, 'utf8');
        const signature = [...nacl.sign.detached(message, key)];
        const response = {
            body: transaction,
            signature: signature
        };
        return response;
    };

    async submitTransaction (method, args) {
        const senderKey = 'originator';
        const sender = args[senderKey];
        const nonce = await this.getNonce(sender);
        const response = await this.request(method, Object.assign({}, args, { nonce }));
        const transaction = response.body;
        const signedTransaction = await this.signTransaction(transaction, sender);
        const submitResponse = await this.request('submit_transaction', signedTransaction);
        return submitResponse;
    };

    async getNonce (account_id) {
        return (await this.viewAccount(account_id)).nonce + 1;
    };

    async request (methodName, params) {
        return await this.nearConnection.request(methodName, params);
    };

    async generateNewKeyFromRandomSeed () {
        const response = {};
        var newKeypair = nacl.sign.keyPair()
        response['public_key'] = this.encodeBufferInBs58(newKeypair.publicKey);
        response['secret_key'] = this.encodeBufferInBs58(newKeypair.secretKey);
        return response;
    };

    encodeBufferInBs58(buffer) {
        const bytes = Buffer.from(buffer);
        const encodedValue = bs58.encode(bytes);
        return encodedValue;
    }
}

module.exports = NearClient;