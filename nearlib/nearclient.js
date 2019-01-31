const { SignedTransaction } = require('./protos');

/**
 * Client for communicating with near blockchain. 
 */

function _arrayBufferToBase64(buffer) {
    return Buffer.from(buffer).toString('base64');
}

class NearClient {
    constructor (signer, nearConnection) {
        this.signer = signer;
        this.nearConnection = nearConnection;
    }

    async viewAccount (account_id) {
        const viewAccountResponse = await this.request('view_account', {
            account_id: account_id,
        });
        return viewAccountResponse;
    }

    async submitTransaction (signedTransaction) {
        const buffer = SignedTransaction.encode(signedTransaction).finish();
        const transaction = _arrayBufferToBase64(buffer);
        const data = { transaction };
        var submitResponse;
        try {
            submitResponse = await this.request('submit_transaction', data);
        } catch(e) {
            if (e.response) { console.log(e.response.text) }
            else { console.log(e) }
            throw (e)
        }
        return submitResponse;
    }

    async getNonce (account_id) {
        return (await this.viewAccount(account_id)).nonce + 1;
    }

    async request (methodName, params) {
        return await this.nearConnection.request(methodName, params);
    }
}

module.exports = NearClient;
