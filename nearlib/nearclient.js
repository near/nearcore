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
        const params = [transaction];
        try {
            return await this.jsonRpcRequest('broadcast_tx_async', params);
        } catch(e) {
            if (e.error) {
                throw new Error(e.error.message);
            }
            throw e;
        }
    }

    async getNonce (account_id) {
        return (await this.viewAccount(account_id)).nonce + 1;
    }

    async jsonRpcRequest(method, params) {
        const request = {
            jsonrpc: '2.0',
            method,
            params,
            id: 'dontcare',
        };
        return await this.nearConnection.request('/', request)
    }

    async request (methodName, params) {
        return await this.nearConnection.request(methodName, params);
    }
}

module.exports = NearClient;
