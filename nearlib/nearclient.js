const { SignedTransaction } = require('./protos');

/**
 * Client for communicating with near blockchain. 
 */

function _arrayBufferToBase64(buffer) {
    return Buffer.from(buffer).toString('base64');
}

class NearClient {
    constructor(signer, nearConnection) {
        this.signer = signer;
        this.nearConnection = nearConnection;
    }

    async viewAccount(accountId) {
        return this.jsonRpcRequest('abci_query', [`account/${accountId}`, '', 0, false]);
    }

    async submitTransaction(signedTransaction) {
        const buffer = SignedTransaction.encode(signedTransaction).finish();
        const transaction = _arrayBufferToBase64(buffer);
        const params = [transaction];
        return this.jsonRpcRequest('broadcast_tx_async', params);
    }

    async getTransactionStatus(transactionHash) {
        const encodedHash = _arrayBufferToBase64(transactionHash);
        const response = await this.nearClient.jsonRpcRequest('tx', [encodedHash, false]);
        let status;
        switch (response.code) {
            case 0: status = 'Completed'; break;
            case 1: status = 'Failed'; break;
            case 2: status = 'Stated'; break;
            default: status = 'Unknown';
        }
        return {result: {logs: response.log, status}};
    }

    async getNonce(accountId) {
        return (await this.viewAccount(accountId)).nonce + 1;
    }

    async jsonRpcRequest(method, params) {
        const request = {
            jsonrpc: '2.0',
            method,
            params,
            id: 'dontcare',
        };
        const response = await this.nearConnection.request('', request);
        if (response.error) {
            throw Error(response.error.message);
        }
        return response.result;
    }

    async request(methodName, params) {
        return this.nearConnection.request(methodName, params);
    }
}

module.exports = NearClient;
