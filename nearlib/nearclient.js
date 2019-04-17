const { SignedTransaction } = require('./protos');

/**
 * Client for communicating with near blockchain. 
 */

function _arrayBufferToBase64(buffer) {
    return Buffer.from(buffer).toString('base64');
}

function _base64ToBuffer(str) {
    return new Buffer.from(str, 'base64')
}

class NearClient {
    constructor(signer, nearConnection) {
        this.signer = signer;
        this.nearConnection = nearConnection;
    }

    async viewAccount(accountId) {
        const resp = await this.jsonRpcRequest('abci_query', [`account/${accountId}`, '', 0, false]);
        return JSON.parse(_base64ToBuffer(resp.response.value).toString());
    }

    async submitTransaction(signedTransaction) {
        const buffer = SignedTransaction.encode(signedTransaction).finish();
        const transaction = _arrayBufferToBase64(buffer);
        const params = [transaction];
        return this.jsonRpcRequest('broadcast_tx_async', params);
    }

    async callViewFunction(contractAccountId, methodName, args) {
        if (!args) {
            args = {};
        }
        const serializedArgs = _arrayBufferToBase64(JSON.stringify(args));
        const result = await this.jsonRpcRequest('abci_query', [`call/${contractAccountId}/${methodName}`, serializedArgs, 0, false]);
        const response = result.response;
        response.log.split("\n").forEach(line => {
            console.log(`[${contractAccountId}]: ${line}`);
        });
        // If error, raise exception after printing logs.
        if (response.code != 0) {
            throw Error(response.info)
        }
        const json = JSON.parse(_base64ToBuffer(response.value).toString());
        return json;
    }

    async getTransactionStatus(transactionHash) {
        const encodedHash = _arrayBufferToBase64(transactionHash);
        const response = await this.jsonRpcRequest('tx', [encodedHash, false]);
        let status;
        switch (response.tx_result.code) {
            case 0: status = 'Completed'; break;
            case 1: status = 'Failed'; break;
            case 2: status = 'Stated'; break;
            default: status = 'Unknown';
        }
        return {logs: response.tx_result.log.split('\n'), status, value: response.tx_result.data };
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
