/**
 * Client for communicating with near blockchain. 
 */

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

    async submitTransaction (method, args) {
        const senderKey = 'originator';
        const sender = args[senderKey];
        const nonce = await this.getNonce(sender);
        const tx_args = Object.assign({}, args, { nonce });
        const response = await this.request(method, tx_args);
        const signature = await this.signer.signTransaction(response, sender);
        const signedTransaction = {
            body: response.body,
            signature: signature
        };
        var submitResponse;
        try {
            submitResponse = await this.request('submit_transaction', signedTransaction);
        } catch(e) {
            console.log(e.response.text);
            throw (e);
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