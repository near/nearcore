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
    };

    async submitTransaction (method, args) {
        const senderKey = 'originator';
        const sender = args[senderKey];
        const nonce = await this.getNonce(sender);
        const response = await this.request(method, Object.assign({}, args, { nonce }));
        const transaction = response.body;
        const stringifiedTxn = JSON.stringify(transaction);
        const message = Buffer.from(stringifiedTxn, 'utf8');
        const signature = await this.signer.signTransaction(message, sender);
        const signedTransaction = {
            body: transaction,
            signature: signature
        };
        const submitResponse = await this.request('submit_transaction', signedTransaction);
        return submitResponse;
    };

    async getNonce (account_id) {
        return (await this.viewAccount(account_id)).nonce + 1;
    };

    async request (methodName, params) {
        return await this.nearConnection.request(methodName, params);
    };
}

module.exports = NearClient;