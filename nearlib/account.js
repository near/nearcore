const superagent = require('superagent');
const ed25519 = require('ed25519');
const bs58 = require('bs58');
const MAX_RETRIES = 3;

class Account {
    constructor(keyStore) {
        this.keyStore = keyStore;
    }

    /**
     * Creates a new account with a given name and key,
     */
    async createAccount (newAccountId, publicKey, amount, originatorAccountId) {
        const createAccountParams = {
            originator: originatorAccountId,
            new_account_id: newAccountId,
            amount: amount,
            public_key: publicKey,
        };

        const transactionResponse = await this.submitTransaction("create_account", createAccountParams);
        return transactionResponse;
    };

    /**
     * Retrieves account data by plain-text account id. 
     */
    async viewAccount (account_id) {
        return await viewAccount(account_id);
    };

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
        const signature = [...ed25519.Sign(new Buffer(stringifiedTxn, 'utf8'), key)];
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
        return { nonce: nonce };
    };

    async getNonce (account_id) {
        return (await this.viewAccount(account_id)).nonce + 1;
    };

    async request (methodName, params) {
        // TODO: (issue 320) make this configurable and testable
        const response = await superagent
            .post(`http://localhost:3030/${methodName}`)
            .use(require('superagent-logger'))
            .send(params);
        return JSON.parse(response.text);
    };
};
module.exports = Account;