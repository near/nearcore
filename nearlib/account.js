const NearBase = require('./nearbase');

class Account extends NearBase {
    constructor(keyStore, nearConnection) {
        super(keyStore, nearConnection);
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
        return await super.viewAccount(account_id);
    };
};
module.exports = Account;