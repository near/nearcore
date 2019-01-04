class Account {
    constructor(nearClient) {
        this.nearClient = nearClient;
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

        const transactionResponse = await this.nearClient.submitTransaction("create_account", createAccountParams);
        return transactionResponse;
    };

    /**
     * Retrieves account data by plain-text account id. 
     */
    async viewAccount (account_id) {
        return await this.nearClient.viewAccount(account_id);
    };
};
module.exports = Account;