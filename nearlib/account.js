const KeyPair = require("./signing/key_pair");

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
     * Generate a key from a random seed and create a new account with this key.
     */
    async createAccountWithRandomKey (newAccountId, amount, originatorAccountId) {
        const keyWithRandomSeed = await KeyPair.fromRandomSeed();
        const createAccountResult = await this.createAccount(
            newAccountId, keyWithRandomSeed.getPublicKey(), amount, originatorAccountId);
        const response = {};
        response["key"] = keyWithRandomSeed;
        return response;
    };

    /**
     * Retrieves account data by plain-text account id. 
     */
    async viewAccount (account_id) {
        return await this.nearClient.viewAccount(account_id);
    };
};
module.exports = Account;