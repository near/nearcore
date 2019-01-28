const KeyPair = require('./signing/key_pair');

/**
 * Near account and account related operations. 
 */
class Account {
    constructor(nearClient) {
        this.nearClient = nearClient;
    }

    /**
     * Creates a new account with a given name and key,
     * @param {string} newAccountId id of the new account.
     * @param {string} publicKey public key to associate with the new account
     * @param {number} amount amount of tokens to transfer from originator account id to the new account as part of the creation. 
     * @param {string} originatorAccountId existing account on the blockchain to use for transferring tokens into the new account
     */
    async createAccount (newAccountId, publicKey, amount, originatorAccountId) {
        const createAccountParams = {
            originator: originatorAccountId,
            new_account_id: newAccountId,
            amount: amount,
            public_key: publicKey,
        };

        const transactionResponse = await this.nearClient.submitTransaction('create_account', createAccountParams);
        return transactionResponse;
    }

    /**
    * Creates a new account with a new random key pair. Returns the key pair to the caller. It's the caller's responsibility to
    * manage this key pair.
    * @param {string} newAccountId id of the new account
    * @param {number} amount amount of tokens to transfer from originator account id to the new account as part of the creation. 
    * @param {string} originatorAccountId existing account on the blockchain to use for transferring tokens into the new account
    */
    async createAccountWithRandomKey (newAccountId, amount, originatorAccountId) {
        const keyWithRandomSeed = await KeyPair.fromRandomSeed();
        const createAccountResult = await this.createAccount(
            newAccountId, keyWithRandomSeed.getPublicKey(), amount, originatorAccountId);
        return { key: keyWithRandomSeed, ...createAccountResult };
    }

    /**
     * 
     * @param {string} accountId id of the account to look up 
     */
    async viewAccount (accountId) {
        return await this.nearClient.viewAccount(accountId);
    }
}
module.exports = Account;
