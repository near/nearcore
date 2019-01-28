const bs58 = require('bs58');

const { CreateAccountTransaction, SignedTransaction } = require('./protos');
const KeyPair = require("./signing/key_pair");

class Account {
    constructor(nearClient) {
        this.nearClient = nearClient;
    }

    /**
     * Creates a new account with a given name and key,
     */
    async createAccount (newAccountId, publicKey, amount, originator) {
        const nonce = await this.nearClient.getNonce(originator);
        publicKey = bs58.decode(publicKey);
        const createAccount = CreateAccountTransaction.create({
            nonce,
            originator,
            newAccountId,
            amount,
            publicKey,
        });
        const buffer = CreateAccountTransaction.encode(createAccount).finish();
        const signature = await this.nearClient.signer.signTransactionBody(
            buffer,
            originator,
        );

        const signedTransaction = SignedTransaction.create({
            createAccount,
            signature,
        });
        return await this.nearClient.submitTransaction(signedTransaction);
    };

    /**
     * Generate a key from a random seed and create a new account with this key.
     */
    async createAccountWithRandomKey (newAccountId, amount, originatorAccountId) {
        const keyWithRandomSeed = await KeyPair.fromRandomSeed();
        await this.createAccount(
            newAccountId,
            keyWithRandomSeed.getPublicKey(),
            amount,
            originatorAccountId,
        );
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
