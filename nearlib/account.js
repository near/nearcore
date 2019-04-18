const bs58 = require('bs58');

const { CreateAccountTransaction, SignedTransaction } = require('./protos');
const KeyPair = require('./signing/key_pair');

/**
 * Near account and account related operations. 
 * @example
 * const account = new Account(nearjs.nearClient);
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
     * @example
     * const createAccountResponse = await account.createAccount(
     *    mainTestAccountName,
     *    keyWithRandomSeed.getPublicKey(),
     *    1000,
     *    aliceAccountName);
     */
    async createAccount(newAccountId, publicKey, amount, originator) {
        const nonce = await this.nearClient.getNonce(originator);
        publicKey = bs58.decode(publicKey);
        const createAccount = CreateAccountTransaction.create({
            originator,
            newAccountId,
            publicKey,
        });
        // Integers with value of 0 must be omitted
        // https://github.com/dcodeIO/protobuf.js/issues/1138
        if (nonce !== 0) {
            createAccount.nonce = nonce;
        }
        if (amount !== 0) {
            createAccount.amount = amount;
        }

        const buffer = CreateAccountTransaction.encode(createAccount).finish();
        const signature = await this.nearClient.signer.signTransactionBody(
            buffer,
            originator,
        );

        const signedTransaction = SignedTransaction.create({
            createAccount,
            signature,
        });
        return this.nearClient.submitTransaction(signedTransaction);
    }

    /**
    * Creates a new account with a new random key pair. Returns the key pair to the caller. It's the caller's responsibility to
    * manage this key pair.
    * @param {string} newAccountId id of the new account
    * @param {number} amount amount of tokens to transfer from originator account id to the new account as part of the creation. 
    * @param {string} originatorAccountId existing account on the blockchain to use for transferring tokens into the new account
    * @example
    * const createAccountResponse = await account.createAccountWithRandomKey(
    *     newAccountName,
    *     amount,
    *     aliceAccountName);
    */
    async createAccountWithRandomKey (newAccountId, amount, originatorAccountId) {
        const keyWithRandomSeed = await KeyPair.fromRandomSeed();
        const createAccountResult = await this.createAccount(
            newAccountId,
            keyWithRandomSeed.getPublicKey(),
            amount,
            originatorAccountId,
        );
        return { key: keyWithRandomSeed, ...createAccountResult }; 
    }

    /**
     * Returns an existing account with a given `accountId`
     * @param {string} accountId id of the account to look up 
     * @example
     * const viewAccountResponse = await account.viewAccount(existingAccountId);
     */
    async viewAccount (accountId) {
        return await this.nearClient.viewAccount(accountId);
    }
}
module.exports = Account;
