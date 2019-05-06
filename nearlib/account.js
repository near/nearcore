const bs58 = require('bs58');

const { google, AccessKey, AddKeyTransaction, CreateAccountTransaction, SignedTransaction } = require('./protos');
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
            nonce,
            originator,
            newAccountId,
            publicKey,
        });
        // Integers with value of 0 must be omitted
        // https://github.com/dcodeIO/protobuf.js/issues/1138
        if (amount !== 0) {
            createAccount.amount = amount;
        }

        const buffer = CreateAccountTransaction.encode(createAccount).finish();
        const signatureAndPublicKey = await this.nearClient.signer.signBuffer(
            buffer,
            originator,
        );

        const signedTransaction = SignedTransaction.create({
            createAccount,
            signature: signatureAndPublicKey.signature,
            publicKey: signatureAndPublicKey.publicKey,
        });
        return this.nearClient.submitTransaction(signedTransaction);
    }

    /**
     * Adds a new access key to the owners account for an some app to use.
     * @param {string} ownersAccountId id of the owner's account.
     * @param {string} newPublicKey public key for the access key.
     * @param {string} contractId if the given contractId is not empty, then this access key will only be able to call
     *      the given contractId.
     * @param {string} methodName If the given method name is not empty, then this access key will only be able to call
     *      the given method name.
     * @param {string} fundingOwner account id to own the funding of this access key. If empty then account owner is used by default.
     *      fundingOwner should be used if this access key would be sponsored by the app. In this case the app would
     *      prefer to own funding of this access key, to get it back when the key is removed.
     * @param {number} fundingAmount amount of funding to withdraw from the owner's account and put to this access key.
     *      Make sure you that you don't fund the access key when the fundingOwner is different from the account's owner.
     * @example
     * const addAccessKeyResponse = await account.addAccessKey(
     *    accountId,
     *    keyWithRandomSeed.getPublicKey(),
     *    contractId,
     *    "",
     *    "",
     *    10);
     */
    async addAccessKey(ownersAccountId, newPublicKey, contractId, methodName, fundingOwner, fundingAmount) {
        const nonce = await this.nearClient.getNonce(ownersAccountId);
        newPublicKey = bs58.decode(newPublicKey);
        const accessKey = AccessKey.create({});
        if (contractId) {
            accessKey.contractId = google.protobuf.StringValue.create({
                value: contractId,
            });
        }
        if (methodName) {
            accessKey.methodName = google.protobuf.BytesValue.create({
                value: new Uint8Array(Buffer.from(methodName)),
            });
        }
        if (fundingOwner) {
            accessKey.balanceOwner = google.protobuf.StringValue.create({
                value: fundingOwner,
            });
        }
        if (fundingAmount > 0) {
            accessKey.amount = fundingAmount;
        }
        const addKey = AddKeyTransaction.create({
            nonce,
            originator: ownersAccountId,
            newKey: newPublicKey,
            accessKey,
        });
        const buffer = AddKeyTransaction.encode(addKey).finish();
        const signatureAndPublicKey = await this.nearClient.signer.signBuffer(
            buffer,
            ownersAccountId,
        );

        const signedTransaction = SignedTransaction.create({
            addKey,
            signature: signatureAndPublicKey.signature,
            publicKey: signatureAndPublicKey.publicKey,
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
        const keyWithRandomSeed = KeyPair.fromRandomSeed();
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
