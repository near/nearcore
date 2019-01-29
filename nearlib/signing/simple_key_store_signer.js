/**
 * Simple signer that acquires a key from its single keystore and signs transactions.
 */
const bs58 = require('bs58');
const nacl = require('tweetnacl');

class SimpleKeyStoreSigner {
    constructor(keyStore) {
        this.keyStore = keyStore;
    }

    /**
     * Sign a given hash. If the key for senderAccountId is not present, this operation
     * will fail.
     * @param {Buffer} hash
     * @param {string} senderAccountId
     */
    async signHash(hash, senderAccountId) {
        const encodedKey = await this.keyStore.getKey(senderAccountId);
        const message = bs58.decode(hash);
        const key = bs58.decode(encodedKey.getSecretKey());
        const signature = [...nacl.sign.detached(message, key)];
        return signature;
    }

    /**
     * Sign a transaction. If the key for senderAccountId is not present, this operation
     * will fail.
     * @param {object} tx Transaction details
     * @param {string} senderAccountId
     */
    signTransaction(tx, senderAccountId) {
        return this.signHash(tx.hash, senderAccountId);
    }

}

module.exports = SimpleKeyStoreSigner;