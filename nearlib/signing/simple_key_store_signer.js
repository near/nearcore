/**
 * Simple signer that acquires a key from its single keystore and signs transactions.
 */
const bs58 = require('bs58');
const nacl = require('tweetnacl');
const { sha256 } = require('js-sha256');

class SimpleKeyStoreSigner {
    constructor(keyStore) {
        this.keyStore = keyStore;
    }

    /**
     * Sign a transaction body. If the key for senderAccountId is not present, 
     * this operation will fail.
     * @param {object} body
     * @param {string} senderAccountId
     * @param {string} networkId
     */
    async signTransactionBody(body, senderAccountId) {
        return this.signHash(new Uint8Array(sha256.array(body)), senderAccountId);
    }

    async signHash(hash, senderAccountId) {
        const encodedKey = await this.keyStore.getKey(senderAccountId);
        if (!encodedKey) {
            throw new Error(`Cannot find key for sender ${senderAccountId}`);
        }
        const key = bs58.decode(encodedKey.getSecretKey());
        const signature = [...nacl.sign.detached(Uint8Array.from(hash), key)];
        return signature;
    }
}

module.exports = SimpleKeyStoreSigner;
