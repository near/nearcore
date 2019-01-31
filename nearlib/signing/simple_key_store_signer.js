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
     */
    async signTransactionBody(body, senderAccountId) {
        const encodedKey = await this.keyStore.getKey(senderAccountId);
        const message = new Uint8Array(sha256.array(body));
        const key = bs58.decode(encodedKey.getSecretKey());
        const signature = [...nacl.sign.detached(message, key)];
        return signature;
    }
}

module.exports = SimpleKeyStoreSigner;
