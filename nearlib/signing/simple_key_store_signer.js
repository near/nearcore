/**
 * Simple signer that acquires a key from its single keystore and signs transactions.
 */
const bs58 = require('bs58');
const nacl = require('tweetnacl');
const { sha256 } = require('js-sha256');
const { google } = require('../protos');

class SimpleKeyStoreSigner {
    constructor(keyStore) {
        this.keyStore = keyStore;
    }

    /**
     * Signs a buffer. If the key for originator is not present,
     * this operation will fail.
     * @param {Uint8Array} buffer
     * @param {string} originator
     */
    async signBuffer(buffer, originator) {
        return this.signHash(new Uint8Array(sha256.array(buffer)), originator);
    }

    async signHash(hash, originator) {
        const encodedKey = await this.keyStore.getKey(originator);
        if (!encodedKey) {
            throw new Error(`Cannot find key for originator ${originator}`);
        }
        const key = bs58.decode(encodedKey.getSecretKey());
        const signature = [...nacl.sign.detached(Uint8Array.from(hash), key)];
        return {
            signature,
            publicKey: google.protobuf.BytesValue.create({
                value: bs58.decode(encodedKey.getPublicKey()),
            }),
        };
    }
}

module.exports = SimpleKeyStoreSigner;
