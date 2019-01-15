/**
 * Key pair.
 */
const bs58 = require('bs58');
const nacl = require("tweetnacl");

class KeyPair {
    constructor(publicKey, secretKey) {
        this.publicKey = publicKey;
        this.secretKey = secretKey;
    }

    getPublicKey() {
        return this.publicKey;
    }

    getSecretKey() {
        return this.secretKey;
    }

    static async fromRandomSeed() {
        const response = {};
        var newKeypair = nacl.sign.keyPair()
        const result = new KeyPair(
            KeyPair.encodeBufferInBs58(newKeypair.publicKey),
            KeyPair.encodeBufferInBs58(newKeypair.secretKey));
        return result;
    }

    static encodeBufferInBs58(buffer) {
        const bytes = Buffer.from(buffer);
        const encodedValue = bs58.encode(bytes);
        return encodedValue;
    }
};
module.exports = KeyPair;