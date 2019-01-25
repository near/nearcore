
const bs58 = require('bs58');
const nacl = require('tweetnacl');

/**
 * This class provides key pair functionality (generating key pairs, encoding key pairs).
 */
class KeyPair {
    /**
     * Construct an instance of key pair given a public key and secret key. It's generally assumed that these
     * are encoded in bs58.
     * @param {string} publicKey 
     * @param {string} secretKey 
     */
    constructor(publicKey, secretKey) {
        this.publicKey = publicKey;
        this.secretKey = secretKey;
    }

    /**
     * Get the public key.
     */
    getPublicKey() {
        return this.publicKey;
    }

    /**
     * Get the secret key.
     */
    getSecretKey() {
        return this.secretKey;
    }

    /**
     * Generate a new keypair from a random seed
     */
    static async fromRandomSeed() {
        var newKeypair = nacl.sign.keyPair();
        const result = new KeyPair(
            KeyPair.encodeBufferInBs58(newKeypair.publicKey),
            KeyPair.encodeBufferInBs58(newKeypair.secretKey));
        return result;
    }

    /**
     * Encode a buffer as string using bs58
     * @param {Buffer} buffer 
     */
    static encodeBufferInBs58(buffer) {
        const bytes = Buffer.from(buffer);
        const encodedValue = bs58.encode(bytes);
        return encodedValue;
    }
}
module.exports = KeyPair;