const KeyPair = require('./key_pair');

/**
 * Standard format for storing account info in json
 */
class JsonAccountInfo {
    constructor(json) {
        if (!json.public_key || !json.secret_key || !json.account_id || !json.network_id) {
            throw 'Invalid account info format. Please ensure it contains public_key, secret_key, and account_id".';
        }
        this.json = json;
    }

    /**
     * Gets a key pair from account info.
     */
    getKeyPair() {
        const result = new KeyPair(this.json.public_key, this.json.secret_key);
        return result;
    }

    /**
     * Gets a key pair from account info.
     */
    getAccountId() {
        return this.json.account_id;
    }

    getNetworkId() {
        return this.json.network_id;
    }
}

module.exports = JsonAccountInfo;