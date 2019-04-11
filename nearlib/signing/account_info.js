const KeyPair = require('./key_pair');

/**
 * Standard format for storing account info in json
 */
class AccountInfo {

    constructor(accountId, keyPair, networkId) {
        this.accountId = accountId;
        this.keyPair = keyPair;
        this.networkId = networkId;
    }

    /**
     * Reconstruct account info object from json.
     * @param {Object} json 
     */
    static fromJson(json) {
        if (!json.public_key || !json.secret_key || !json.account_id || !json.network_id) {
            throw 'Invalid account info format. Please ensure it contains public_key, secret_key, and account_id".';
        }
        return new AccountInfo(json.account_id, new KeyPair(json.public_key, json.secret_key), json.network_id);
    }

    /**
     * Convert to standard json format.
     */
    toJson() {
        return {
            account_id: this.accountId,
            public_key: this.keyPair.getPublicKey(),
            secret_key: this.keyPair.getSecretKey(),
            network_id: this.networkId
        };
    }

    /**
     * Gets a key pair from account info.
     */
    getKeyPair() {
        return this.keyPair;
    }

    /**
     * Gets a key pair from account info.
     */
    getAccountId() {
        return this.accountId;
    }

    getNetworkId() {
        return this.networkId;
    }

    /**
     * Utility function to download account info as a standard file.
     */
    downloadAsFile() {
        const fileName = getKeyFileName();
        const text = JSON.stringify(this.toJson());
      
        var element = document.createElement('a');
        element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(text));
        element.setAttribute('download', fileName);
      
        element.style.display = 'none';
        document.body.appendChild(element);
      
        element.click();
      
        document.body.removeChild(element);
    }

    getKeyFileName() {
        return this.networkId + '_' + this.networkIdaccountId;
    }
}

module.exports = AccountInfo;