const KeyPair = require('./key_pair');

/**
 * Utility functions for dealing with account information. 
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
    toJSON() {
        return {
            account_id: this.accountId,
            public_key: this.keyPair.getPublicKey(),
            secret_key: this.keyPair.getSecretKey(),
            network_id: this.networkId
        };
    }

    /**
     * Utility function to download account info as a standard file.
     */
    downloadAsFile() {
        const fileName = this.keyFileName;
        const text = JSON.stringify(this.toJSON());
      
        var element = document.createElement('a');
        element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(text));
        element.setAttribute('download', fileName);
      
        element.style.display = 'none';
        document.body.appendChild(element);
      
        element.click();
      
        document.body.removeChild(element);
    }

    get keyFileName() {
        return this.networkId + '_' + this.accountId;
    }
}

module.exports = AccountInfo;
