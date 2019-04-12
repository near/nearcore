const KeyPair = require('./key_pair');

/**
 * Utility functions for dealing with account information. 
 */
class AccountInfo {
    constructor(accountId, keyPair, networkId) {
        this._accountId = accountId;
        this._keyPair = keyPair;
        this._networkId = networkId;
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
            account_id: this._accountId,
            public_key: this._keyPair.getPublicKey(),
            secret_key: this._keyPair.getSecretKey(),
            network_id: this._networkId
        };
    }

    /**
     * Gets/sets a key pair for account info.
     */
    get keyPair() {
        return this._keyPair;
    }
    set keyPair(keyPair) {
        this._keyPair = keyPair;
    }

    /**
     * Gets a key pair from account info.
     */
    get accountId() {
        return this._accountId;
    }
    set accountId(accountId) {
        this._accountId = accountId;
    }

    /**
     * Gets/sets network id.
     */
    get networkId() {
        return this._networkId;
    }
    set networkId(networkId) {
        this._networkId = networkId;
    }

    /**
     * Utility function to download account info as a standard file.
     */
    downloadAsFile() {
        const fileName = getKeyFileName();
        const text = JSON.stringify(this.toJSON());
      
        var element = document.createElement('a');
        element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(text));
        element.setAttribute('download', fileName);
      
        element.style.display = 'none';
        document.body.appendChild(element);
      
        element.click();
      
        document.body.removeChild(element);
    }

    getKeyFileName() {
        return this._networkId + '_' + this._accountId;
    }
}

module.exports = AccountInfo;