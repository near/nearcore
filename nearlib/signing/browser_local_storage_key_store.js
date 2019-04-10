/**
 * Stores keys in the browser local storage. This allows to retain keys between
 * browser sessions. Local storage likes to work with strings so we store public and private key separately.
 */
const KeyPair = require('./key_pair');
const JsonAccountInfo = require('./json_account_info');

const LOCAL_STORAGE_SECRET_KEY_SUFFIX = '_secretkey';
const LOCAL_STORAGE_PUBLIC_KEY_SUFFIX = '_publickey';




class BrowserLocalStorageKeystore {
    constructor(networkId = 'unknown', localStorage = window.localStorage) {
        this.networkId = networkId;
        this.localStorage = localStorage;
    }

    static storageKeyForPublicKey(accountId) {
        return accountId + '_' + this.networkId + LOCAL_STORAGE_PUBLIC_KEY_SUFFIX;
    }

    static storageKeyForSecretKey(accountId) {
        return accountId + '_' + this.networkId + LOCAL_STORAGE_SECRET_KEY_SUFFIX;
    }

    /**
     * Save the key in local storage. 
     * @param {string} accountId 
     * @param {KeyPair} key 
     */
    async setKey(accountId, key) {
        this.localStorage.setItem(
            BrowserLocalStorageKeystore.storageKeyForPublicKey(accountId), key.getPublicKey());
        this.localStorage.setItem(
            BrowserLocalStorageKeystore.storageKeyForSecretKey(accountId), key.getSecretKey());
    }

    async setKeyFromJson(json) {
        const accountInfo = new JsonAccountInfo(json);
        if (this.networkId != accountInfo.getNetworkId()) {
            throw 'Setting key for a wrong network';
        }
        this.setKey(accountInfo.getAccountId(), accountInfo.getKeyPair());
    }

    /**
     * Get the key from local storage and return as KeyPair object.
     * @param {string} accountId 
     */
    async getKey(accountId) {
        if (!this.localStorage.getItem(
                BrowserLocalStorageKeystore.storageKeyForPublicKey(accountId)) ||
            !this.localStorage.getItem(
                BrowserLocalStorageKeystore.storageKeyForSecretKey(accountId))) {
            throw 'Key lookup failed. Please make sure you set up an account.';
        }
        return new KeyPair(
            this.localStorage.getItem(
                BrowserLocalStorageKeystore.storageKeyForPublicKey(accountId)),
            this.localStorage.getItem(
                BrowserLocalStorageKeystore.storageKeyForSecretKey(accountId))
        );
    }

    static getAccounts() {
        return Object.keys(this.localStorage).map(function(key) {
            if (key.endsWith('_public')) {
                return key.substr(0, key.length() - 7);
            }
        });
    }
}

module.exports = BrowserLocalStorageKeystore;